// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/taskengine/jobs"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// 批量数据同步工作流错误
var (
	ErrEmptyAPINames       = errors.New("api_names cannot be empty")
	ErrEmptyDataSourceName = errors.New("data_source_name is required")
	ErrEmptyToken          = errors.New("token is required")
	ErrEmptyTargetDBPath   = errors.New("target_db_path is required")
	ErrEmptyStartDate      = errors.New("start_date is required")
	ErrEmptyEndDate        = errors.New("end_date is required")
)

// APITimeWindowStrategy 时间窗口模板策略（工作流内部使用的 DTO）
// 用于声明某个 API 需要按时间窗口 + 模板任务拆分子任务。
type APITimeWindowStrategy struct {
	// Enabled 是否启用时间窗口模板
	Enabled bool
	// Freq 时间步长（如 "D", "3H"），由 GenerateDatetimeRange 解析
	Freq string
	// DateParamKey 若非空，则使用窗口起始时间转换为 YYYYMMDD 注入到该参数，
	// 而不是使用 start_date/end_date（例如 cctv_news 的 "date"）。
	DateParamKey string
	// UseTradeCalendar 预留：未来可按交易日历（trade_cal）切片，目前按自然时间切片。
	UseTradeCalendar bool
}

// APISyncStrategy API 同步策略（工作流内部使用的 DTO）
// 定义每个 API 优先使用的同步维度
type APISyncStrategy struct {
	// PreferredParam 优先使用的参数
	// "trade_date" - 支持按日期查询全市场数据，使用 direct 模式
	// "ts_code" - 仅支持按股票代码查询，使用 template 模式按股票生成子任务
	PreferredParam string
	// SupportDateRange 是否支持日期范围查询（start_date/end_date）
	// true: 可以用 start_date+end_date 或 trade_date
	// false: 只能用 trade_date（单日查询）
	SupportDateRange bool
	// RequiredParams 必需的参数（除了 PreferredParam 之外）
	RequiredParams []string
	// Dependencies 依赖的上游任务
	Dependencies []string
	// APIParamName 当 API 实际参数名与迭代键不同时使用
	// 例如 index_weight 通过 ts_code 列表迭代，但 API 参数名为 index_code
	APIParamName string
	// FixedParams 为该 API 请求固定追加的参数（例如 fields）
	FixedParams map[string]interface{}
	// FixedParamKeys 中的 key 将始终以 FixedParams 为准，上游调用方即便传了同名参数也会被忽略
	FixedParamKeys []string
	// IterateParams 需要迭代的参数及其值列表（如 news 的 src: ["sina","cls",...]）
	// 当非空时，Build 会生成 SyncMultiParamAPIData 任务
	IterateParams map[string][]string
	// RealtimeTsCodeChunkSize 实时模式下 ts_code 分批大小（0 表示不分批）
	RealtimeTsCodeChunkSize int
	// RealtimeTsCodeFormat 实时模式下 ts_code 格式（如 "000001.SZ"）
	RealtimeTsCodeFormat string
	// TimeWindow 若非空且 Enabled=true，则优先使用 GenerateDatetimeRange + GenerateTimeWindowSubTasks
	// 执行「时间窗口 + 模板任务」的同步策略。
	TimeWindow *APITimeWindowStrategy
}

// APISyncStrategyProvider 策略提供者接口
// 用于从外部（如数据库）获取 API 同步策略
type APISyncStrategyProvider interface {
	// GetStrategy 获取指定 API 的同步策略
	// 如果找不到，返回 nil（调用方应使用默认策略）
	GetStrategy(ctx context.Context, dataSourceID shared.ID, apiName string) (*APISyncStrategy, error)

	// GetStrategies 批量获取同步策略
	// 返回 map[apiName]strategy，不存在的 API 不会出现在 map 中
	GetStrategies(ctx context.Context, dataSourceID shared.ID, apiNames []string) (map[string]*APISyncStrategy, error)
}

// RepositoryStrategyProvider 基于仓储的策略提供者
type RepositoryStrategyProvider struct {
	repo metadata.Repository
}

// NewRepositoryStrategyProvider 创建基于仓储的策略提供者
func NewRepositoryStrategyProvider(repo metadata.Repository) *RepositoryStrategyProvider {
	return &RepositoryStrategyProvider{repo: repo}
}

// GetStrategy 从仓储获取策略
func (p *RepositoryStrategyProvider) GetStrategy(ctx context.Context, dataSourceID shared.ID, apiName string) (*APISyncStrategy, error) {
	entity, err := p.repo.GetAPISyncStrategyByAPIName(ctx, dataSourceID, apiName)
	if err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, nil
	}
	return convertEntityToStrategy(entity), nil
}

// GetStrategies 批量获取策略
func (p *RepositoryStrategyProvider) GetStrategies(ctx context.Context, dataSourceID shared.ID, apiNames []string) (map[string]*APISyncStrategy, error) {
	entities, err := p.repo.ListAPISyncStrategiesByAPINames(ctx, dataSourceID, apiNames)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*APISyncStrategy, len(entities))
	for _, entity := range entities {
		strategy := convertEntityToStrategy(entity)
		var iterateKeys []string
		for k := range strategy.IterateParams {
			iterateKeys = append(iterateKeys, k)
		}
		log.Printf("📋 [RepositoryStrategyProvider] 加载策略: API=%s, PreferredParam=%s, SupportDateRange=%v, RequiredParams=%v, Dependencies=%v, APIParamName=%s, FixedParams=%v, FixedParamKeys=%v, IterateParamsKeys=%v",
			entity.APIName,
			strategy.PreferredParam,
			strategy.SupportDateRange,
			strategy.RequiredParams,
			strategy.Dependencies,
			strategy.APIParamName,
			strategy.FixedParams,
			strategy.FixedParamKeys,
			iterateKeys,
		)
		result[entity.APIName] = strategy
	}
	return result, nil
}

// convertEntityToStrategy 将领域实体转换为工作流 DTO
// 注意：DB 中目前只存储了基础字段（preferred_param/support_date_range/required_params/dependencies/fixed_params），
// 新增的 APIParamName / IterateParams 等增强能力通过代码内置默认策略提供。
// 因此这里需要将「数据库策略」与「默认策略」进行合并：
//   - 优先使用数据库中的 PreferredParam / SupportDateRange（用户可通过 API 自定义）
//   - RequiredParams / Dependencies 取并集（避免丢失代码中新增加的依赖/必填项）
//   - APIParamName / IterateParams 始终来自默认策略（DB 目前没有对应字段）
func convertEntityToStrategy(entity *metadata.APISyncStrategy) *APISyncStrategy {
	if entity == nil {
		return nil
	}

	// 默认策略（代码内置，包含最新的 APIParamName / IterateParams 等信息）
	base := GetAPISyncStrategy(entity.APIName)

	// 合并 RequiredParams（并集，保持配置与默认的兼容性）
	required := mergeStringSlicesUnique(entity.RequiredParams, base.RequiredParams)

	// 合并 Dependencies（并集，避免丢失新增依赖，如 FetchIndexBasic）
	deps := mergeStringSlicesUnique(entity.Dependencies, base.Dependencies)

	// IterateParams：DB 有则用 entity，否则用 base
	iterateParams := base.IterateParams
	if len(entity.IterateParams) > 0 {
		iterateParams = entity.IterateParams
	}

	strategy := &APISyncStrategy{
		PreferredParam:          string(entity.PreferredParam),
		SupportDateRange:        entity.SupportDateRange,
		RequiredParams:          required,
		Dependencies:            deps,
		APIParamName:            base.APIParamName,
		FixedParams:             entity.FixedParams,
		FixedParamKeys:          entity.FixedParamKeys,
		IterateParams:           iterateParams,
		RealtimeTsCodeChunkSize: entity.RealtimeTsCodeChunkSize,
		RealtimeTsCodeFormat:    entity.RealtimeTsCodeFormat,
		TimeWindow:              base.TimeWindow,
	}

	// 如 fixed_params 中包含 time_window 配置，则覆盖默认的 TimeWindow
	if entity.FixedParams != nil {
		if twRaw, ok := entity.FixedParams["time_window"]; ok {
			switch v := twRaw.(type) {
			case map[string]interface{}:
				tw := &APITimeWindowStrategy{}
				if enabled, ok := v["enabled"].(bool); ok {
					tw.Enabled = enabled
				} else {
					tw.Enabled = true
				}
				if freq, ok := v["freq"].(string); ok {
					tw.Freq = freq
				}
				if key, ok := v["date_param_key"].(string); ok {
					tw.DateParamKey = key
				}
				if useCal, ok := v["use_trade_calendar"].(bool); ok {
					tw.UseTradeCalendar = useCal
				}
				strategy.TimeWindow = tw
			}
		}
	}

	return strategy
}

// mergeStringSlicesUnique 合并两个 string 切片并去重（保持先后顺序稳定）
func mergeStringSlicesUnique(a, b []string) []string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}

	seen := make(map[string]bool, len(a)+len(b))
	result := make([]string, 0, len(a)+len(b))

	for _, v := range a {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	for _, v := range b {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}

	return result
}

// defaultAPISyncStrategies 默认的 Tushare API 同步策略配置（作为回退）
// 当数据库中没有配置时使用这些默认值
//
// 根据 Tushare 官方文档中各 API 的必填参数来确定同步策略：
// - "none": 无必填参数，直接查询即可
// - "trade_date": 支持 trade_date 参数按日期查询全市场，效率最高
// - "ts_code": 必须提供 ts_code，需要按股票代码拆分任务
//
// SupportDateRange 说明：
// - true: API 支持 start_date + end_date 日期范围查询（如 daily）
// - false: API 只支持 trade_date 单日查询（如 weekly, monthly, top_list）
//
// 参考文档: https://tushare.pro/document/2
var defaultAPISyncStrategies = map[string]APISyncStrategy{
	// ========== 无必填参数（直接查询）==========
	"trade_cal":   {PreferredParam: "none", SupportDateRange: true, Dependencies: nil},
	"stock_basic": {PreferredParam: "none", Dependencies: nil},
	"namechange":  {PreferredParam: "none", SupportDateRange: true, Dependencies: nil},
	"index_basic": {PreferredParam: "none", RequiredParams: []string{"market"}, Dependencies: nil},
	"concept":     {PreferredParam: "none", Dependencies: nil}, // 概念股分类列表，ListConcepts 读此表
	"hs_const":    {PreferredParam: "none", RequiredParams: []string{"hs_type"}, Dependencies: nil},
	"stk_limit":   {PreferredParam: "none", SupportDateRange: true, Dependencies: nil},

	// ========== 支持 trade_date（按日期查询全市场）==========
	// daily：传入单日 trade_date 只能获取一天数据，需将 date range 扩展为多个 trade_date，通过 trade_cal 在 [start_date,end_date] 内截取交易日逐日拉取
	"daily":   {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	"weekly":  {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	"monthly": {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	// daily_basic：Tushare 单次最多返回 6000 条，用 start_date/end_date 大范围只拿到约一天数据，需按 trade_date 逐日拉取才能覆盖日期范围
	"daily_basic": {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	// adj_factor：同 daily，按 trade_date 从 trade_cal 截取日期范围逐日拉取
	"adj_factor":    {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	"top_list":      {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	"top_inst":      {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	"margin":        {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"margin_detail": {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"block_trade":   {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"index_daily":   {PreferredParam: "ts_code", SupportDateRange: true, Dependencies: []string{"FetchIndexBasic", "FetchTradeCal"}},
	"index_weight":  {PreferredParam: "ts_code", SupportDateRange: false, APIParamName: "index_code", Dependencies: []string{"FetchIndexBasic", "FetchTradeCal"}},

	// ========== 资金流向 API ==========
	"moneyflow_hsgt":    {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"moneyflow_ind_ths": {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"moneyflow_cnt_ths": {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"moneyflow_mkt_dc":  {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"moneyflow_ind_dc":  {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"moneyflow":         {PreferredParam: "ts_code", SupportDateRange: true, Dependencies: []string{"FetchStockBasic"}},
	"moneyflow_ths":     {PreferredParam: "ts_code", SupportDateRange: true, Dependencies: []string{"FetchStockBasic"}},
	"moneyflow_dc":      {PreferredParam: "ts_code", SupportDateRange: true, Dependencies: []string{"FetchStockBasic"}},

	// ========== 龙虎榜相关 API ==========
	"hsgt_top10":     {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"ggt_top10":      {PreferredParam: "trade_date", SupportDateRange: false, Dependencies: []string{"FetchTradeCal"}},
	"limit_list_d":   {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"limit_list_ths": {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}}, // 同花顺涨跌停榜单，涨停原因等详情优先用此表
	"limit_step":     {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}}, // 涨停连板天梯

	// ========== 同花顺概念板块 API ==========
	"ths_index":  {PreferredParam: "none", Dependencies: nil},
	"ths_daily":  {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"ths_member": {PreferredParam: "none", RequiredParams: []string{"ts_code"}, Dependencies: nil},

	// ========== 开盘啦题材数据 API ==========
	"kpl_list":         {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"kpl_concept":      {PreferredParam: "trade_date", SupportDateRange: true, Dependencies: []string{"FetchTradeCal"}},
	"kpl_concept_cons": {PreferredParam: "none", RequiredParams: []string{"ts_code"}, Dependencies: nil},

	// ========== 必须提供 ts_code（按股票拆分）==========
	"income":         {PreferredParam: "ts_code", Dependencies: []string{"FetchStockBasic"}},
	"balancesheet":   {PreferredParam: "ts_code", Dependencies: []string{"FetchStockBasic"}},
	"cashflow":       {PreferredParam: "ts_code", Dependencies: []string{"FetchStockBasic"}},
	"fina_indicator": {PreferredParam: "ts_code", Dependencies: []string{"FetchStockBasic"}},
	"fina_mainbz":    {PreferredParam: "ts_code", Dependencies: []string{"FetchStockBasic"}},

	// stk_mins：历史分钟数据，必填 ts_code + freq，支持 start_date/end_date（格式 yyyy-mm-dd HH:MM:SS），freq 默认 1min 在 job 内注入
	"stk_mins": {PreferredParam: "ts_code", SupportDateRange: true, RequiredParams: []string{"freq"}, Dependencies: []string{"FetchStockBasic"}},

	// ========== 新闻资讯 API ==========
	// news：快讯，必填 src + start_date + end_date，按 src 迭代
	"news": {
		PreferredParam:   "none",
		SupportDateRange: true,
		IterateParams: map[string][]string{
			"src": {"sina", "cls", "eastmoney", "10jqka", "wallstreetcn", "yuncaijing", "fenghuang", "jinrongjie", "yicai"},
		},
		TimeWindow: &APITimeWindowStrategy{
			Enabled:          true,
			Freq:             "D",
			DateParamKey:     "",
			UseTradeCalendar: false,
		},
	},
	// major_news：通讯，src 可选，按 src 迭代
	"major_news": {
		PreferredParam:   "none",
		SupportDateRange: true,
		IterateParams: map[string][]string{
			"src": {"新浪财经", "财联社", "新华网", "凤凰财经", "同花顺", "华尔街见闻", "中证网", "财新网", "第一财经"},
		},
		TimeWindow: &APITimeWindowStrategy{
			Enabled:          true,
			Freq:             "3H",
			DateParamKey:     "",
			UseTradeCalendar: false,
		},
	},
	// cctv_news：新闻联播，必填 date（YYYYMMDD），按自然日逐日
	"cctv_news": {
		PreferredParam:   "none",
		SupportDateRange: false,
		APIParamName:     "date",
		TimeWindow: &APITimeWindowStrategy{
			Enabled:          true,
			Freq:             "D",
			DateParamKey:     "date",
			UseTradeCalendar: false,
		},
	},
	// npr：政策法规库，支持 start_date/end_date
	"npr": {PreferredParam: "none", SupportDateRange: true},
	// anns_d：上市公司全量公告，支持 ann_date 或 start_date/end_date
	"anns_d": {PreferredParam: "trade_date", SupportDateRange: false, APIParamName: "ann_date", Dependencies: []string{"FetchTradeCal"}},
}

// GetAPISyncStrategy 获取 API 的同步策略（使用默认配置）
// 如果没有配置，默认返回 ts_code 策略（保守策略）
func GetAPISyncStrategy(apiName string) APISyncStrategy {
	return GetAPISyncStrategyWithFallback(apiName, nil)
}

// GetAPISyncStrategyWithFallback 获取 API 的同步策略，支持外部策略缓存
// strategies: 外部提供的策略缓存（通常从数据库加载），可以为 nil
func GetAPISyncStrategyWithFallback(apiName string, strategies map[string]*APISyncStrategy) APISyncStrategy {
	// 1. 先从外部策略缓存查找
	if strategies != nil {
		if strategy, ok := strategies[apiName]; ok && strategy != nil {
			return *strategy
		}
	}

	// 2. 从默认配置查找
	if strategy, ok := defaultAPISyncStrategies[apiName]; ok {
		return strategy
	}

	// 3. 返回保守的默认策略（需要按股票拆分）
	return APISyncStrategy{
		PreferredParam: "ts_code",
		Dependencies:   []string{"FetchStockBasic"},
	}
}

// APISyncConfig API 同步配置
// 定义单个 API 的同步方式和参数来源
type APISyncConfig struct {
	APIName        string                 // API 名称（必填）
	SyncMode       string                 // 同步模式: "direct" | "template"，默认根据 ParamKey 自动判断
	ParamKey       string                 // 模板任务的参数键名（如 "ts_code"），为空则使用 direct 模式
	UpstreamTask   string                 // 上游任务名称（用于获取参数列表或参数值）
	UpstreamParams map[string]interface{} // 上游参数映射配置（用于 direct 模式）
	ExtraParams    map[string]interface{} // 额外固定参数
	Dependencies   []string               // 依赖的任务列表（可选，默认自动推断）
}

// BatchDataSyncParams 批量数据同步工作流参数
type BatchDataSyncParams struct {
	DataSourceName string          // 数据源名称（必填）
	Token          string          // API Token（必填）
	TargetDBPath   string          // 目标数据库路径（必填）
	StartDate      string          // 开始日期（必填，格式: "20251201"）
	EndDate        string          // 结束日期（必填，格式: "20251231"）
	StartTime      string          // 开始时间（可选，格式: "09:30:00"）
	EndTime        string          // 结束时间（可选，格式: "15:00:00"）
	APINames       []string        // 需要同步的 API 列表（简单模式，兼容旧用法）
	APIConfigs     []APISyncConfig // API 同步配置（高级模式，优先使用）
	MaxStocks      int             // 最大股票数量（用于限制子任务，0=不限制）
	CommonDataAPIs []string        // 公共数据 API 名列表（SyncAPIDataJob 走 Cache→DuckDB→API）
	// SubtaskBatchSize：GenerateDataSyncSubTasks / GenerateTimeWindowSubTasks 每批 AtomicAdd 子任务数（0=使用 Job 默认 200）
	SubtaskBatchSize int
	// SyncMemHighMB / SyncMemCriticalMB：堆占用阈值（MB），超过则缩小批次并批间暂停（0=使用 Job 内默认或与 WriteQueue 对齐）
	SyncMemHighMB     int
	SyncMemCriticalMB int
	// StkMinsWindowFreq：stk_mins 分片时 GenerateDatetimeRange 的步长，如 "30D"、"14D"、"7D"；空则默认 30D
	StkMinsWindowFreq string
}

// Validate 验证参数
func (p *BatchDataSyncParams) Validate() error {
	if p.DataSourceName == "" {
		return ErrEmptyDataSourceName
	}
	if p.Token == "" {
		return ErrEmptyToken
	}
	if p.TargetDBPath == "" {
		return ErrEmptyTargetDBPath
	}
	// StartDate/EndDate 可选：仅当计划内 API 包含 date/time/dt 等参数时才由调用方保证非空
	// APIConfigs 或 APINames 至少有一个不为空
	if len(p.APINames) == 0 && len(p.APIConfigs) == 0 {
		return ErrEmptyAPINames
	}
	return nil
}

// GetStartDateTime 获取开始日期时间字符串
// 如果设置了 StartTime，返回 "20251201 09:30:00" 格式
// 否则返回纯日期 "20251201"
func (p *BatchDataSyncParams) GetStartDateTime() string {
	if p.StartTime != "" {
		return fmt.Sprintf("%s %s", p.StartDate, p.StartTime)
	}
	return p.StartDate
}

// GetEndDateTime 获取结束日期时间字符串
// 如果设置了 EndTime，返回 "20251231 15:00:00" 格式
// 否则返回纯日期 "20251231"
func (p *BatchDataSyncParams) GetEndDateTime() string {
	if p.EndTime != "" {
		return fmt.Sprintf("%s %s", p.EndDate, p.EndTime)
	}
	return p.EndDate
}

// BatchDataSyncWorkflowBuilder 批量数据同步工作流构建器
type BatchDataSyncWorkflowBuilder struct {
	registry                  task.FunctionRegistry
	params                    BatchDataSyncParams
	strategyCache             map[string]*APISyncStrategy // 策略缓存（从数据库加载）
	strategyProvider          APISyncStrategyProvider     // 策略提供者（可选）
	dataSourceID              shared.ID                   // 数据源 ID（用于从提供者获取策略）
	syncAPIDataJobTimeoutSec int // 秒；0 则 jobs.Effective 使用默认与下限
}

// NewBatchDataSyncWorkflowBuilder 创建批量数据同步工作流构建器
func NewBatchDataSyncWorkflowBuilder(registry task.FunctionRegistry) *BatchDataSyncWorkflowBuilder {
	return &BatchDataSyncWorkflowBuilder{
		registry: registry,
	}
}

// WithSyncAPIDataJobTimeout 设置 SyncAPIData / SyncMultiParamAPIData 单任务超时（秒），与 task_engine.task_timeout 对齐。
func (b *BatchDataSyncWorkflowBuilder) WithSyncAPIDataJobTimeout(sec int) *BatchDataSyncWorkflowBuilder {
	b.syncAPIDataJobTimeoutSec = sec
	return b
}

func (b *BatchDataSyncWorkflowBuilder) syncAPIDataJobTimeoutSeconds() int {
	return jobs.EffectiveSyncAPIDataJobTimeoutSeconds(b.syncAPIDataJobTimeoutSec)
}

// WithStrategyProvider 设置策略提供者和数据源 ID
// 使用提供者时，会在构建工作流时从数据库加载策略
func (b *BatchDataSyncWorkflowBuilder) WithStrategyProvider(provider APISyncStrategyProvider, dataSourceID shared.ID) *BatchDataSyncWorkflowBuilder {
	b.strategyProvider = provider
	b.dataSourceID = dataSourceID
	return b
}

// WithStrategyCache 直接设置策略缓存
// 用于已经从数据库加载好策略的场景
func (b *BatchDataSyncWorkflowBuilder) WithStrategyCache(cache map[string]*APISyncStrategy) *BatchDataSyncWorkflowBuilder {
	b.strategyCache = cache
	return b
}

// loadStrategies 加载策略到缓存
func (b *BatchDataSyncWorkflowBuilder) loadStrategies(ctx context.Context, apiNames []string) error {
	if b.strategyProvider == nil || b.dataSourceID == "" {
		return nil // 没有提供者，使用默认策略
	}

	strategies, err := b.strategyProvider.GetStrategies(ctx, b.dataSourceID, apiNames)
	if err != nil {
		return fmt.Errorf("failed to load strategies from provider: %w", err)
	}

	b.strategyCache = strategies
	return nil
}

// getStrategy 获取 API 的同步策略
func (b *BatchDataSyncWorkflowBuilder) getStrategy(apiName string) APISyncStrategy {
	return GetAPISyncStrategyWithFallback(apiName, b.strategyCache)
}

// checkDependency 检查待同步 API 列表中是否有任何 API 依赖指定的上游任务。
// 同时检查 APIConfigs 和 APINames 模式。
func (b *BatchDataSyncWorkflowBuilder) checkDependency(params BatchDataSyncParams, depName string) bool {
	var allAPINames []string
	if len(params.APIConfigs) > 0 {
		for _, config := range params.APIConfigs {
			allAPINames = append(allAPINames, config.APIName)
		}
	} else {
		allAPINames = params.APINames
	}
	for _, apiName := range allAPINames {
		strategy := b.getStrategy(apiName)
		for _, dep := range strategy.Dependencies {
			if dep == depName {
				return true
			}
		}
	}
	return false
}

// WithParams 设置工作流参数
func (b *BatchDataSyncWorkflowBuilder) WithParams(params BatchDataSyncParams) *BatchDataSyncWorkflowBuilder {
	b.params = params
	return b
}

// mergeSubtaskMemoryParams 将内存治理相关参数注入模板任务（与 jobs 包约定键名一致）。
func (b *BatchDataSyncWorkflowBuilder) mergeSubtaskMemoryParams(dst map[string]interface{}) {
	if dst == nil {
		return
	}
	if b.params.SubtaskBatchSize > 0 {
		dst["subtask_batch_size"] = b.params.SubtaskBatchSize
	}
	if b.params.SyncMemHighMB > 0 {
		dst["sync_mem_high_mb"] = b.params.SyncMemHighMB
	}
	if b.params.SyncMemCriticalMB > 0 {
		dst["sync_mem_critical_mb"] = b.params.SyncMemCriticalMB
	}
}

// WithDataSource 设置数据源和 Token
func (b *BatchDataSyncWorkflowBuilder) WithDataSource(name, token string) *BatchDataSyncWorkflowBuilder {
	b.params.DataSourceName = name
	b.params.Token = token
	return b
}

// WithTargetDB 设置目标数据库路径
func (b *BatchDataSyncWorkflowBuilder) WithTargetDB(path string) *BatchDataSyncWorkflowBuilder {
	b.params.TargetDBPath = path
	return b
}

// WithDateRange 设置同步日期范围（纯日期）
func (b *BatchDataSyncWorkflowBuilder) WithDateRange(startDate, endDate string) *BatchDataSyncWorkflowBuilder {
	b.params.StartDate = startDate
	b.params.EndDate = endDate
	return b
}

// WithTimeRange 设置同步时间范围（可选）
// 时间格式: "09:30:00" 或 "093000"
func (b *BatchDataSyncWorkflowBuilder) WithTimeRange(startTime, endTime string) *BatchDataSyncWorkflowBuilder {
	b.params.StartTime = startTime
	b.params.EndTime = endTime
	return b
}

// WithDateTimeRange 同时设置日期和时间范围
func (b *BatchDataSyncWorkflowBuilder) WithDateTimeRange(startDate, startTime, endDate, endTime string) *BatchDataSyncWorkflowBuilder {
	b.params.StartDate = startDate
	b.params.StartTime = startTime
	b.params.EndDate = endDate
	b.params.EndTime = endTime
	return b
}

// WithAPIs 设置需要同步的 API 列表（必填）
func (b *BatchDataSyncWorkflowBuilder) WithAPIs(apis ...string) *BatchDataSyncWorkflowBuilder {
	b.params.APINames = apis
	return b
}

// WithMaxStocks 设置最大股票数量（限制子任务数量）
func (b *BatchDataSyncWorkflowBuilder) WithMaxStocks(max int) *BatchDataSyncWorkflowBuilder {
	b.params.MaxStocks = max
	return b
}

// WithAPIConfigs 设置 API 同步配置（高级模式）
// 使用此方法可以精确控制每个 API 的同步方式、参数来源等
func (b *BatchDataSyncWorkflowBuilder) WithAPIConfigs(configs ...APISyncConfig) *BatchDataSyncWorkflowBuilder {
	b.params.APIConfigs = configs
	return b
}

// WithCommonDataAPIs 设置公共数据 API 名列表（SyncAPIDataJob 将对这些 API 走 Cache→DuckDB→API）
func (b *BatchDataSyncWorkflowBuilder) WithCommonDataAPIs(apis []string) *BatchDataSyncWorkflowBuilder {
	b.params.CommonDataAPIs = apis
	return b
}

// AddAPIConfig 添加单个 API 同步配置
func (b *BatchDataSyncWorkflowBuilder) AddAPIConfig(config APISyncConfig) *BatchDataSyncWorkflowBuilder {
	b.params.APIConfigs = append(b.params.APIConfigs, config)
	return b
}

// AddDirectSyncAPI 添加直接同步的 API（非模板任务）
// 适用于不需要按 ts_code 拆分的 API，如 trade_cal, top_list 等
func (b *BatchDataSyncWorkflowBuilder) AddDirectSyncAPI(apiName string, upstreamTask string, upstreamParams map[string]interface{}, dependencies ...string) *BatchDataSyncWorkflowBuilder {
	config := APISyncConfig{
		APIName:        apiName,
		SyncMode:       "direct",
		UpstreamTask:   upstreamTask,
		UpstreamParams: upstreamParams,
		Dependencies:   dependencies,
	}
	return b.AddAPIConfig(config)
}

// AddTemplateSyncAPI 添加模板同步的 API（按参数拆分子任务）
// 适用于需要按 ts_code 拆分的 API，如 daily, adj_factor 等
func (b *BatchDataSyncWorkflowBuilder) AddTemplateSyncAPI(apiName, paramKey, upstreamTask string, extraParams map[string]interface{}, dependencies ...string) *BatchDataSyncWorkflowBuilder {
	config := APISyncConfig{
		APIName:      apiName,
		SyncMode:     "template",
		ParamKey:     paramKey,
		UpstreamTask: upstreamTask,
		ExtraParams:  extraParams,
		Dependencies: dependencies,
	}
	return b.AddAPIConfig(config)
}

// Build 构建批量数据同步工作流
//
// 工作流结构：
// Level 0（并行执行）：
//   - FetchTradeCal - 获取交易日历
//   - FetchStockBasic - 获取股票基础信息
//
// Level 1（依赖 Level 0，根据 APIConfigs 或 APINames 动态生成）：
//   - 模板任务：按 ts_code 拆分的 API（如 daily, adj_factor）
//   - 直接任务：不需要拆分的 API（如 top_list, index_daily）
//
// 事务支持：启用 SAGA 事务，同步失败时按 sync_batch_id 回滚数据
//
// 参数占位符支持：如果参数为空，将使用占位符（如 ${data_source_name}），
// 执行时通过 workflow.ReplaceParams() 替换为实际值
func (b *BatchDataSyncWorkflowBuilder) Build() (*workflow.Workflow, error) {
	params := b.params

	// 检查是否使用占位符模式（所有必填参数都为空）
	usePlaceholders := params.DataSourceName == "" && params.Token == "" &&
		params.TargetDBPath == "" && params.StartDate == "" && params.EndDate == "" &&
		len(params.APINames) == 0 && len(params.APIConfigs) == 0

	// 仅在非占位符模式下验证参数
	if !usePlaceholders {
		if err := params.Validate(); err != nil {
			return nil, err
		}
	}

	var tasks []*task.Task
	var depNames []string // 所有同步任务名，用于 BatchSyncComplete 依赖
	addedTaskNames := make(map[string]bool)

	// 如果参数为空，使用占位符
	dataSourceName := params.DataSourceName
	if dataSourceName == "" {
		dataSourceName = "${data_source_name}"
	}
	token := params.Token
	if token == "" {
		token = "${token}"
	}
	targetDBPath := params.TargetDBPath
	if targetDBPath == "" {
		targetDBPath = "${target_db_path}"
	}
	startDate := params.GetStartDateTime()
	if startDate == "" && usePlaceholders {
		startDate = "${start_date}"
	}
	endDate := params.GetEndDateTime()
	if endDate == "" && usePlaceholders {
		endDate = "${end_date}"
	}

	// 基础参数（含 common_data_apis 供 SyncAPIDataJob 走缓存）
	// 自动补全 stock_basic/trade_cal 到 common_data_apis，与 Job 层 implicitCommonDataAPIs 双保险
	// 预序列化为 JSON 字符串，避免 task-engine builder 将 []string 转为 Go fmt 格式 [val1 val2]
	// 导致 Job 端 convertToStringSlice JSON 解析失败
	effectiveCommonAPIs := ensureImplicitCommonAPIs(params.CommonDataAPIs)
	var commonDataAPIsParam interface{} = effectiveCommonAPIs
	if len(effectiveCommonAPIs) > 0 {
		if b, err := json.Marshal(effectiveCommonAPIs); err == nil {
			commonDataAPIsParam = string(b)
		}
	}
	baseParams := map[string]interface{}{
		"data_source_name": dataSourceName,
		"token":            token,
		"target_db_path":   targetDBPath,
		"common_data_apis": commonDataAPIsParam,
	}

	// 日期时间参数（支持可选时间）
	dateTimeParams := map[string]interface{}{
		"start_date": startDate,
		"end_date":   endDate,
	}

	// ==================== Level 0: 基础数据获取 ====================

	// 处理日期参数（交易日历只用日期，不使用时间）
	startDateOnly := params.StartDate
	if startDateOnly == "" {
		startDateOnly = "${start_date}"
	}
	endDateOnly := params.EndDate
	if endDateOnly == "" {
		endDateOnly = "${end_date}"
	}

	// Task: 获取交易日历（全量数据，不限制日期范围）
	// 始终创建：implicitCommonDataAPIs 保证优先从 DuckDB 短路读取（毫秒级），不会超时
	fetchTradeCalTask, err := builder.NewTaskBuilder("FetchTradeCal", "获取交易日历", b.registry).
		WithJobFunction("SyncAPIData", mergeParams(baseParams, map[string]interface{}{
			"api_name": "trade_cal",
			"params":   map[string]interface{}{},
		})).
		WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, fetchTradeCalTask)
	depNames = append(depNames, "FetchTradeCal")
	addedTaskNames["FetchTradeCal"] = true

	// Task: 获取股票基础信息（含上市 L 与退市 D，便于数据质量按 delist_date 算有效区间）
	// 始终创建：implicitCommonDataAPIs 保证优先从 DuckDB 短路读取（毫秒级），不会超时
	fetchStockBasicTask, err := builder.NewTaskBuilder("FetchStockBasic", "获取股票基础信息", b.registry).
		WithJobFunction("SyncAPIData", mergeParams(baseParams, map[string]interface{}{
			"api_name": "stock_basic",
			"params": map[string]interface{}{
				"list_status": "L,D",
				"fields":      "ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type",
			},
		})).
		WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, fetchStockBasicTask)
	depNames = append(depNames, "FetchStockBasic")
	addedTaskNames["FetchStockBasic"] = true

	// Task: 获取指数基础信息（多市场：SSE/SZSE/CSI/SW/OTH，条件添加）
	// 仅当待同步 API 列表中存在依赖 FetchIndexBasic 的 API 时才添加
	needsIndexBasic := b.checkDependency(params, "FetchIndexBasic")
	if needsIndexBasic {
		fetchIndexBasicTask, err := builder.NewTaskBuilder("FetchIndexBasic", "获取指数基础信息（多市场）", b.registry).
			WithJobFunction("SyncMultiParamAPIData", mergeParams(baseParams, map[string]interface{}{
				"api_name":       "index_basic",
				"iterate_param":  "market",
				"iterate_values": []string{"SSE", "SZSE", "CSI", "SW", "OTH"},
			})).
			WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
			WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
			WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
			WithCompensationFunction("CompensateSyncData").
			Build()
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, fetchIndexBasicTask)
		depNames = append(depNames, "FetchIndexBasic")
	}

	// ==================== Level 1: 数据同步任务 ====================

	// 优先使用 APIConfigs（高级配置模式）
	if len(params.APIConfigs) > 0 {
		// 加载策略（如果有策略提供者），使 FixedParams/FixedParamKeys 在高级配置模式中也能生效
		ctx := context.Background()
		var apiNames []string
		for _, config := range params.APIConfigs {
			apiNames = append(apiNames, config.APIName)
		}
		if err := b.loadStrategies(ctx, apiNames); err != nil {
			log.Printf("⚠️ [BuildWorkflow] 加载策略失败（APIConfigs），将使用默认策略: %v", err)
		}

		for _, config := range params.APIConfigs {
			// 与简单模式一致：Level 0 已 always 建 FetchTradeCal/FetchStockBasic，勿再生成 Sync_*，否则并行重复请求同一接口（易触发任务引擎短超时）。
			if config.APIName == "trade_cal" || config.APIName == "stock_basic" {
				log.Printf("⏭️ [BuildWorkflow] APIConfigs 跳过基础 API（已由 Fetch* 覆盖）: %s", config.APIName)
				continue
			}
			// 优先处理时间窗口模板策略
			strategy := b.getStrategy(config.APIName)
			if strategy.TimeWindow != nil && strategy.TimeWindow.Enabled {
				twTasks, twDepName, err := b.buildTimeWindowTasks(config.APIName, strategy, baseParams, startDate, endDate, params.MaxStocks)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, twTasks...)
				depNames = append(depNames, twDepName)
				continue
			}

			config = filterMissingBaseDependencies(config, addedTaskNames)
			if config.APIName == "stk_mins" && isStkMinsTemplateAPIConfig(config) {
				upstreamTask := config.UpstreamTask
				if upstreamTask == "" {
					upstreamTask = "FetchStockBasic"
				}
				paramKey := config.ParamKey
				if paramKey == "" {
					paramKey = "ts_code"
				}
				stkTasks, depName, errStk := b.buildStkMinsChunkedSyncTasks(baseParams, dateTimeParams, config.ExtraParams, upstreamTask, paramKey, params.MaxStocks)
				if errStk != nil {
					return nil, errStk
				}
				tasks = append(tasks, stkTasks...)
				addedTaskNames["Sync_"+config.APIName] = true
				depNames = append(depNames, depName)
				continue
			}
			syncTask, err := b.buildAPITask(config, baseParams, dateTimeParams)
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, syncTask)
			addedTaskNames["Sync_"+config.APIName] = true
			depNames = append(depNames, "Sync_"+config.APIName)
		}
	} else {
		// 使用 APINames（简单模式）+ 智能策略选择
		// 根据每个 API 的特性自动选择最优同步策略
		apiNames := params.APINames
		if len(apiNames) == 0 {
			// 如果为空，使用占位符
			apiNames = []string{"${api_names}"}
		}

		// 加载策略（如果有策略提供者）
		ctx := context.Background()
		if err := b.loadStrategies(ctx, apiNames); err != nil {
			log.Printf("⚠️ [BuildWorkflow] 加载策略失败，将使用默认策略: %v", err)
		}

		for _, apiName := range apiNames {
			// 跳过基础数据（已在 Level 0 处理）
			if apiName == "trade_cal" || apiName == "stock_basic" || apiName == "index_basic" {
				log.Printf("⏭️ [BuildWorkflow] 跳过基础数据 API: %s", apiName)
				continue
			}

			// 获取 API 同步策略（优先使用缓存的策略）
			strategy := b.getStrategy(apiName)
			taskName := "Sync_" + apiName
			log.Printf("🔨 [BuildWorkflow] 开始构建任务: API=%s, TaskName=%s, PreferredParam=%s", apiName, taskName, strategy.PreferredParam)

			// 优先处理时间窗口模板策略：GenerateDatetimeRange + GenerateTimeWindowSubTasks
			if strategy.TimeWindow != nil && strategy.TimeWindow.Enabled {
				twTasks, twDepName, err := b.buildTimeWindowTasks(apiName, strategy, baseParams, startDate, endDate, params.MaxStocks)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, twTasks...)
				depNames = append(depNames, twDepName)
				continue
			}

			var syncTask *task.Task
			var err error

			switch strategy.PreferredParam {
			case "trade_date":
				// 支持 trade_date 的 API：使用 direct 模式，按日期查询全市场数据
				// 根据 SupportDateRange 决定使用日期范围还是单日查询
				if strategy.SupportDateRange {
					// 支持日期范围查询：使用 start_date + end_date
					apiParams := dateTimeParams
					log.Printf("🔧 [BuildWorkflow] API=%s, SupportDateRange=true, params=%v", apiName, apiParams)

					syncTask, err = builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（按日期）", b.registry).
						WithJobFunction("SyncAPIData", mergeParams(baseParams, map[string]interface{}{
							"api_name": apiName,
							"params":   apiParams,
						})).
						WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
						WithDependency("FetchTradeCal"). // 依赖交易日历
						WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
						WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
						WithCompensationFunction("CompensateSyncData").
						Build()
				} else {
					// 只支持单日查询
					// cctv_news 使用 GenerateDatetimeRange + GenerateTimeWindowSubTasks，按自然日（YYYYMMDD）逐日同步
					if apiName == "cctv_news" {
						rangeTaskName := "GenerateCCTVNewsDateRange"
						rangeParams := map[string]interface{}{
							"start":      startDateOnly,
							"end":        endDateOnly,
							"freq":       "D",
							"inclusive":  "both",
							"as_windows": true,
						}
						rangeTask, errRange := builder.NewTaskBuilder(rangeTaskName, "生成 cctv_news 日期窗口", b.registry).
							WithJobFunction("GenerateDatetimeRange", rangeParams).
							WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
							WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
							Build()
						if errRange != nil {
							log.Printf("❌ [BuildWorkflow] 构建 cctv_news 日期窗口任务失败: TaskName=%s, Error=%v", rangeTaskName, errRange)
							return nil, errRange
						}
						tasks = append(tasks, rangeTask)

						templateParams := mergeParams(baseParams, map[string]interface{}{
							"api_name":       apiName,
							"upstream_task":  rangeTaskName,
							"window_field":   "windows",
							"date_param_key": "date",
							"max_sub_tasks":  params.MaxStocks,
						})
						b.mergeSubtaskMemoryParams(templateParams)

						templateTask, errTpl := builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（按自然日逐日）", b.registry).
							WithJobFunction("GenerateTimeWindowSubTasks", templateParams).
							WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
							WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
							WithTemplate(true).
							WithCompensationFunction("CompensateSyncData").
							Build()
						if errTpl != nil {
							log.Printf("❌ [BuildWorkflow] 构建 cctv_news 模板任务失败: TaskName=%s, Error=%v", taskName, errTpl)
							return nil, errTpl
						}
						tasks = append(tasks, templateTask)
						depNames = append(depNames, taskName)
						log.Printf("✅ [BuildWorkflow] 任务构建成功（日期窗口 + 模板）: API=%s, TaskName=%s", apiName, taskName)
						continue
					}

					// 其他仅支持单日查询的 trade_date API：使用模板任务遍历交易日历中的每个交易日
					// 从 FetchTradeCal 任务中提取交易日列表，为每个交易日生成子任务
					// APIParamName 如 "date"(cctv_news)、"ann_date"(anns_d) 表示 API 实际参数名
					paramKey := "trade_date"
					if strategy.APIParamName != "" {
						paramKey = strategy.APIParamName
					}
					taskParams := map[string]interface{}{
						"api_name":      apiName,
						"param_key":     paramKey,
						"upstream_task": "FetchTradeCal",
						"max_sub_tasks": params.MaxStocks,
						"start_date":    startDateOnly,
						"end_date":      endDateOnly,
					}
					if paramKey != "trade_date" {
						taskParams["upstream_param_key"] = "trade_date"
					}
					log.Printf("🔧 [BuildWorkflow] API=%s, SupportDateRange=false, 使用模板任务遍历交易日, param_key=%s", apiName, paramKey)

					mergedTradeTpl := mergeParams(baseParams, taskParams)
					b.mergeSubtaskMemoryParams(mergedTradeTpl)
					syncTask, err = builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（按交易日）", b.registry).
						WithJobFunction("GenerateDataSyncSubTasks", mergedTradeTpl).
						WithDependency("FetchTradeCal"). // 依赖交易日历
						WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
						WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
						WithTemplate(true). // 标记为模板任务
						WithCompensationFunction("CompensateSyncData").
						Build()
				}

			case "none":
				// 有 IterateParams 的 API（如自定义多源迭代）：使用 SyncMultiParamAPIData 多参数迭代
				if len(strategy.IterateParams) > 0 {
					var iterateParam string
					var iterateValues []string
					for k, v := range strategy.IterateParams {
						iterateParam = k
						iterateValues = v
						break
					}
					if iterateParam != "" && len(iterateValues) > 0 {
						dateParams := map[string]interface{}{}
						if strategy.SupportDateRange {
							dateParams["start_date"] = startDate
							dateParams["end_date"] = endDate
						}
						// 将策略层的 FixedParams 与日期参数合并；若 key 在 FixedParamKeys 中，则以策略为准
						fixedParams := mergeParamsWithStrategy(strategy, dateParams, nil)
						log.Printf("🔧 [BuildWorkflow] API=%s, IterateParams=%s, values=%v, fixedParams=%v", apiName, iterateParam, iterateValues, fixedParams)
						syncTask, err = builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（多源迭代）", b.registry).
							WithJobFunction("SyncMultiParamAPIData", mergeParams(baseParams, map[string]interface{}{
								"api_name":       apiName,
								"iterate_param":  iterateParam,
								"iterate_values": iterateValues,
								"params":         fixedParams,
							})).
							WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
							WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
							WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
							WithCompensationFunction("CompensateSyncData").
							Build()
						if err == nil {
							tasks = append(tasks, syncTask)
							depNames = append(depNames, taskName)
						}
						if err != nil {
							log.Printf("❌ [BuildWorkflow] 构建任务失败: API=%s, TaskName=%s, Error=%v", apiName, taskName, err)
							return nil, err
						}
						log.Printf("✅ [BuildWorkflow] 任务构建成功: API=%s, TaskName=%s", apiName, taskName)
						continue
					}
				}

				// 基础数据 API：直接查询，不需要拆分
				// 但需要检查是否有额外的必填参数
				apiParams := map[string]interface{}{}

				// 处理有额外必填参数的 API
				// 这些参数值需要根据 API 特性预设
				switch apiName {
				case "hs_const":
					// hs_const 需要 hs_type 参数，有 SH（沪股通）和 SZ（深股通）两种
					// 这里只同步沪股通，如需两者都同步，需要创建两个任务
					apiParams["hs_type"] = "SH"
				// index_basic 由 Level 0 的 FetchIndexBasic 处理，此处已 skip
				case "stk_limit":
					// stk_limit 支持 trade_date 参数，使用日期范围查询更高效
					apiParams["start_date"] = params.StartDate
					apiParams["end_date"] = params.EndDate
				case "npr":
					// npr 政策法规库支持 start_date/end_date
					apiParams["start_date"] = startDate
					apiParams["end_date"] = endDate
				}

				syncTask, err = builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（直接查询）", b.registry).
					WithJobFunction("SyncAPIData", mergeParams(baseParams, map[string]interface{}{
						"api_name": apiName,
						"params":   apiParams,
					})).
					WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
					WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
					WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
					WithCompensationFunction("CompensateSyncData").
					Build()

			case "ts_code":
				// 按代码迭代的 API：使用 template 模式生成子任务
				// stk_mins 的 freq 在 GenerateDataSyncSubTasks/SyncAPIData 中默认注入为 1min，故不跳过
				// 对于 RequiredParams，仅当存在我们无法自动提供的参数时才跳过：
				//   - 若 RequiredParam 与 APIParamName 相同（如 index_weight 的 index_code），视为由模板参数提供；
				//   - 若 APIParamName 为空且 RequiredParam 为 "ts_code"，视为由模板参数提供；
				//   - 其他 RequiredParam 会导致跳过（需要用户通过高级配置显式指定）。
				if apiName != "stk_mins" {
					skippable := false
					for _, reqParam := range strategy.RequiredParams {
						// 由模板参数提供的字段：ts_code 或 APIParamName 本身
						if reqParam == "ts_code" && (strategy.APIParamName == "" || strategy.APIParamName == "ts_code") {
							continue
						}
						if strategy.APIParamName != "" && reqParam == strategy.APIParamName {
							continue
						}
						// 其他必填参数当前无法自动提供，需要用户在 APIConfigs 中配置，跳过简单模式
						skippable = true
						break
					}
					if skippable {
						log.Printf("⏭️ [BuildWorkflow] 跳过 ts_code 模式 API（存在无法自动提供的必填参数）: API=%s, RequiredParams=%v, APIParamName=%s", apiName, strategy.RequiredParams, strategy.APIParamName)
						continue
					}
				}

				// 确定上游任务和参数键
				upstreamTask := "FetchStockBasic"
				for _, dep := range strategy.Dependencies {
					if dep == "FetchIndexBasic" {
						upstreamTask = "FetchIndexBasic"
						break
					}
				}

				apiParamKey := "ts_code"
				if strategy.APIParamName != "" {
					apiParamKey = strategy.APIParamName
				}

				taskParams := mergeParams(mergeParams(baseParams, dateTimeParams), map[string]interface{}{
					"api_name":      apiName,
					"param_key":     apiParamKey,
					"upstream_task": upstreamTask,
					"max_sub_tasks": params.MaxStocks,
				})
				if apiParamKey != "ts_code" {
					taskParams["upstream_param_key"] = "ts_code"
				}
				b.mergeSubtaskMemoryParams(taskParams)

				if apiName == "stk_mins" {
					stkTasks, depName, errStk := b.buildStkMinsChunkedSyncTasks(baseParams, dateTimeParams, nil, upstreamTask, apiParamKey, params.MaxStocks)
					if errStk != nil {
						log.Printf("❌ [BuildWorkflow] 构建 stk_mins 分片任务失败: %v", errStk)
						return nil, errStk
					}
					tasks = append(tasks, stkTasks...)
					addedTaskNames[taskName] = true
					depNames = append(depNames, depName)
					log.Printf("✅ [BuildWorkflow] 任务构建成功: API=%s, TaskName=%s（stk_mins 分片时间窗）", apiName, taskName)
					continue
				}

				taskDesc := "同步" + apiName + "数据（按代码）"
				syncTask, err = builder.NewTaskBuilder(taskName, taskDesc, b.registry).
					WithJobFunction("GenerateDataSyncSubTasks", taskParams).
					WithDependency(upstreamTask).
					WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
					WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
					WithTemplate(true).
					Build()

			default:
				// 其他未知策略：使用 template 模式，按股票代码生成子任务
				defTpl := mergeParams(mergeParams(baseParams, dateTimeParams), map[string]interface{}{
					"api_name":      apiName,
					"param_key":     "ts_code",
					"upstream_task": "FetchStockBasic",
					"max_sub_tasks": params.MaxStocks,
				})
				b.mergeSubtaskMemoryParams(defTpl)
				syncTask, err = builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（按股票）", b.registry).
					WithJobFunction("GenerateDataSyncSubTasks", defTpl).
					WithDependency("FetchStockBasic"). // 依赖股票基础信息以获取 ts_codes
					WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
					WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
					WithTemplate(true). // 标记为模板任务
					Build()
			}

			if err != nil {
				log.Printf("❌ [BuildWorkflow] 构建任务失败: API=%s, TaskName=%s, Error=%v", apiName, taskName, err)
				return nil, err
			}
			log.Printf("✅ [BuildWorkflow] 任务构建成功: API=%s, TaskName=%s", apiName, taskName)
			tasks = append(tasks, syncTask)
			addedTaskNames[taskName] = true
			depNames = append(depNames, taskName)
		}
	}

	// BatchSyncComplete：依赖所有同步任务，成功/失败均触发 DataSyncCompleteHandler → execution 回调 → Plan.MarkCompleted
	completeTaskBuilder := builder.NewTaskBuilder("BatchSyncComplete", "批量同步完成（触发 execution 回调）", b.registry).
		WithJobFunction("NotifySyncComplete", map[string]interface{}{}).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncComplete").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncComplete")
	for _, dep := range depNames {
		completeTaskBuilder = completeTaskBuilder.WithDependency(dep)
	}
	completeTask, err := completeTaskBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("build BatchSyncComplete task: %w", err)
	}
	tasks = append(tasks, completeTask)

	// 构建工作流
	wfBuilder := builder.NewWorkflowBuilder("BatchDataSync", "批量数据同步工作流 - 支持用户指定时间区间和 API")
	for _, t := range tasks {
		wfBuilder.WithTask(t)
	}

	wf, err := wfBuilder.Build()
	if err != nil {
		return nil, err
	}

	// 启用 SAGA 事务
	wf.SetTransactional(true)

	return wf, nil
}

// buildTimeWindowTasks 为指定 API 构建时间窗口任务链（GenerateDatetimeRange → GenerateTimeWindowSubTasks）。
// 返回需要添加到 workflow 的任务列表和模板任务名（用作下游依赖）。
// 调用方需确保 strategy.TimeWindow != nil && strategy.TimeWindow.Enabled == true。
func (b *BatchDataSyncWorkflowBuilder) buildTimeWindowTasks(
	apiName string,
	strategy APISyncStrategy,
	baseParams map[string]interface{},
	startDate, endDate string,
	maxStocks int,
) ([]*task.Task, string, error) {
	rangeTaskName := fmt.Sprintf("Generate_%s_TimeWindow", apiName)
	taskName := "Sync_" + apiName

	rangeParams := map[string]interface{}{
		"start":      startDate,
		"end":        endDate,
		"freq":       strategy.TimeWindow.Freq,
		"inclusive":  "both",
		"as_windows": true,
	}
	rangeTask, err := builder.NewTaskBuilder(rangeTaskName, "生成"+apiName+"时间窗口", b.registry).
		WithJobFunction("GenerateDatetimeRange", rangeParams).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, "", fmt.Errorf("build time-window task for %s: %w", apiName, err)
	}

	templateParams := mergeParams(baseParams, map[string]interface{}{
		"api_name":      apiName,
		"upstream_task": rangeTaskName,
		"window_field":  "windows",
		"max_sub_tasks": maxStocks,
	})
	// task-engine builder 会将非 string 类型用 fmt.Sprintf 转为字符串，
	// 导致 []string/map 变成不可逆的 Go 格式。这里提前 JSON 编码，job 端再解码。
	if len(strategy.FixedParams) > 0 {
		if fpJSON, err := json.Marshal(strategy.FixedParams); err == nil {
			templateParams["fixed_params"] = string(fpJSON)
		}
	}
	if srcVals, ok := strategy.IterateParams["src"]; ok && len(srcVals) > 0 {
		if svJSON, err := json.Marshal(srcVals); err == nil {
			templateParams["src_values"] = string(svJSON)
		}
	}
	if strategy.TimeWindow.DateParamKey != "" {
		templateParams["date_param_key"] = strategy.TimeWindow.DateParamKey
	}
	b.mergeSubtaskMemoryParams(templateParams)

	templateTask, err := builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（时间窗口模板）", b.registry).
		WithJobFunction("GenerateTimeWindowSubTasks", templateParams).
		WithDependency(rangeTaskName).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithTemplate(true).
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, "", fmt.Errorf("build time-window template task for %s: %w", apiName, err)
	}

	log.Printf("✅ [BuildWorkflow] 任务构建成功（时间窗口 + 模板）: API=%s, TaskName=%s, Freq=%s", apiName, taskName, strategy.TimeWindow.Freq)
	return []*task.Task{rangeTask, templateTask}, taskName, nil
}

func isStkMinsTemplateAPIConfig(config APISyncConfig) bool {
	if strings.EqualFold(strings.TrimSpace(config.SyncMode), "template") {
		return true
	}
	return strings.TrimSpace(config.ParamKey) != ""
}

func stkMinsDateRangeFromMap(dateTimeParams map[string]interface{}) (startRaw, endRaw string, err error) {
	if dateTimeParams == nil {
		return "", "", fmt.Errorf("dateTimeParams is nil")
	}
	s, ok := stringParamFromMap(dateTimeParams, "start_date")
	if !ok {
		return "", "", fmt.Errorf("start_date is required for stk_mins chunked sync")
	}
	e, ok := stringParamFromMap(dateTimeParams, "end_date")
	if !ok {
		return "", "", fmt.Errorf("end_date is required for stk_mins chunked sync")
	}
	return s, e, nil
}

func stringParamFromMap(m map[string]interface{}, key string) (string, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", false
	}
	switch s := v.(type) {
	case string:
		s = strings.TrimSpace(s)
		return s, s != ""
	default:
		out := strings.TrimSpace(fmt.Sprintf("%v", v))
		return out, out != ""
	}
}

// buildStkMinsChunkedSyncTasks 构建 GenerateDatetimeRange（默认 30D 窗口，可由 StkMinsWindowFreq 改为 14D/7D 等）+ GenerateDataSyncSubTasks（窗口×ts_code）任务链。
func (b *BatchDataSyncWorkflowBuilder) buildStkMinsChunkedSyncTasks(
	baseParams, dateTimeParams map[string]interface{},
	extraParams map[string]interface{},
	upstreamTask, paramKey string,
	maxStocks int,
) ([]*task.Task, string, error) {
	startRaw, endRaw, err := stkMinsDateRangeFromMap(dateTimeParams)
	if err != nil {
		return nil, "", err
	}
	rangeStart, rangeEnd, err := jobs.StkMinsGenerateDatetimeRangeStepSpan(startRaw, endRaw)
	if err != nil {
		return nil, "", fmt.Errorf("stk_mins datetime span: %w", err)
	}
	const rangeTaskName = "Generate_stk_mins_TimeWindow"
	taskName := "Sync_stk_mins"
	winFreq := strings.TrimSpace(b.params.StkMinsWindowFreq)
	if winFreq == "" {
		winFreq = "30D"
	}
	rangeParams := map[string]interface{}{
		"start":      rangeStart,
		"end":        rangeEnd,
		"freq":       winFreq,
		"inclusive":  "both",
		"as_windows": true,
	}
	rangeTask, err := builder.NewTaskBuilder(rangeTaskName, "生成 stk_mins "+winFreq+" 时间窗", b.registry).
		WithJobFunction("GenerateDatetimeRange", rangeParams).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, "", fmt.Errorf("build stk_mins range task: %w", err)
	}
	templateParams := mergeParams(mergeParams(baseParams, dateTimeParams), map[string]interface{}{
		"api_name":                "stk_mins",
		"param_key":               paramKey,
		"upstream_task":           upstreamTask,
		"max_sub_tasks":           maxStocks,
		"datetime_range_upstream": rangeTaskName,
		"window_field":            "windows",
	})
	if paramKey != "ts_code" {
		templateParams["upstream_param_key"] = "ts_code"
	}
	if len(extraParams) > 0 {
		templateParams = mergeParams(templateParams, extraParams)
	}
	b.mergeSubtaskMemoryParams(templateParams)
	templateTask, err := builder.NewTaskBuilder(taskName, "同步 stk_mins 数据（按代码与时间窗）", b.registry).
		WithJobFunction("GenerateDataSyncSubTasks", templateParams).
		WithDependency(rangeTaskName).
		WithDependency(upstreamTask).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithTemplate(true).
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, "", fmt.Errorf("build stk_mins template: %w", err)
	}
	log.Printf("✅ [BuildWorkflow] stk_mins: 已构建 %s 时间窗 + 按代码模板", winFreq)
	return []*task.Task{rangeTask, templateTask}, taskName, nil
}

// buildAPITask 根据 APISyncConfig 构建同步任务
func (b *BatchDataSyncWorkflowBuilder) buildAPITask(config APISyncConfig, baseParams, dateTimeParams map[string]interface{}) (*task.Task, error) {
	taskName := "Sync_" + config.APIName

	// 确定同步模式：如果有 ParamKey 则是模板任务，否则是直接任务
	syncMode := config.SyncMode
	if syncMode == "" {
		if config.ParamKey != "" {
			syncMode = "template"
		} else {
			syncMode = "direct"
		}
	}

	// 确定依赖任务
	dependencies := config.Dependencies
	if len(dependencies) == 0 {
		// 默认依赖推断
		if syncMode == "template" {
			dependencies = []string{"FetchStockBasic"}
		} else if config.UpstreamTask != "" {
			dependencies = []string{config.UpstreamTask}
		}
	}

	if syncMode == "template" {
		// 模板任务：按参数拆分生成子任务
		upstreamTask := config.UpstreamTask
		if upstreamTask == "" {
			upstreamTask = "FetchStockBasic"
		}

		// 将 dateTimeParams（start_date/end_date）平铺为顶层参数，
		// 因为 task-engine 的 Builder.Build() 会将嵌套 map 序列化为不可逆的字符串
		templateParams := mergeParams(baseParams, dateTimeParams)
		templateParams = mergeParams(templateParams, map[string]interface{}{
			"api_name":      config.APIName,
			"param_key":     config.ParamKey,
			"upstream_task": upstreamTask,
			"max_sub_tasks": b.params.MaxStocks,
		})
		if config.ExtraParams != nil {
			templateParams = mergeParams(templateParams, config.ExtraParams)
		}
		b.mergeSubtaskMemoryParams(templateParams)

		taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据（模板任务）", b.registry).
			WithJobFunction("GenerateDataSyncSubTasks", templateParams).
			WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
			WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
			WithTemplate(true)

		for _, dep := range dependencies {
			taskBuilder = taskBuilder.WithDependency(dep)
		}

		return taskBuilder.Build()
	}

	// 直接任务：直接调用 SyncAPIData
	jobParams := mergeParams(baseParams, map[string]interface{}{
		"api_name": config.APIName,
	})

	// 构造请求参数：
	// - 默认包含全局日期范围（start_date/end_date），使支持日期区间的 API 能按计划时间段执行
	// - 若配置了 ExtraParams，则在其基础上叠加日期范围，调用方可通过 ExtraParams 显式覆盖同名字段
	// - 再应用 APISyncStrategy.FixedParams/FixedParamKeys，保证策略中的固定参数生效
	params := map[string]interface{}{}
	if dateTimeParams != nil {
		params = mergeParams(params, dateTimeParams)
	}
	if config.ExtraParams != nil {
		params = mergeParams(params, config.ExtraParams)
	}

	strategy := b.getStrategy(config.APIName)
	if len(strategy.FixedParams) > 0 || len(strategy.FixedParamKeys) > 0 {
		params = mergeParamsWithStrategy(strategy, params, nil)
		log.Printf("🔧 [buildAPITask] 应用策略 FixedParams: API=%s, FixedParams=%v, FixedParamKeys=%v, mergedParams=%v",
			config.APIName, strategy.FixedParams, strategy.FixedParamKeys, params)
	}

	// 特殊处理：Tushare/forecast 要求 ann_date 和 ts_code 至少填一个参数。
	// 若调用方未显式指定 ann_date 或 ts_code，则使用 end_date 作为 ann_date，避免参数校验失败。
	if config.APIName == "forecast" {
		if _, hasAnn := params["ann_date"]; !hasAnn {
			if _, hasTsCode := params["ts_code"]; !hasTsCode {
				if end, ok := params["end_date"].(string); ok && end != "" {
					params["ann_date"] = end
				}
			}
		}
	}

	if len(params) > 0 {
		jobParams["params"] = params
	}

	// 添加上游参数映射
	if config.UpstreamParams != nil {
		jobParams["upstream_params"] = config.UpstreamParams
	}

	taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据", b.registry).
		WithJobFunction("SyncAPIData", jobParams).
		WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateSyncData")

	for _, dep := range dependencies {
		taskBuilder = taskBuilder.WithDependency(dep)
	}

	return taskBuilder.Build()
}

// mergeParams 合并参数 map
func mergeParams(base, extra map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range base {
		result[k] = v
	}
	for k, v := range extra {
		result[k] = v
	}
	return result
}

// mergeParamsWithStrategy 将策略中的 FixedParams、FixedParamKeys 与基础参数/运行时参数合并。
// 合并优先级：FixedParams > base > runtime，当 key 在 FixedParamKeys 中时，始终以 FixedParams 为准。
func mergeParamsWithStrategy(strategy APISyncStrategy, base map[string]interface{}, runtime map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})

	// 1) 先放 FixedParams
	for k, v := range strategy.FixedParams {
		out[k] = v
	}

	// 2) 再放 base 参数
	for k, v := range base {
		if contains(strategy.FixedParamKeys, k) {
			continue
		}
		out[k] = v
	}

	// 3) 最后放运行时参数
	for k, v := range runtime {
		if contains(strategy.FixedParamKeys, k) {
			continue
		}
		out[k] = v
	}

	return out
}

func contains(list []string, key string) bool {
	for _, v := range list {
		if v == key {
			return true
		}
	}
	return false
}

// ensureImplicitCommonAPIs 确保 stock_basic/trade_cal 始终在 common_data_apis 中，
// 使 SyncAPIDataJob 对这些基础参照表优先走 DuckDB 短路。
func ensureImplicitCommonAPIs(existing []string) []string {
	implicit := []string{"stock_basic", "trade_cal"}
	set := make(map[string]bool, len(existing))
	for _, v := range existing {
		set[v] = true
	}
	result := make([]string, len(existing))
	copy(result, existing)
	for _, name := range implicit {
		if !set[name] {
			result = append(result, name)
		}
	}
	return result
}

// filterMissingBaseDependencies 在未创建基础 Fetch 任务时，移除对应依赖，避免无效依赖阻塞任务编排。
func filterMissingBaseDependencies(config APISyncConfig, addedTaskNames map[string]bool) APISyncConfig {
	if len(config.Dependencies) == 0 {
		return config
	}
	deps := make([]string, 0, len(config.Dependencies))
	for _, dep := range config.Dependencies {
		if dep == "FetchTradeCal" || dep == "FetchStockBasic" {
			if !addedTaskNames[dep] {
				log.Printf("⏭️ [BuildWorkflow] 依赖移除: API=%s, dep=%s（对应 Fetch 任务未创建）", config.APIName, dep)
				continue
			}
		}
		deps = append(deps, dep)
	}
	config.Dependencies = deps
	return config
}

// ==================== ExecutionGraph 支持 ====================

// BuildFromExecutionGraph 从 ExecutionGraph 构建工作流
// 这是 SyncPlan 的核心执行方法，根据依赖解析后的执行图构建工作流
//
// 参数：
//   - graph: 依赖解析后的执行图
//   - dataSourceName: 数据源名称
//   - token: API Token
//   - targetDBPath: 目标数据库路径
//   - startDate: 开始日期
//   - endDate: 结束日期
//   - startTime: 开始时间（可选）
//   - endTime: 结束时间（可选）
//   - maxStocks: 最大股票数量（用于限制子任务）
func (b *BatchDataSyncWorkflowBuilder) BuildFromExecutionGraph(
	graph *sync.ExecutionGraph,
	dataSourceName, token, targetDBPath string,
	startDate, endDate, startTime, endTime string,
	maxStocks int,
) (*workflow.Workflow, error) {
	if graph == nil || len(graph.Levels) == 0 {
		return nil, errors.New("execution graph is empty")
	}

	// 验证必填参数
	if dataSourceName == "" {
		return nil, ErrEmptyDataSourceName
	}
	if token == "" {
		return nil, ErrEmptyToken
	}
	if targetDBPath == "" {
		return nil, ErrEmptyTargetDBPath
	}
	// startDate/endDate 可为空：仅同步无需日期参数的 API 时由调用方传空即可

	// 预加载策略缓存（如果有提供者），以便在构建任务时应用 FixedParams 等
	var apiNames []string
	for _, level := range graph.Levels {
		for _, apiName := range level {
			apiNames = append(apiNames, apiName)
		}
	}
	if len(apiNames) > 0 {
		ctx := context.Background()
		if err := b.loadStrategies(ctx, apiNames); err != nil {
			log.Printf("⚠️ [BuildFromExecutionGraph] 加载策略失败，将使用默认策略: %v", err)
		}
	}

	var tasks []*task.Task
	var depNames []string

	// 基础参数（含 common_data_apis，来自 builder，自动补全 stock_basic/trade_cal）
	// 预序列化为 JSON，避免 task-engine builder 将 []string 转为 Go fmt 格式
	graphCommonAPIs := ensureImplicitCommonAPIs(b.params.CommonDataAPIs)
	var graphCommonParam interface{} = graphCommonAPIs
	if len(graphCommonAPIs) > 0 {
		if b, err := json.Marshal(graphCommonAPIs); err == nil {
			graphCommonParam = string(b)
		}
	}
	baseParams := map[string]interface{}{
		"data_source_name": dataSourceName,
		"token":            token,
		"target_db_path":   targetDBPath,
		"common_data_apis": graphCommonParam,
	}

	// 日期时间参数
	startDateTime := startDate
	if startTime != "" {
		startDateTime = fmt.Sprintf("%s %s", startDate, startTime)
	}
	endDateTime := endDate
	if endTime != "" {
		endDateTime = fmt.Sprintf("%s %s", endDate, endTime)
	}
	dateTimeParams := map[string]interface{}{
		"start_date": startDateTime,
		"end_date":   endDateTime,
	}

	// 遍历执行图的每一层
	for _, level := range graph.Levels {
		for _, apiName := range level {
			config, exists := graph.TaskConfigs[apiName]
			if !exists {
				// 如果没有配置，使用默认直接模式
				config = &sync.TaskConfig{
					APIName:  apiName,
					SyncMode: sync.TaskSyncModeDirect,
				}
			}

			// 优先处理时间窗口模板策略
			strategy := b.getStrategy(apiName)
			taskName := "Sync_" + apiName
			if strategy.TimeWindow != nil && strategy.TimeWindow.Enabled {
				twTasks, twDepName, err := b.buildTimeWindowTasks(apiName, strategy, baseParams, startDateTime, endDateTime, maxStocks)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, twTasks...)
				depNames = append(depNames, twDepName)
				continue
			}

			if apiName == "stk_mins" && config.SyncMode == sync.TaskSyncModeTemplate {
				var paramKey, upstreamTask string
				for _, pm := range config.ParamMappings {
					if pm.IsList {
						paramKey = pm.ParamName
						upstreamTask = pm.SourceTask
						break
					}
				}
				if paramKey == "" {
					paramKey = "ts_code"
				}
				if upstreamTask == "" {
					upstreamTask = "FetchStockBasic"
				}
				stkTasks, depName, errStk := b.buildStkMinsChunkedSyncTasks(baseParams, dateTimeParams, nil, upstreamTask, paramKey, maxStocks)
				if errStk != nil {
					return nil, fmt.Errorf("build stk_mins for %s: %w", apiName, errStk)
				}
				tasks = append(tasks, stkTasks...)
				depNames = append(depNames, depName)
				continue
			}

			syncTask, err := b.buildTaskFromConfig(config, baseParams, dateTimeParams, maxStocks)
			if err != nil {
				return nil, fmt.Errorf("build task for %s: %w", apiName, err)
			}
			tasks = append(tasks, syncTask)
			depNames = append(depNames, taskName)
		}
	}

	// BatchSyncComplete：依赖所有同步任务，成功/失败均触发 DataSyncCompleteHandler → execution 回调
	completeTaskBuilder := builder.NewTaskBuilder("BatchSyncComplete", "批量同步完成（触发 execution 回调）", b.registry).
		WithJobFunction("NotifySyncComplete", map[string]interface{}{}).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncComplete").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncComplete")
	for _, dep := range depNames {
		completeTaskBuilder = completeTaskBuilder.WithDependency(dep)
	}
	completeTask, err := completeTaskBuilder.Build()
	if err != nil {
		return nil, fmt.Errorf("build BatchSyncComplete task: %w", err)
	}
	tasks = append(tasks, completeTask)

	// 构建工作流
	wfBuilder := builder.NewWorkflowBuilder("BatchDataSync", "批量数据同步工作流 - 基于 ExecutionGraph")
	for _, t := range tasks {
		wfBuilder.WithTask(t)
	}

	wf, err := wfBuilder.Build()
	if err != nil {
		return nil, err
	}

	// 启用 SAGA 事务
	wf.SetTransactional(true)

	return wf, nil
}

// buildTaskFromConfig 根据 sync.TaskConfig 构建任务
func (b *BatchDataSyncWorkflowBuilder) buildTaskFromConfig(
	config *sync.TaskConfig,
	baseParams, dateTimeParams map[string]interface{},
	maxStocks int,
) (*task.Task, error) {
	taskName := "Sync_" + config.APIName

	// 根据 SyncMode 构建不同类型的任务
	if config.SyncMode == sync.TaskSyncModeTemplate {
		return b.buildTemplateTask(taskName, config, baseParams, dateTimeParams, maxStocks)
	}

	// Direct 模式
	return b.buildDirectTask(taskName, config, baseParams, dateTimeParams)
}

// buildTemplateTask 构建模板任务（按参数拆分子任务）
func (b *BatchDataSyncWorkflowBuilder) buildTemplateTask(
	taskName string,
	config *sync.TaskConfig,
	baseParams, dateTimeParams map[string]interface{},
	maxStocks int,
) (*task.Task, error) {
	// 从 ParamMappings 中获取主参数和上游任务
	var paramKey, upstreamTask string
	for _, pm := range config.ParamMappings {
		if pm.IsList {
			paramKey = pm.ParamName
			upstreamTask = pm.SourceTask
			break
		}
	}

	// 默认值
	if paramKey == "" {
		paramKey = "ts_code"
	}
	if upstreamTask == "" {
		upstreamTask = "FetchStockBasic"
	}

	cfgTpl := mergeParams(mergeParams(baseParams, dateTimeParams), map[string]interface{}{
		"api_name":      config.APIName,
		"param_key":     paramKey,
		"upstream_task": upstreamTask,
		"max_sub_tasks": maxStocks,
	})
	b.mergeSubtaskMemoryParams(cfgTpl)

	taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据（模板任务）", b.registry).
		WithJobFunction("GenerateDataSyncSubTasks", cfgTpl).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithTemplate(true)

	// 添加依赖
	for _, dep := range config.Dependencies {
		taskBuilder = taskBuilder.WithDependency(dep)
	}

	return taskBuilder.Build()
}

// buildDirectTask 构建直接任务
func (b *BatchDataSyncWorkflowBuilder) buildDirectTask(
	taskName string,
	config *sync.TaskConfig,
	baseParams, dateTimeParams map[string]interface{},
) (*task.Task, error) {
	jobParams := mergeParams(baseParams, map[string]interface{}{
		"api_name": config.APIName,
	})

	// 构建上游参数映射
	if len(config.ParamMappings) > 0 {
		upstreamParams := make(map[string]interface{})
		for _, pm := range config.ParamMappings {
			upstreamParams[pm.ParamName] = map[string]interface{}{
				"source_task":  pm.SourceTask,
				"source_field": pm.SourceField,
				"select":       pm.Select,
			}
			if pm.FilterField != "" {
				upstreamParams[pm.ParamName].(map[string]interface{})["filter_field"] = pm.FilterField
				upstreamParams[pm.ParamName].(map[string]interface{})["filter_value"] = pm.FilterValue
			}
		}
		jobParams["upstream_params"] = upstreamParams
	}

	// 添加日期参数并应用 APISyncStrategy.FixedParams（若存在）
	params := map[string]interface{}{}
	if dateTimeParams != nil {
		params = mergeParams(params, dateTimeParams)
	}

	strategy := b.getStrategy(config.APIName)
	if len(strategy.FixedParams) > 0 || len(strategy.FixedParamKeys) > 0 {
		params = mergeParamsWithStrategy(strategy, params, nil)
		log.Printf("🔧 [buildDirectTask] 应用策略 FixedParams: API=%s, FixedParams=%v, FixedParamKeys=%v, mergedParams=%v",
			config.APIName, strategy.FixedParams, strategy.FixedParamKeys, params)
	}

	if len(params) > 0 {
		jobParams["params"] = params
	}

	taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据", b.registry).
		WithJobFunction("SyncAPIData", jobParams).
		WithTimeout(b.syncAPIDataJobTimeoutSeconds()).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateSyncData")

	// 添加依赖
	for _, dep := range config.Dependencies {
		taskBuilder = taskBuilder.WithDependency(dep)
	}

	return taskBuilder.Build()
}

// ConvertAPIConfigsFromGraph 将 ExecutionGraph 中的 TaskConfigs 转换为 APISyncConfig 列表
// 用于兼容旧的 WithAPIConfigs 方法
func ConvertAPIConfigsFromGraph(graph *sync.ExecutionGraph) []APISyncConfig {
	var configs []APISyncConfig

	for _, level := range graph.Levels {
		for _, apiName := range level {
			taskConfig, exists := graph.TaskConfigs[apiName]
			if !exists {
				configs = append(configs, APISyncConfig{
					APIName:  apiName,
					SyncMode: "direct",
				})
				continue
			}

			config := APISyncConfig{
				APIName:      taskConfig.APIName,
				Dependencies: taskConfig.Dependencies,
			}

			if taskConfig.SyncMode == sync.TaskSyncModeTemplate {
				config.SyncMode = "template"
				// 从 ParamMappings 中提取
				for _, pm := range taskConfig.ParamMappings {
					if pm.IsList {
						config.ParamKey = pm.ParamName
						config.UpstreamTask = pm.SourceTask
						break
					}
				}
			} else {
				config.SyncMode = "direct"
				if len(taskConfig.ParamMappings) > 0 {
					config.UpstreamTask = taskConfig.ParamMappings[0].SourceTask
					upstreamParams := make(map[string]interface{})
					for _, pm := range taskConfig.ParamMappings {
						upstreamParams[pm.ParamName] = map[string]interface{}{
							"source_task":  pm.SourceTask,
							"source_field": pm.SourceField,
							"select":       pm.Select,
						}
					}
					config.UpstreamParams = upstreamParams
				}
			}

			configs = append(configs, config)
		}
	}

	return configs
}
