// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import (
	"errors"
	"fmt"

	"qdhub/internal/domain/sync"

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
	if p.StartDate == "" {
		return ErrEmptyStartDate
	}
	if p.EndDate == "" {
		return ErrEmptyEndDate
	}
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
	registry task.FunctionRegistry
	params   BatchDataSyncParams
}

// NewBatchDataSyncWorkflowBuilder 创建批量数据同步工作流构建器
func NewBatchDataSyncWorkflowBuilder(registry task.FunctionRegistry) *BatchDataSyncWorkflowBuilder {
	return &BatchDataSyncWorkflowBuilder{
		registry: registry,
	}
}

// WithParams 设置工作流参数
func (b *BatchDataSyncWorkflowBuilder) WithParams(params BatchDataSyncParams) *BatchDataSyncWorkflowBuilder {
	b.params = params
	return b
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
	if startDate == "" {
		startDate = "${start_date}"
	}
	endDate := params.GetEndDateTime()
	if endDate == "" {
		endDate = "${end_date}"
	}

	// 基础参数
	baseParams := map[string]interface{}{
		"data_source_name": dataSourceName,
		"token":            token,
		"target_db_path":   targetDBPath,
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

	// Task: 获取交易日历
	fetchTradeCalTask, err := builder.NewTaskBuilder("FetchTradeCal", "获取交易日历", b.registry).
		WithJobFunction("SyncAPIData", mergeParams(baseParams, map[string]interface{}{
			"api_name": "trade_cal",
			"params": map[string]interface{}{
				"exchange":   "SSE",
				"start_date": startDateOnly, // 交易日历只用日期
				"end_date":   endDateOnly,
			},
		})).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, fetchTradeCalTask)

	// Task: 获取股票基础信息
	fetchStockBasicTask, err := builder.NewTaskBuilder("FetchStockBasic", "获取股票基础信息", b.registry).
		WithJobFunction("SyncAPIData", mergeParams(baseParams, map[string]interface{}{
			"api_name": "stock_basic",
			"params": map[string]interface{}{
				"list_status": "L",
			},
		})).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, fetchStockBasicTask)

	// ==================== Level 1: 数据同步任务 ====================

	// 优先使用 APIConfigs（高级配置模式）
	if len(params.APIConfigs) > 0 {
		for _, config := range params.APIConfigs {
			syncTask, err := b.buildAPITask(config, baseParams, dateTimeParams)
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, syncTask)
		}
	} else {
		// 兼容旧用法：使用 APINames（简单模式，默认都是模板任务）
		apiNames := params.APINames
		if len(apiNames) == 0 {
			// 如果为空，使用占位符
			apiNames = []string{"${api_names}"}
		}

		for _, apiName := range apiNames {
			taskName := "Sync_" + apiName
			templateTask, err := builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（模板任务）", b.registry).
				WithJobFunction("GenerateDataSyncSubTasks", mergeParams(baseParams, map[string]interface{}{
					"api_name":      apiName,
					"param_key":     "ts_code",
					"upstream_task": "FetchStockBasic",
					"max_sub_tasks": params.MaxStocks,
					"extra_params":  dateTimeParams,
				})).
				WithDependency("FetchStockBasic"). // 依赖股票基础信息以获取 ts_codes
				WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
				WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
				WithTemplate(true). // 标记为模板任务
				Build()
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, templateTask)
		}
	}

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

		extraParams := dateTimeParams
		if config.ExtraParams != nil {
			extraParams = mergeParams(dateTimeParams, config.ExtraParams)
		}

		taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据（模板任务）", b.registry).
			WithJobFunction("GenerateDataSyncSubTasks", mergeParams(baseParams, map[string]interface{}{
				"api_name":      config.APIName,
				"param_key":     config.ParamKey,
				"upstream_task": upstreamTask,
				"max_sub_tasks": b.params.MaxStocks,
				"extra_params":  extraParams,
			})).
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

	// 添加上游参数映射
	if config.UpstreamParams != nil {
		jobParams["upstream_params"] = config.UpstreamParams
	}

	// 添加额外参数
	if config.ExtraParams != nil {
		jobParams["params"] = config.ExtraParams
	}

	taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据", b.registry).
		WithJobFunction("SyncAPIData", jobParams).
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
	if startDate == "" {
		return nil, ErrEmptyStartDate
	}
	if endDate == "" {
		return nil, ErrEmptyEndDate
	}

	var tasks []*task.Task

	// 基础参数
	baseParams := map[string]interface{}{
		"data_source_name": dataSourceName,
		"token":            token,
		"target_db_path":   targetDBPath,
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

			syncTask, err := b.buildTaskFromConfig(config, baseParams, dateTimeParams, maxStocks)
			if err != nil {
				return nil, fmt.Errorf("build task for %s: %w", apiName, err)
			}
			tasks = append(tasks, syncTask)
		}
	}

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

	taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据（模板任务）", b.registry).
		WithJobFunction("GenerateDataSyncSubTasks", mergeParams(baseParams, map[string]interface{}{
			"api_name":      config.APIName,
			"param_key":     paramKey,
			"upstream_task": upstreamTask,
			"max_sub_tasks": maxStocks,
			"extra_params":  dateTimeParams,
		})).
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

	// 添加日期参数
	jobParams["params"] = dateTimeParams

	taskBuilder := builder.NewTaskBuilder(taskName, "同步"+config.APIName+"数据", b.registry).
		WithJobFunction("SyncAPIData", jobParams).
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
