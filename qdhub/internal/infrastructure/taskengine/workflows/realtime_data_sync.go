// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import (
	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// RealtimeDataSyncParams 增量实时同步工作流参数
//
// 设计说明：数据源的实时 API 通常始终提供最新数据，因此本工作流不对日期范围做校验或要求。
// 时间范围（start_date/end_date 或表+日期列）仅用于「补充历史实时数据」的可选场景；
// 不配置时视为仅同步最新数据（下游按“最新一日”处理）。
type RealtimeDataSyncParams struct {
	DataSourceName string   // 数据源名称（必填）
	Token          string   // API Token（必填）
	TargetDBPath   string   // 目标数据库路径（必填）
	APINames       []string // 需要同步的 API 列表（必填，不能为空）
	MaxStocks      int      // 最大股票数量（用于限制子任务，0=不限制）
	CronExpr       string   // Cron 表达式（可选，用于定时调度）

	// 时间范围（可选，仅用于补充历史）：不配置时不同步历史，仅拉最新
	StartDate                  string // 可选，起始日（20060102）
	EndDate                    string // 可选，结束日；未传时由 FetchLatestTradingDate 提供
	IncrementalStartDateTable  string // 可选，用于 MAX(列) 的表名
	IncrementalStartDateColumn string // 可选，日期列名（如 trade_date）
}

// Validate 验证参数
func (p *RealtimeDataSyncParams) Validate() error {
	if p.DataSourceName == "" {
		return ErrEmptyDataSourceName
	}
	if p.Token == "" {
		return ErrEmptyToken
	}
	if p.TargetDBPath == "" {
		return ErrEmptyTargetDBPath
	}
	if len(p.APINames) == 0 {
		return ErrEmptyAPINames
	}
	return nil
}

// RealtimeDataSyncWorkflowBuilder 增量实时同步工作流构建器
type RealtimeDataSyncWorkflowBuilder struct {
	registry task.FunctionRegistry
	params   RealtimeDataSyncParams
}

// NewRealtimeDataSyncWorkflowBuilder 创建增量实时同步工作流构建器
func NewRealtimeDataSyncWorkflowBuilder(registry task.FunctionRegistry) *RealtimeDataSyncWorkflowBuilder {
	return &RealtimeDataSyncWorkflowBuilder{
		registry: registry,
		params:   RealtimeDataSyncParams{},
	}
}

// WithParams 设置工作流参数
func (b *RealtimeDataSyncWorkflowBuilder) WithParams(params RealtimeDataSyncParams) *RealtimeDataSyncWorkflowBuilder {
	b.params = params
	return b
}

// WithDataSource 设置数据源和 Token
func (b *RealtimeDataSyncWorkflowBuilder) WithDataSource(name, token string) *RealtimeDataSyncWorkflowBuilder {
	b.params.DataSourceName = name
	b.params.Token = token
	return b
}

// WithTargetDB 设置目标数据库路径
func (b *RealtimeDataSyncWorkflowBuilder) WithTargetDB(path string) *RealtimeDataSyncWorkflowBuilder {
	b.params.TargetDBPath = path
	return b
}

// WithSyncRange 设置同步范围：起始/结束日或表+日期列（与 SyncPlan 增量一致）
func (b *RealtimeDataSyncWorkflowBuilder) WithSyncRange(startDate, endDate, table, column string) *RealtimeDataSyncWorkflowBuilder {
	b.params.StartDate = startDate
	b.params.EndDate = endDate
	b.params.IncrementalStartDateTable = table
	b.params.IncrementalStartDateColumn = column
	return b
}

// WithAPIs 设置需要同步的 API 列表（必填）
func (b *RealtimeDataSyncWorkflowBuilder) WithAPIs(apis ...string) *RealtimeDataSyncWorkflowBuilder {
	b.params.APINames = apis
	return b
}

// WithMaxStocks 设置最大股票数量（限制子任务数量）
func (b *RealtimeDataSyncWorkflowBuilder) WithMaxStocks(max int) *RealtimeDataSyncWorkflowBuilder {
	b.params.MaxStocks = max
	return b
}

// WithCronExpr 设置 Cron 定时表达式
// 格式: "秒 分 时 日 月 周" (6位)
// 示例: "0 0 18 * * 1-5" 每个工作日18:00执行
func (b *RealtimeDataSyncWorkflowBuilder) WithCronExpr(cronExpr string) *RealtimeDataSyncWorkflowBuilder {
	b.params.CronExpr = cronExpr
	return b
}

// Build 构建增量实时同步工作流
//
// 工作流结构（不依赖 checkpoint 表，与 SyncPlan 增量逻辑一致）：
// Level 0：
//   - GetSyncRangeFromTarget - 从目标库表+日期列计算 start_date（或使用传入的 start_date）
//
// Level 1（依赖 Level 0）：
//   - FetchLatestTradingDate - 获取最新交易日作为 end_date
//
// Level 2（依赖 Level 1，并行执行模板任务）：
//   - IncrementalSync_{api_name} [模板任务] - 根据 APINames 动态生成增量同步子任务
//
// 事务支持：启用 SAGA 事务，同步失败时按 sync_batch_id 回滚数据
//
// 参数占位符支持：如果参数为空，将使用占位符（如 ${data_source_name}），
// 执行时通过 workflow.ReplaceParams() 替换为实际值
func (b *RealtimeDataSyncWorkflowBuilder) Build() (*workflow.Workflow, error) {
	params := b.params

	// 检查是否使用占位符模式（所有必填参数都为空）
	usePlaceholders := params.DataSourceName == "" && params.Token == "" &&
		params.TargetDBPath == "" && len(params.APINames) == 0

	// 仅在非占位符模式下验证参数
	if !usePlaceholders {
		if err := params.Validate(); err != nil {
			return nil, err
		}
	}

	var tasks []*task.Task

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

	baseParams := map[string]interface{}{
		"data_source_name": dataSourceName,
		"token":            token,
		"target_db_path":   targetDBPath,
	}

	// 同步范围参数（表+列或 start/end）；空时使用占位符，执行时 ReplaceParams 替换
	rangeParams := map[string]interface{}{
		"start_date":                    params.StartDate,
		"end_date":                      params.EndDate,
		"incremental_start_date_table":  params.IncrementalStartDateTable,
		"incremental_start_date_column": params.IncrementalStartDateColumn,
	}
	if rangeParams["start_date"] == "" {
		rangeParams["start_date"] = "${start_date}"
	}
	if rangeParams["end_date"] == "" {
		rangeParams["end_date"] = "${end_date}"
	}
	if rangeParams["incremental_start_date_table"] == "" {
		rangeParams["incremental_start_date_table"] = "${incremental_start_date_table}"
	}
	if rangeParams["incremental_start_date_column"] == "" {
		rangeParams["incremental_start_date_column"] = "${incremental_start_date_column}"
	}

	// ==================== Level 0: 从目标库计算同步范围（不依赖 checkpoint） ====================

	getSyncRangeTask, err := builder.NewTaskBuilder("GetSyncRangeFromTarget", "从目标库计算同步起始日", b.registry).
		WithJobFunction("GetSyncRangeFromTarget", mergeParams(baseParams, rangeParams)).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, getSyncRangeTask)

	// ==================== Level 1: 获取最新交易日 ====================

	fetchLatestTradingDateTask, err := builder.NewTaskBuilder("FetchLatestTradingDate", "获取最新交易日", b.registry).
		WithJobFunction("FetchLatestTradingDate", mergeParams(baseParams, map[string]interface{}{
			"exchange": "SSE",
		})).
		WithDependency("GetSyncRangeFromTarget").
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, fetchLatestTradingDateTask)

	// ==================== Level 2: 增量同步模板任务 ====================

	apiNames := params.APINames
	if len(apiNames) == 0 {
		apiNames = []string{"${api_names}"}
	}

	for _, apiName := range apiNames {
		taskName := "IncrementalSync_" + apiName
		templateTask, err := builder.NewTaskBuilder(taskName, "增量同步"+apiName+"数据（模板任务）", b.registry).
			WithJobFunction("GenerateIncrementalSyncSubTasks", mergeParams(baseParams, map[string]interface{}{
				"api_name":      apiName,
				"param_key":     "ts_code",
				"max_sub_tasks": params.MaxStocks,
			})).
			WithDependency("FetchLatestTradingDate").
			WithDependency("GetSyncRangeFromTarget").
			WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
			WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
			WithTemplate(true).
			Build()
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, templateTask)
	}

	// 构建工作流
	wfBuilder := builder.NewWorkflowBuilder("RealtimeDataSync", "增量实时同步工作流 - 同步范围由表+日期列或起止日，支持定时调度")
	for _, t := range tasks {
		wfBuilder.WithTask(t)
	}

	// 设置 Cron 表达式（如果有）
	if params.CronExpr != "" {
		wfBuilder.WithCronExpr(params.CronExpr)
	}

	wf, err := wfBuilder.Build()
	if err != nil {
		return nil, err
	}

	// 启用 SAGA 事务
	wf.SetTransactional(true)

	return wf, nil
}
