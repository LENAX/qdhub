// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import (
	"errors"
	"fmt"

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

// BatchDataSyncParams 批量数据同步工作流参数
type BatchDataSyncParams struct {
	DataSourceName string   // 数据源名称（必填）
	Token          string   // API Token（必填）
	TargetDBPath   string   // 目标数据库路径（必填）
	StartDate      string   // 开始日期（必填，格式: "20251201"）
	EndDate        string   // 结束日期（必填，格式: "20251231"）
	StartTime      string   // 开始时间（可选，格式: "09:30:00"）
	EndTime        string   // 结束时间（可选，格式: "15:00:00"）
	APINames       []string // 需要同步的 API 列表（必填，不能为空）
	MaxStocks      int      // 最大股票数量（用于限制子任务，0=不限制）
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
	if len(p.APINames) == 0 {
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

// Build 构建批量数据同步工作流
//
// 工作流结构：
// Level 0（并行执行）：
//   - FetchTradeCal - 获取交易日历
//   - FetchStockBasic - 获取股票基础信息
//
// Level 1（依赖 Level 0，并行执行模板任务）：
//   - Sync_{api_name} [模板任务] - 根据 APINames 动态生成
//   - SyncTopList - 同步龙虎榜（非模板，直接执行）
//
// 事务支持：启用 SAGA 事务，同步失败时按 sync_batch_id 回滚数据
//
// 参数占位符支持：如果参数为空，将使用占位符（如 ${data_source_name}），
// 执行时通过 workflow.ReplaceParams() 替换为实际值
//
// 返回错误：
//   - ErrEmptyAPINames: api_names 不能为空（仅在非占位符模式下验证）
//   - ErrEmptyDataSourceName: data_source_name 必填（仅在非占位符模式下验证）
//   - ErrEmptyToken: token 必填（仅在非占位符模式下验证）
//   - ErrEmptyTargetDBPath: target_db_path 必填（仅在非占位符模式下验证）
//   - ErrEmptyStartDate: start_date 必填（仅在非占位符模式下验证）
//   - ErrEmptyEndDate: end_date 必填（仅在非占位符模式下验证）
func (b *BatchDataSyncWorkflowBuilder) Build() (*workflow.Workflow, error) {
	params := b.params

	// 检查是否使用占位符模式（所有必填参数都为空）
	usePlaceholders := params.DataSourceName == "" && params.Token == "" && 
		params.TargetDBPath == "" && params.StartDate == "" && params.EndDate == "" &&
		len(params.APINames) == 0

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

	// ==================== Level 1: 数据同步模板任务 ====================

	// 处理 API 名称列表
	apiNames := params.APINames
	if len(apiNames) == 0 {
		// 如果为空，使用占位符（注意：这需要特殊处理，因为占位符是字符串，不是数组）
		// 这里我们创建一个特殊的占位符任务，执行时会通过参数替换处理
		apiNames = []string{"${api_names}"} // 注意：这需要执行时解析为数组
	}

	// 为每个 API 创建模板任务
	for _, apiName := range apiNames {
		taskName := "Sync_" + apiName
		templateTask, err := builder.NewTaskBuilder(taskName, "同步"+apiName+"数据（模板任务）", b.registry).
			WithJobFunction("GenerateDataSyncSubTasks", mergeParams(baseParams, map[string]interface{}{
				"api_name":      apiName,
				"param_key":     "ts_code",
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

	// Task: 同步龙虎榜（非模板任务，直接执行）
	syncTopListTask, err := builder.NewTaskBuilder("SyncTopList", "同步龙虎榜数据", b.registry).
		WithJobFunction("SyncAPIData", mergeParams(baseParams, map[string]interface{}{
			"api_name": "top_list",
			"params": map[string]interface{}{
				"trade_date": startDateOnly,
			},
		})).
		WithDependency("FetchTradeCal"). // 依赖交易日历
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateSyncData").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, syncTopListTask)

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
