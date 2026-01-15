// Package workflows provides built-in workflow definitions for QDHub.
package workflows

import (
	"errors"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// 增量实时同步工作流错误
var (
	ErrEmptyCheckpointTable = errors.New("checkpoint_table is required")
)

// RealtimeDataSyncParams 增量实时同步工作流参数
type RealtimeDataSyncParams struct {
	DataSourceName  string   // 数据源名称（必填）
	Token           string   // API Token（必填）
	TargetDBPath    string   // 目标数据库路径（必填）
	CheckpointTable string   // 检查点表名（必填，用于记录同步位置）
	APINames        []string // 需要同步的 API 列表（必填，不能为空）
	MaxStocks       int      // 最大股票数量（用于限制子任务，0=不限制）
	CronExpr        string   // Cron 表达式（可选，用于定时调度）
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
	if p.CheckpointTable == "" {
		return ErrEmptyCheckpointTable
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
		params: RealtimeDataSyncParams{
			CheckpointTable: "sync_checkpoint", // 默认检查点表名
		},
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

// WithCheckpointTable 设置检查点表名
func (b *RealtimeDataSyncWorkflowBuilder) WithCheckpointTable(tableName string) *RealtimeDataSyncWorkflowBuilder {
	b.params.CheckpointTable = tableName
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
// 工作流结构：
// Level 0（串行执行）：
//   - GetSyncCheckpoint - 获取上次同步检查点
//
// Level 1（依赖 Level 0）：
//   - FetchLatestTradingDate - 获取最新交易日
//
// Level 2（依赖 Level 1，并行执行模板任务）：
//   - IncrementalSync_{api_name} [模板任务] - 根据 APINames 动态生成增量同步子任务
//
// Level 3（依赖 Level 2）：
//   - UpdateSyncCheckpoint - 更新同步检查点
//
// 事务支持：启用 SAGA 事务，同步失败时按 sync_batch_id 回滚数据
//
// 返回错误：
//   - ErrEmptyAPINames: api_names 不能为空
//   - ErrEmptyDataSourceName: data_source_name 必填
//   - ErrEmptyToken: token 必填
//   - ErrEmptyTargetDBPath: target_db_path 必填
//   - ErrEmptyCheckpointTable: checkpoint_table 必填
func (b *RealtimeDataSyncWorkflowBuilder) Build() (*workflow.Workflow, error) {
	params := b.params

	// 参数验证
	if err := params.Validate(); err != nil {
		return nil, err
	}

	var tasks []*task.Task

	// 基础参数
	baseParams := map[string]interface{}{
		"data_source_name": params.DataSourceName,
		"token":            params.Token,
		"target_db_path":   params.TargetDBPath,
		"checkpoint_table": params.CheckpointTable,
	}

	// ==================== Level 0: 获取同步检查点 ====================

	// Task: 获取同步检查点
	getSyncCheckpointTask, err := builder.NewTaskBuilder("GetSyncCheckpoint", "获取上次同步检查点", b.registry).
		WithJobFunction("GetSyncCheckpoint", mergeParams(baseParams, map[string]interface{}{
			"api_names": params.APINames,
		})).
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, getSyncCheckpointTask)

	// ==================== Level 1: 获取最新交易日 ====================

	// Task: 获取最新交易日
	fetchLatestTradingDateTask, err := builder.NewTaskBuilder("FetchLatestTradingDate", "获取最新交易日", b.registry).
		WithJobFunction("FetchLatestTradingDate", mergeParams(baseParams, map[string]interface{}{
			"exchange": "SSE", // 默认上交所
		})).
		WithDependency("GetSyncCheckpoint").
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, fetchLatestTradingDateTask)

	// ==================== Level 2: 增量同步模板任务 ====================

	// 为每个 API 创建增量同步模板任务
	for _, apiName := range params.APINames {
		taskName := "IncrementalSync_" + apiName
		templateTask, err := builder.NewTaskBuilder(taskName, "增量同步"+apiName+"数据（模板任务）", b.registry).
			WithJobFunction("GenerateIncrementalSyncSubTasks", mergeParams(baseParams, map[string]interface{}{
				"api_name":      apiName,
				"param_key":     "ts_code",
				"max_sub_tasks": params.MaxStocks,
			})).
			WithDependency("FetchLatestTradingDate"). // 依赖最新交易日
			WithDependency("GetSyncCheckpoint").      // 依赖检查点（获取上次同步位置）
			WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
			WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
			WithTemplate(true). // 标记为模板任务
			Build()
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, templateTask)
	}

	// ==================== Level 3: 更新同步检查点 ====================

	// 构建依赖列表（所有增量同步任务）
	syncTaskDeps := make([]string, 0, len(params.APINames))
	for _, apiName := range params.APINames {
		syncTaskDeps = append(syncTaskDeps, "IncrementalSync_"+apiName)
	}

	// Task: 更新同步检查点
	updateCheckpointTask, err := builder.NewTaskBuilder("UpdateSyncCheckpoint", "更新同步检查点", b.registry).
		WithJobFunction("UpdateSyncCheckpoint", mergeParams(baseParams, map[string]interface{}{
			"api_names": params.APINames,
		})).
		WithDependencies(syncTaskDeps). // 依赖所有增量同步任务
		WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
		WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
		WithCompensationFunction("CompensateUpdateCheckpoint"). // 补偿：回滚检查点
		Build()
	if err != nil {
		return nil, err
	}
	tasks = append(tasks, updateCheckpointTask)

	// 构建工作流
	wfBuilder := builder.NewWorkflowBuilder("RealtimeDataSync", "增量实时同步工作流 - 支持断点续传和定时调度")
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
