// Package workflow contains the workflow domain services.
package workflow

import (
	"context"

	"qdhub/internal/domain/shared"
)

// ==================== 领域服务接口（纯业务逻辑）====================

// WorkflowValidator defines domain service for workflow validation.
// Implementation: workflow/service_impl.go
type WorkflowValidator interface {
	// ValidateWorkflowDefinition validates workflow definition.
	ValidateWorkflowDefinition(definition *WorkflowDefinition) error

	// ValidateWorkflowInstance validates workflow instance.
	ValidateWorkflowInstance(instance *WorkflowInstance) error

	// ValidateDefinitionYAML validates workflow definition YAML format.
	ValidateDefinitionYAML(yamlContent string) error

	// ValidateTriggerParams validates trigger parameters.
	ValidateTriggerParams(triggerType TriggerType, params map[string]interface{}) error
}

// ProgressCalculator defines domain service for progress calculation.
// Implementation: workflow/service_impl.go
type ProgressCalculator interface {
	// CalculateProgress calculates workflow progress based on task instances.
	CalculateProgress(tasks []TaskInstance) float64

	// EstimateRemainingTime estimates remaining time based on current progress.
	EstimateRemainingTime(instance *WorkflowInstance) *int64
}

// ==================== 数据传输对象 ====================

// WorkflowStatus represents the detailed status of a workflow instance.
type WorkflowStatus struct {
	InstanceID     string             `json:"instance_id"` // Task Engine uses string ID
	Status         string             `json:"status"`      // Task Engine status string
	Progress       float64            `json:"progress"`
	TaskCount      int                `json:"task_count"`
	CompletedTask  int                `json:"completed_task"`
	FailedTask     int                `json:"failed_task"`
	RunningCount   int                `json:"running_count"`   // 正在运行的任务数（来自引擎快照，0 时也返回以便与内部一致）
	PendingCount   int                `json:"pending_count"`   // 挂起的任务数（来自引擎快照，0 时也返回）
	RunningTaskIDs []string           `json:"running_task_ids,omitempty"` // 正在运行的任务 ID（存储可能滞后）
	PendingTaskIDs []string           `json:"pending_task_ids,omitempty"`  // 挂起的任务 ID（存储可能滞后）
	StartedAt      shared.Timestamp   `json:"started_at"`
	FinishedAt     *shared.Timestamp  `json:"finished_at,omitempty"`
	ErrorMessage   *string            `json:"error_message,omitempty"`
}

// ==================== 外部依赖接口（领域定义，基础设施实现）====================

// ==================== Workflow 执行请求参数类型 ====================

// MetadataCrawlRequest 元数据爬取请求参数
type MetadataCrawlRequest struct {
	DataSourceID   shared.ID // 数据源 ID（必填，用于校验和执行）
	DataSourceName string    // 数据源名称（必填，如 "tushare"）
	MaxAPICrawl    int       // 最大爬取 API 数量（可选，0表示不限制）
}

// CreateTablesRequest 建表请求参数
type CreateTablesRequest struct {
	DataSourceID   shared.ID // 数据源 ID（必填）
	DataSourceName string    // 数据源名称（必填）
	TargetDBPath   string    // 目标数据库路径（必填）
	MaxTables      int       // 最大建表数量（可选，0表示不限制）
}

// BatchDataSyncRequest 批量同步请求参数
// APIConfigs 与 APINames 二选一：有 APIConfigs 时优先使用（用于 SyncPlan 执行），否则使用 APINames + 默认策略。
type BatchDataSyncRequest struct {
	DataSourceID   shared.ID         // 数据源 ID（可选，用于查询策略）
	DataSourceName string            // 数据源名称（必填）
	Token          string            // API Token（必填）
	TargetDBPath   string            // 目标数据库路径（必填）
	StartDate      string            // 开始日期（必填，格式: "20251201"）
	EndDate        string            // 结束日期（必填，格式: "20251231"）
	StartTime      string            // 开始时间（可选，格式: "09:30:00"）
	EndTime        string            // 结束时间（可选，格式: "15:00:00"）
	APINames       []string          // 需要同步的 API 列表（与 APIConfigs 二选一）
	APIConfigs     []APISyncConfig   // API 同步配置（与 APINames 二选一，优先使用）
	MaxStocks      int               // 最大股票数量（可选，0表示不限制）
	CommonDataAPIs []string          // 公共数据 API 名列表（如 trade_cal, stock_basic），SyncAPIDataJob 走 Cache→DuckDB→API
}

// RealtimeDataSyncRequest 实时同步请求参数
type RealtimeDataSyncRequest struct {
	DataSourceName  string   // 数据源名称（必填）
	Token           string   // API Token（必填）
	TargetDBPath    string   // 目标数据库路径（必填）
	CheckpointTable string   // 检查点表名（可选，默认: "sync_checkpoint"）
	APINames        []string // 需要同步的 API 列表（必填）
	MaxStocks       int      // 最大股票数量（可选，0表示不限制）
	CronExpr        string   // Cron 表达式（可选）
}

// APISyncConfig API 同步配置（用于 SyncPlan 执行）
type APISyncConfig struct {
	APIName        string                 // API 名称
	SyncMode       string                 // 同步模式: direct | template
	ParamKey       string                 // 参数键（模板任务时用于拆分子任务）
	UpstreamTask   string                 // 上游任务名称（如 FetchTradeCal, FetchStockBasic）
	UpstreamParams map[string]interface{} // 上游参数映射（direct 模式，用于 SyncAPIData upstream_params）
	Dependencies   []string               // 依赖的任务列表
	ExtraParams    map[string]interface{} // 额外固定参数
}

// ExecutionGraphSyncRequest 基于 ExecutionGraph 的同步请求参数
type ExecutionGraphSyncRequest struct {
	ExecutionGraph interface{}           // ExecutionGraph 结构（使用 interface{} 避免循环依赖）
	DataSourceName string                // 数据源名称（必填）
	Token          string                // API Token（必填）
	TargetDBPath   string                // 目标数据库路径（必填）
	StartDate      string                // 开始日期（必填，格式: "20251201"）
	EndDate        string                // 结束日期（必填，格式: "20251231"）
	StartTime      string                // 开始时间（可选，格式: "09:30:00"）
	EndTime        string                // 结束时间（可选，格式: "15:00:00"）
	MaxStocks      int                   // 最大股票数量（可选，0表示不限制）
	SyncedAPIs     []string              // 需要同步的 API 列表
	SkippedAPIs    []string              // 跳过的 API 列表
}

// WorkflowExecutor defines the interface for executing built-in workflows.
// This is a domain service interface that abstracts workflow execution.
// Implementation: infrastructure/taskengine/
//
// Purpose: This interface allows domain services and application services
// to execute workflows without directly depending on WorkflowApplicationService,
// following the Dependency Inversion Principle.
//
// Note: Business validation (e.g., data source existence) should be done in
// the application service layer before calling these methods. WorkflowExecutor
// only handles parameter conversion and workflow execution.
type WorkflowExecutor interface {
	// ExecuteBuiltInWorkflow executes a built-in workflow by its API name.
	// Returns the workflow instance ID.
	// Deprecated: Use specific methods (ExecuteMetadataCrawl, etc.) for type safety.
	ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error)

	// ExecuteMetadataCrawl executes the metadata_crawl built-in workflow.
	// Crawls API documentation from a data source and saves metadata.
	ExecuteMetadataCrawl(ctx context.Context, req MetadataCrawlRequest) (shared.ID, error)

	// ExecuteCreateTables executes the create_tables built-in workflow.
	// Creates database tables based on API metadata.
	ExecuteCreateTables(ctx context.Context, req CreateTablesRequest) (shared.ID, error)

	// ExecuteBatchDataSync executes the batch_data_sync built-in workflow.
	// Syncs historical data for a specified date range.
	ExecuteBatchDataSync(ctx context.Context, req BatchDataSyncRequest) (shared.ID, error)

	// ExecuteRealtimeDataSync executes the realtime_data_sync built-in workflow.
	// Performs incremental data sync with checkpoint support.
	ExecuteRealtimeDataSync(ctx context.Context, req RealtimeDataSyncRequest) (shared.ID, error)

	// ExecuteFromExecutionGraph executes a data sync workflow based on ExecutionGraph.
	// This is the primary method for SyncPlan execution, supporting field-level dependencies.
	ExecuteFromExecutionGraph(ctx context.Context, req ExecutionGraphSyncRequest) (shared.ID, error)
}

// TaskEngineAdapter defines the interface for Task Engine integration.
// Implementation: infrastructure/taskengine/
type TaskEngineAdapter interface {
	// SubmitWorkflow submits a workflow to Task Engine.
	SubmitWorkflow(ctx context.Context, definition *WorkflowDefinition, params map[string]interface{}) (string, error)

	// SubmitDynamicWorkflow submits a dynamically built workflow to Task Engine.
	// Unlike SubmitWorkflow, this method accepts a raw workflow object without
	// requiring a WorkflowDefinition. Use this for workflows that are built
	// at execution time (e.g., BatchDataSync with variable API lists).
	SubmitDynamicWorkflow(ctx context.Context, wf *Workflow) (string, error)

	// PauseInstance pauses a workflow instance.
	PauseInstance(ctx context.Context, engineInstanceID string) error

	// ResumeInstance resumes a workflow instance.
	ResumeInstance(ctx context.Context, engineInstanceID string) error

	// CancelInstance cancels a workflow instance.
	CancelInstance(ctx context.Context, engineInstanceID string) error

	// GetInstanceStatus retrieves instance status from Task Engine.
	GetInstanceStatus(ctx context.Context, engineInstanceID string) (*WorkflowStatus, error)

	// RegisterWorkflow registers a workflow definition with Task Engine.
	RegisterWorkflow(ctx context.Context, definition *WorkflowDefinition) error

	// UnregisterWorkflow unregisters a workflow definition.
	UnregisterWorkflow(ctx context.Context, definitionID string) error

	// GetTaskInstances retrieves all task instances for a workflow instance.
	GetTaskInstances(ctx context.Context, engineInstanceID string) ([]*TaskInstance, error)

	// RetryTask retries a failed task instance.
	RetryTask(ctx context.Context, taskInstanceID string) error

	// GetFunctionRegistry returns the Task Engine function registry.
	// This is needed for dynamically building workflows at execution time.
	GetFunctionRegistry() interface{}
}
