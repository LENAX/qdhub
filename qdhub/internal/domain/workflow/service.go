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
	InstanceID    string // Task Engine uses string ID
	Status        string // Task Engine status string
	Progress      float64
	TaskCount     int
	CompletedTask int
	FailedTask    int
	StartedAt     shared.Timestamp
	FinishedAt    *shared.Timestamp
	ErrorMessage  *string
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
type BatchDataSyncRequest struct {
	DataSourceName string   // 数据源名称（必填）
	Token          string   // API Token（必填）
	TargetDBPath   string   // 目标数据库路径（必填）
	StartDate      string   // 开始日期（必填，格式: "20251201"）
	EndDate        string   // 结束日期（必填，格式: "20251231"）
	StartTime      string   // 开始时间（可选，格式: "09:30:00"）
	EndTime        string   // 结束时间（可选，格式: "15:00:00"）
	APINames       []string // 需要同步的 API 列表（必填）
	MaxStocks      int      // 最大股票数量（可选，0表示不限制）
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
}

// TaskEngineAdapter defines the interface for Task Engine integration.
// Implementation: infrastructure/taskengine/
type TaskEngineAdapter interface {
	// SubmitWorkflow submits a workflow to Task Engine.
	SubmitWorkflow(ctx context.Context, definition *WorkflowDefinition, params map[string]interface{}) (string, error)

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
}
