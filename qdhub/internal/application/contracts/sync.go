// Package contracts defines application service interfaces and DTOs.
package contracts

import (
	"context"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// SyncApplicationService defines application service for sync management.
// Responsibilities:
//   - Orchestrate complete sync use cases
//   - Manage transactions
//   - Coordinate with WorkflowApplicationService
//   - Handle Task Engine scheduler integration
type SyncApplicationService interface {
	// ==================== Sync Job Management ====================

	// CreateSyncJob creates a new sync job.
	CreateSyncJob(ctx context.Context, req CreateSyncJobRequest) (*sync.SyncJob, error)

	// GetSyncJob retrieves a sync job by ID.
	GetSyncJob(ctx context.Context, id shared.ID) (*sync.SyncJob, error)

	// UpdateSyncJob updates a sync job.
	UpdateSyncJob(ctx context.Context, id shared.ID, req UpdateSyncJobRequest) error

	// DeleteSyncJob deletes a sync job.
	DeleteSyncJob(ctx context.Context, id shared.ID) error

	// ListSyncJobs lists all sync jobs.
	ListSyncJobs(ctx context.Context) ([]*sync.SyncJob, error)

	// ==================== Job Execution ====================

	// ExecuteSyncJob executes a sync job manually.
	// This is a complex use case involving:
	//   1. Validate job status
	//   2. Create workflow instance
	//   3. Submit to Task Engine
	//   4. Create sync execution record
	//   5. Return execution ID
	ExecuteSyncJob(ctx context.Context, jobID shared.ID) (shared.ID, error)

	// GetSyncExecution retrieves a sync execution by ID.
	GetSyncExecution(ctx context.Context, id shared.ID) (*sync.SyncExecution, error)

	// ListSyncExecutions lists all executions for a sync job.
	ListSyncExecutions(ctx context.Context, jobID shared.ID) ([]*sync.SyncExecution, error)

	// CancelExecution cancels a running sync execution.
	CancelExecution(ctx context.Context, executionID shared.ID) error

	// ==================== Scheduling ====================

	// EnableJob enables a sync job and schedules it if it has a cron expression.
	EnableJob(ctx context.Context, jobID shared.ID) error

	// DisableJob disables a sync job and unschedules it.
	DisableJob(ctx context.Context, jobID shared.ID) error

	// UpdateSchedule updates the cron schedule for a sync job.
	UpdateSchedule(ctx context.Context, jobID shared.ID, cronExpression string) error

	// ==================== Callback Handlers ====================

	// HandleExecutionCallback handles execution result callback from workflow.
	// This is called by workflow engine when sync execution completes.
	HandleExecutionCallback(ctx context.Context, req ExecutionCallbackRequest) error

	// ==================== Built-in Workflow Execution ====================

	// SyncDataSource executes the batch_data_sync built-in workflow.
	// This is a convenience method that:
	//   1. Validates data source exists
	//   2. Retrieves token for the data source
	//   3. Executes the batch data sync workflow
	// Returns the workflow instance ID for tracking.
	SyncDataSource(ctx context.Context, req SyncDataSourceRequest) (shared.ID, error)

	// SyncDataSourceRealtime executes the realtime_data_sync built-in workflow.
	// This is a convenience method that:
	//   1. Validates data source exists
	//   2. Retrieves token for the data source
	//   3. Executes the realtime data sync workflow with checkpoint support
	// Returns the workflow instance ID for tracking.
	SyncDataSourceRealtime(ctx context.Context, req SyncDataSourceRealtimeRequest) (shared.ID, error)
}

// ==================== Request/Response DTOs ====================

// CreateSyncJobRequest represents a request to create a sync job.
type CreateSyncJobRequest struct {
	Name           string
	Description    string
	APIMetadataID  shared.ID
	DataStoreID    shared.ID
	WorkflowDefID  shared.ID
	Mode           sync.SyncMode
	CronExpression *string
	Params         map[string]interface{}
	ParamRules     []sync.ParamRule
}

// UpdateSyncJobRequest represents a request to update a sync job.
type UpdateSyncJobRequest struct {
	Name           *string
	Description    *string
	Mode           *sync.SyncMode
	CronExpression *string
	Params         *map[string]interface{}
	ParamRules     *[]sync.ParamRule
}

// ExecutionCallbackRequest represents a callback request from workflow engine.
type ExecutionCallbackRequest struct {
	ExecutionID  shared.ID
	Success      bool
	RecordCount  int64
	ErrorMessage *string
}

// SyncDataSourceRequest represents a request to sync data source using batch workflow.
type SyncDataSourceRequest struct {
	DataSourceID shared.ID // 数据源 ID（必填，用于校验和获取 token）
	TargetDBPath string    // 目标数据库路径（必填）
	StartDate    string    // 开始日期（必填，格式: "20251201"）
	EndDate      string    // 结束日期（必填，格式: "20251231"）
	StartTime    string    // 开始时间（可选，格式: "09:30:00"）
	EndTime      string    // 结束时间（可选，格式: "15:00:00"）
	APINames     []string  // 需要同步的 API 列表（必填）
	MaxStocks    int       // 最大股票数量（可选，0表示不限制）
}

// SyncDataSourceRealtimeRequest represents a request to sync data source using realtime workflow.
type SyncDataSourceRealtimeRequest struct {
	DataSourceID    shared.ID // 数据源 ID（必填，用于校验和获取 token）
	TargetDBPath    string    // 目标数据库路径（必填）
	CheckpointTable string    // 检查点表名（可选，默认: "sync_checkpoint"）
	APINames        []string  // 需要同步的 API 列表（必填）
	MaxStocks       int       // 最大股票数量（可选，0表示不限制）
	CronExpr        string    // Cron 表达式（可选）
}
