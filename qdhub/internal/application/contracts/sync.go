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
	// ==================== Sync Plan Management ====================

	// CreateSyncPlan creates a new sync plan.
	CreateSyncPlan(ctx context.Context, req CreateSyncPlanRequest) (*sync.SyncPlan, error)

	// GetSyncPlan retrieves a sync plan by ID.
	GetSyncPlan(ctx context.Context, id shared.ID) (*sync.SyncPlan, error)

	// UpdateSyncPlan updates a sync plan.
	UpdateSyncPlan(ctx context.Context, id shared.ID, req UpdateSyncPlanRequest) error

	// DeleteSyncPlan deletes a sync plan.
	DeleteSyncPlan(ctx context.Context, id shared.ID) error

	// ListSyncPlans lists all sync plans.
	ListSyncPlans(ctx context.Context) ([]*sync.SyncPlan, error)

	// ResolveSyncPlan resolves dependencies for a sync plan.
	ResolveSyncPlan(ctx context.Context, planID shared.ID) error

	// ==================== Plan Execution ====================

	// ExecuteSyncPlan executes a sync plan.
	// This is the core method that:
	//   1. Validates plan status
	//   2. Filters tasks by sync frequency
	//   3. Converts ExecutionGraph to API configs
	//   4. Submits to Task Engine
	//   5. Creates sync execution record
	//   6. Returns execution ID
	ExecuteSyncPlan(ctx context.Context, planID shared.ID, req ExecuteSyncPlanRequest) (shared.ID, error)

	// GetSyncExecution retrieves a sync execution by ID.
	GetSyncExecution(ctx context.Context, id shared.ID) (*sync.SyncExecution, error)

	// ListPlanExecutions lists all executions for a sync plan.
	ListPlanExecutions(ctx context.Context, planID shared.ID) ([]*sync.SyncExecution, error)

	// CancelExecution cancels a running sync execution.
	CancelExecution(ctx context.Context, executionID shared.ID) error

	// ==================== Scheduling ====================

	// EnablePlan enables a sync plan and schedules it if it has a cron expression.
	EnablePlan(ctx context.Context, planID shared.ID) error

	// DisablePlan disables a sync plan and unschedules it.
	DisablePlan(ctx context.Context, planID shared.ID) error

	// UpdatePlanSchedule updates the cron schedule for a sync plan.
	UpdatePlanSchedule(ctx context.Context, planID shared.ID, cronExpression string) error

	// ==================== Callback Handlers ====================

	// HandleExecutionCallback handles execution result callback from workflow.
	// This is called by workflow engine when sync execution completes.
	HandleExecutionCallback(ctx context.Context, req ExecutionCallbackRequest) error

	// ==================== Built-in Workflow Execution (Legacy) ====================

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

// CreateSyncPlanRequest represents a request to create a sync plan.
type CreateSyncPlanRequest struct {
	Name           string
	Description    string
	DataSourceID   shared.ID
	DataStoreID    shared.ID
	SelectedAPIs   []string
	CronExpression *string
}

// UpdateSyncPlanRequest represents a request to update a sync plan.
type UpdateSyncPlanRequest struct {
	Name           *string
	Description    *string
	DataStoreID    *shared.ID
	SelectedAPIs   *[]string
	CronExpression *string
}

// ExecuteSyncPlanRequest represents a request to execute a sync plan.
type ExecuteSyncPlanRequest struct {
	TargetDBPath string // 目标数据库路径（必填）
	StartDate    string // 开始日期（必填，格式: "20251201"）
	EndDate      string // 结束日期（必填，格式: "20251231"）
	StartTime    string // 开始时间（可选，格式: "09:30:00"）
	EndTime      string // 结束时间（可选，格式: "15:00:00"）
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
