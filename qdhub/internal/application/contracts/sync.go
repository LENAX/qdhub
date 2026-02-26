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

	// GetPlanSummary returns the latest execution summary for a sync plan (or nil if never executed).
	GetPlanSummary(ctx context.Context, planID shared.ID) (*PlanSummary, error)

	// ListPlanExecutionHistory returns paginated execution history for a sync plan.
	ListPlanExecutionHistory(ctx context.Context, planID shared.ID, limit, offset int) ([]*sync.SyncExecution, int, error)

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

	// HandleExecutionCallbackByWorkflowInstance looks up execution by workflow instance ID,
	// then invokes HandleExecutionCallback. Used by DataSyncCompleteHandler (task engine).
	HandleExecutionCallbackByWorkflowInstance(ctx context.Context, workflowInstID string, success bool, recordCount int64, errMsg *string) error

	// ==================== Progress Query ====================

	// GetExecutionProgress retrieves aggregated progress for a specific sync execution.
	// It combines SyncExecution domain state with underlying workflow instance progress.
	GetExecutionProgress(ctx context.Context, executionID shared.ID) (*SyncExecutionProgress, error)

	// GetPlanProgress retrieves aggregated progress for the latest execution of a sync plan.
	// If the plan has never been executed, it returns a pending progress state.
	GetPlanProgress(ctx context.Context, planID shared.ID) (*SyncExecutionProgress, error)
}

// ==================== Request/Response DTOs ====================

// CreateSyncPlanRequest represents a request to create a sync plan.
type CreateSyncPlanRequest struct {
	Name                 string
	Description          string
	DataSourceID         shared.ID
	DataStoreID          shared.ID
	SelectedAPIs         []string
	CronExpression       *string
	DefaultExecuteParams *sync.ExecuteParams
	IncrementalMode      bool
}

// UpdateSyncPlanRequest represents a request to update a sync plan.
type UpdateSyncPlanRequest struct {
	Name                 *string
	Description          *string
	DataStoreID          *shared.ID
	SelectedAPIs         *[]string
	CronExpression       *string
	DefaultExecuteParams *sync.ExecuteParams
	IncrementalMode      *bool
}

// ExecuteSyncPlanRequest represents a request to execute a sync plan.
// TargetDBPath is resolved from the plan's associated data store; only date/time may be passed.
type ExecuteSyncPlanRequest struct {
	StartDate string // 开始日期（可选，格式: "20251201"，未传时用计划 default_execute_params）
	EndDate   string // 结束日期（可选，格式: "20251231"）
	StartTime string // 开始时间（可选，格式: "09:30:00"）
	EndTime   string // 结束时间（可选，格式: "15:00:00"）
}

// ExecutionCallbackRequest represents a callback request from workflow engine.
type ExecutionCallbackRequest struct {
	ExecutionID  shared.ID
	Success      bool
	RecordCount  int64
	ErrorMessage *string
}

// SyncExecutionProgress represents aggregated progress information for a sync execution.
// It combines SyncExecution state with underlying workflow instance progress.
type SyncExecutionProgress struct {
	// Identifiers
	ExecutionID        shared.ID
	PlanID             shared.ID
	WorkflowInstanceID shared.ID

	// High-level status (normalized)
	Status sync.ExecStatus

	// Workflow progress
	Progress       float64
	TaskCount      int
	CompletedTask  int
	FailedTask     int
	RunningCount   int      // 正在运行的任务数（来自引擎快照时与内部一致）
	PendingCount   int      // 挂起的任务数（来自引擎快照时与内部一致）
	RunningTaskIDs []string // 正在运行的任务 ID（存储可能滞后）
	PendingTaskIDs []string // 挂起的任务 ID（存储可能滞后）

	// Execution result
	RecordCount  int64
	ErrorMessage *string

	// Timeline
	StartedAt  shared.Timestamp
	FinishedAt *shared.Timestamp
}

// PlanSummary represents the latest execution summary for a sync plan.
// Returned by GetPlanSummary; nil when the plan has no executions.
type PlanSummary struct {
	ExecutionID  shared.ID         `json:"execution_id"`
	Status       sync.ExecStatus    `json:"status"`
	StartedAt    shared.Timestamp  `json:"started_at"`
	FinishedAt   *shared.Timestamp `json:"finished_at,omitempty"`
	RecordCount  int64             `json:"record_count"`
	ErrorMessage *string           `json:"error_message,omitempty"`
	SyncedAPIs   []string          `json:"synced_apis,omitempty"`
	SkippedAPIs  []string          `json:"skipped_apis,omitempty"`
}
