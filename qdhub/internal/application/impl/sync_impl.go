// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"time"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
)

// SyncApplicationServiceImpl implements SyncApplicationService.
type SyncApplicationServiceImpl struct {
	syncJobRepo       sync.SyncJobRepository
	workflowDefRepo   workflow.WorkflowDefinitionRepository
	taskEngineAdapter workflow.TaskEngineAdapter
	syncValidator     sync.SyncJobValidator
	cronCalculator    sync.CronScheduleCalculator
	jobScheduler      sync.JobScheduler

	// 新增依赖：用于校验数据源和获取 token
	dataSourceRepo metadata.DataSourceRepository

	// 新增依赖：用于执行内建 workflow
	workflowExecutor workflow.WorkflowExecutor
}

// NewSyncApplicationService creates a new SyncApplicationService implementation.
func NewSyncApplicationService(
	syncJobRepo sync.SyncJobRepository,
	workflowDefRepo workflow.WorkflowDefinitionRepository,
	taskEngineAdapter workflow.TaskEngineAdapter,
	cronCalculator sync.CronScheduleCalculator,
	jobScheduler sync.JobScheduler,
	dataSourceRepo metadata.DataSourceRepository,
	workflowExecutor workflow.WorkflowExecutor,
) contracts.SyncApplicationService {
	return &SyncApplicationServiceImpl{
		syncJobRepo:       syncJobRepo,
		workflowDefRepo:   workflowDefRepo,
		taskEngineAdapter: taskEngineAdapter,
		syncValidator:     sync.NewSyncJobValidator(),
		cronCalculator:    cronCalculator,
		jobScheduler:      jobScheduler,
		dataSourceRepo:    dataSourceRepo,
		workflowExecutor:  workflowExecutor,
	}
}

// ==================== Sync Job Management ====================

// CreateSyncJob creates a new sync job.
func (s *SyncApplicationServiceImpl) CreateSyncJob(ctx context.Context, req contracts.CreateSyncJobRequest) (*sync.SyncJob, error) {
	// Create domain entity
	job := sync.NewSyncJob(
		req.Name,
		req.Description,
		req.APIMetadataID,
		req.DataStoreID,
		req.WorkflowDefID,
		req.Mode,
	)

	// Set optional fields
	if req.CronExpression != nil {
		job.SetCronExpression(*req.CronExpression)
	}
	if req.Params != nil {
		job.SetParams(req.Params)
	}
	for _, rule := range req.ParamRules {
		job.AddParamRule(rule)
	}

	// Validate
	if err := s.syncValidator.ValidateSyncJob(job); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Verify workflow definition exists
	wfDef, err := s.workflowDefRepo.Get(req.WorkflowDefID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if wfDef == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}

	// Persist
	if err := s.syncJobRepo.Create(job); err != nil {
		return nil, fmt.Errorf("failed to create sync job: %w", err)
	}

	return job, nil
}

// GetSyncJob retrieves a sync job by ID.
func (s *SyncApplicationServiceImpl) GetSyncJob(ctx context.Context, id shared.ID) (*sync.SyncJob, error) {
	job, err := s.syncJobRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync job: %w", err)
	}
	if job == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync job not found", nil)
	}
	return job, nil
}

// UpdateSyncJob updates a sync job.
func (s *SyncApplicationServiceImpl) UpdateSyncJob(ctx context.Context, id shared.ID, req contracts.UpdateSyncJobRequest) error {
	job, err := s.syncJobRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get sync job: %w", err)
	}
	if job == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync job not found", nil)
	}

	// Cannot update running job
	if job.Status == sync.JobStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot update a running job", nil)
	}

	// Apply updates
	if req.Name != nil {
		job.Name = *req.Name
	}
	if req.Description != nil {
		job.Description = *req.Description
	}
	if req.Mode != nil {
		job.Mode = *req.Mode
	}
	if req.CronExpression != nil {
		job.SetCronExpression(*req.CronExpression)
	}
	if req.Params != nil {
		job.SetParams(*req.Params)
	}
	if req.ParamRules != nil {
		job.ParamRules = *req.ParamRules
	}

	job.UpdatedAt = shared.Now()

	// Validate
	if err := s.syncValidator.ValidateSyncJob(job); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.syncJobRepo.Update(job); err != nil {
		return fmt.Errorf("failed to update sync job: %w", err)
	}

	return nil
}

// DeleteSyncJob deletes a sync job.
func (s *SyncApplicationServiceImpl) DeleteSyncJob(ctx context.Context, id shared.ID) error {
	job, err := s.syncJobRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get sync job: %w", err)
	}
	if job == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync job not found", nil)
	}

	// Cannot delete running job
	if job.Status == sync.JobStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot delete a running job", nil)
	}

	if err := s.syncJobRepo.Delete(id); err != nil {
		return fmt.Errorf("failed to delete sync job: %w", err)
	}

	return nil
}

// ListSyncJobs lists all sync jobs.
func (s *SyncApplicationServiceImpl) ListSyncJobs(ctx context.Context) ([]*sync.SyncJob, error) {
	jobs, err := s.syncJobRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list sync jobs: %w", err)
	}
	return jobs, nil
}

// ==================== Job Execution ====================

// ExecuteSyncJob executes a sync job manually.
func (s *SyncApplicationServiceImpl) ExecuteSyncJob(ctx context.Context, jobID shared.ID) (shared.ID, error) {
	// Get sync job
	job, err := s.syncJobRepo.Get(jobID)
	if err != nil {
		return "", fmt.Errorf("failed to get sync job: %w", err)
	}
	if job == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "sync job not found", nil)
	}

	// Validate job can be executed
	if job.Status == sync.JobStatusRunning {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "job is already running", nil)
	}
	if job.Status == sync.JobStatusDisabled {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "job is disabled", nil)
	}

	// Get workflow definition
	wfDef, err := s.workflowDefRepo.Get(job.WorkflowDefID.String())
	if err != nil {
		return "", fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if wfDef == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}

	// Submit workflow to Task Engine
	engineInstanceID, err := s.taskEngineAdapter.SubmitWorkflow(ctx, wfDef, job.Params)
	if err != nil {
		return "", fmt.Errorf("failed to submit workflow: %w", err)
	}

	// Create workflow instance record
	workflowInstID := shared.ID(engineInstanceID)

	// Create sync execution record
	execution := sync.NewSyncExecution(jobID, workflowInstID)
	execution.MarkRunning()

	if err := s.syncJobRepo.AddExecution(execution); err != nil {
		// Try to cancel the workflow if we failed to record execution
		_ = s.taskEngineAdapter.CancelInstance(ctx, engineInstanceID)
		return "", fmt.Errorf("failed to create sync execution: %w", err)
	}

	// Mark job as running
	job.MarkRunning()
	if err := s.syncJobRepo.Update(job); err != nil {
		return "", fmt.Errorf("failed to update sync job status: %w", err)
	}

	return execution.ID, nil
}

// GetSyncExecution retrieves a sync execution by ID.
func (s *SyncApplicationServiceImpl) GetSyncExecution(ctx context.Context, id shared.ID) (*sync.SyncExecution, error) {
	exec, err := s.syncJobRepo.GetExecution(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}
	return exec, nil
}

// ListSyncExecutions lists all executions for a sync job.
func (s *SyncApplicationServiceImpl) ListSyncExecutions(ctx context.Context, jobID shared.ID) ([]*sync.SyncExecution, error) {
	execs, err := s.syncJobRepo.GetExecutionsByJob(jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to list sync executions: %w", err)
	}
	return execs, nil
}

// CancelExecution cancels a running sync execution.
func (s *SyncApplicationServiceImpl) CancelExecution(ctx context.Context, executionID shared.ID) error {
	exec, err := s.syncJobRepo.GetExecution(executionID)
	if err != nil {
		return fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}

	// Can only cancel running executions
	if exec.Status != sync.ExecStatusRunning && exec.Status != sync.ExecStatusPending {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "execution is not running", nil)
	}

	// Cancel workflow instance in Task Engine
	if err := s.taskEngineAdapter.CancelInstance(ctx, exec.WorkflowInstID.String()); err != nil {
		return fmt.Errorf("failed to cancel workflow instance: %w", err)
	}

	// Update execution status
	exec.MarkCancelled()
	if err := s.syncJobRepo.UpdateExecution(exec); err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}

	// Update job status
	job, err := s.syncJobRepo.Get(exec.SyncJobID)
	if err != nil {
		return fmt.Errorf("failed to get sync job: %w", err)
	}
	if job != nil && job.Status == sync.JobStatusRunning {
		job.MarkCompleted(nil)
		_ = s.syncJobRepo.Update(job)
	}

	return nil
}

// ==================== Scheduling ====================

// EnableJob enables a sync job and schedules it if it has a cron expression.
func (s *SyncApplicationServiceImpl) EnableJob(ctx context.Context, jobID shared.ID) error {
	job, err := s.syncJobRepo.Get(jobID)
	if err != nil {
		return fmt.Errorf("failed to get sync job: %w", err)
	}
	if job == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync job not found", nil)
	}

	if err := job.Enable(); err != nil {
		return err
	}

	// Schedule the job if cron expression is set
	if job.CronExpression != nil && *job.CronExpression != "" {
		// Calculate next run time
		nextRunTime, err := s.cronCalculator.CalculateNextRunTime(*job.CronExpression, time.Now())
		if err != nil {
			return fmt.Errorf("failed to calculate next run time: %w", err)
		}
		job.NextRunAt = nextRunTime

		// Register with scheduler if available
		if s.jobScheduler != nil {
			if err := s.jobScheduler.ScheduleJob(jobID.String(), *job.CronExpression); err != nil {
				return fmt.Errorf("failed to schedule job: %w", err)
			}
		}
	}

	if err := s.syncJobRepo.Update(job); err != nil {
		// Rollback scheduler registration on failure
		if s.jobScheduler != nil {
			s.jobScheduler.UnscheduleJob(jobID.String())
		}
		return fmt.Errorf("failed to update sync job: %w", err)
	}

	return nil
}

// DisableJob disables a sync job and unschedules it.
func (s *SyncApplicationServiceImpl) DisableJob(ctx context.Context, jobID shared.ID) error {
	job, err := s.syncJobRepo.Get(jobID)
	if err != nil {
		return fmt.Errorf("failed to get sync job: %w", err)
	}
	if job == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync job not found", nil)
	}

	if err := job.Disable(); err != nil {
		return err
	}

	job.NextRunAt = nil

	// Unschedule from scheduler
	if s.jobScheduler != nil {
		s.jobScheduler.UnscheduleJob(jobID.String())
	}

	if err := s.syncJobRepo.Update(job); err != nil {
		return fmt.Errorf("failed to update sync job: %w", err)
	}

	return nil
}

// UpdateSchedule updates the cron schedule for a sync job.
func (s *SyncApplicationServiceImpl) UpdateSchedule(ctx context.Context, jobID shared.ID, cronExpression string) error {
	// Validate cron expression
	if err := s.cronCalculator.ParseCronExpression(cronExpression); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	job, err := s.syncJobRepo.Get(jobID)
	if err != nil {
		return fmt.Errorf("failed to get sync job: %w", err)
	}
	if job == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync job not found", nil)
	}

	job.SetCronExpression(cronExpression)

	// Recalculate next run time and reschedule if job is enabled
	if job.Status == sync.JobStatusEnabled {
		nextRunTime, err := s.cronCalculator.CalculateNextRunTime(cronExpression, time.Now())
		if err != nil {
			return fmt.Errorf("failed to calculate next run time: %w", err)
		}
		job.NextRunAt = nextRunTime

		// Reschedule with new expression
		if s.jobScheduler != nil {
			if err := s.jobScheduler.ScheduleJob(jobID.String(), cronExpression); err != nil {
				return fmt.Errorf("failed to reschedule job: %w", err)
			}
		}
	}

	if err := s.syncJobRepo.Update(job); err != nil {
		return fmt.Errorf("failed to update sync job: %w", err)
	}

	return nil
}

// ==================== Callback Handlers ====================

// HandleExecutionCallback handles execution result callback from workflow.
func (s *SyncApplicationServiceImpl) HandleExecutionCallback(ctx context.Context, req contracts.ExecutionCallbackRequest) error {
	exec, err := s.syncJobRepo.GetExecution(req.ExecutionID)
	if err != nil {
		return fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}

	// Update execution status
	if req.Success {
		exec.MarkSuccess(req.RecordCount)
	} else {
		errorMsg := "unknown error"
		if req.ErrorMessage != nil {
			errorMsg = *req.ErrorMessage
		}
		exec.MarkFailed(errorMsg)
	}

	if err := s.syncJobRepo.UpdateExecution(exec); err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	// Update job status
	job, err := s.syncJobRepo.Get(exec.SyncJobID)
	if err != nil {
		return fmt.Errorf("failed to get sync job: %w", err)
	}
	if job != nil {
		// Get next run time from scheduler or calculate it
		var nextRunAt *time.Time
		if job.CronExpression != nil && *job.CronExpression != "" {
			// Try to get from scheduler first (most accurate)
			if s.jobScheduler != nil {
				nextRunAt = s.jobScheduler.GetNextRunTime(exec.SyncJobID.String())
			}
			// Fallback to calculation if scheduler not available or job not scheduled
			if nextRunAt == nil {
				nextRunAt, _ = s.cronCalculator.CalculateNextRunTime(*job.CronExpression, time.Now())
			}
		}
		job.MarkCompleted(nextRunAt)
		_ = s.syncJobRepo.Update(job)
	}

	return nil
}

// ==================== Built-in Workflow Execution ====================

// SyncDataSource executes the batch_data_sync built-in workflow.
// This is a convenience method that validates prerequisites and executes the workflow.
//
// Pre-conditions validated:
//   - Data source must exist (validated using req.DataSourceID)
//   - Token must be configured for the data source
//
// The same DataSourceID is used for both validation and workflow execution
// to ensure consistency.
func (s *SyncApplicationServiceImpl) SyncDataSource(ctx context.Context, req contracts.SyncDataSourceRequest) (shared.ID, error) {
	// 1. 验证数据源是否存在（前置条件校验）
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// 2. 获取 token（必须配置）
	token, err := s.dataSourceRepo.GetTokenByDataSource(req.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}
	if token == nil {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "token not configured for data source", nil)
	}

	// 3. 验证 workflow executor 是否可用
	if s.workflowExecutor == nil {
		return "", fmt.Errorf("workflow executor is not available")
	}

	// 4. 执行内建的 batch_data_sync workflow
	// 使用类型安全的 ExecuteBatchDataSync 方法
	// 注意：req.DataSourceID 既用于上面的校验，也用于 workflow 执行，确保一致性
	instanceID, err := s.workflowExecutor.ExecuteBatchDataSync(ctx, workflow.BatchDataSyncRequest{
		DataSourceName: ds.Name,           // 从校验通过的数据源获取名称
		Token:          token.TokenValue,  // 从校验通过的数据源获取 token
		TargetDBPath:   req.TargetDBPath,
		StartDate:      req.StartDate,
		EndDate:        req.EndDate,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
		APINames:       req.APINames,
		MaxStocks:      req.MaxStocks,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute batch_data_sync workflow: %w", err)
	}

	return instanceID, nil
}

// SyncDataSourceRealtime executes the realtime_data_sync built-in workflow.
// This is a convenience method that validates prerequisites and executes the workflow.
//
// Pre-conditions validated:
//   - Data source must exist (validated using req.DataSourceID)
//   - Token must be configured for the data source
//
// The same DataSourceID is used for both validation and workflow execution
// to ensure consistency.
func (s *SyncApplicationServiceImpl) SyncDataSourceRealtime(ctx context.Context, req contracts.SyncDataSourceRealtimeRequest) (shared.ID, error) {
	// 1. 验证数据源是否存在（前置条件校验）
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// 2. 获取 token（必须配置）
	token, err := s.dataSourceRepo.GetTokenByDataSource(req.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}
	if token == nil {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "token not configured for data source", nil)
	}

	// 3. 验证 workflow executor 是否可用
	if s.workflowExecutor == nil {
		return "", fmt.Errorf("workflow executor is not available")
	}

	// 4. 设置 checkpoint table 默认值
	checkpointTable := req.CheckpointTable
	if checkpointTable == "" {
		checkpointTable = "sync_checkpoint"
	}

	// 5. 执行内建的 realtime_data_sync workflow
	// 使用类型安全的 ExecuteRealtimeDataSync 方法
	// 注意：req.DataSourceID 既用于上面的校验，也用于 workflow 执行，确保一致性
	instanceID, err := s.workflowExecutor.ExecuteRealtimeDataSync(ctx, workflow.RealtimeDataSyncRequest{
		DataSourceName:  ds.Name,          // 从校验通过的数据源获取名称
		Token:           token.TokenValue, // 从校验通过的数据源获取 token
		TargetDBPath:    req.TargetDBPath,
		CheckpointTable: checkpointTable,
		APINames:        req.APINames,
		MaxStocks:       req.MaxStocks,
		CronExpr:        req.CronExpr,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute realtime_data_sync workflow: %w", err)
	}

	return instanceID, nil
}
