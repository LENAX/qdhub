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
	syncPlanRepo   sync.SyncPlanRepository
	cronCalculator sync.CronScheduleCalculator
	planScheduler  sync.PlanScheduler

	// 依赖：用于校验数据源和获取 token
	dataSourceRepo metadata.DataSourceRepository

	// 依赖：用于执行内建 workflow
	workflowExecutor workflow.WorkflowExecutor

	// 依赖：用于解析依赖关系
	dependencyResolver sync.DependencyResolver
}

// NewSyncApplicationService creates a new SyncApplicationService implementation.
func NewSyncApplicationService(
	syncPlanRepo sync.SyncPlanRepository,
	cronCalculator sync.CronScheduleCalculator,
	planScheduler sync.PlanScheduler,
	dataSourceRepo metadata.DataSourceRepository,
	workflowExecutor workflow.WorkflowExecutor,
	dependencyResolver sync.DependencyResolver,
) contracts.SyncApplicationService {
	return &SyncApplicationServiceImpl{
		syncPlanRepo:       syncPlanRepo,
		cronCalculator:     cronCalculator,
		planScheduler:      planScheduler,
		dataSourceRepo:     dataSourceRepo,
		workflowExecutor:   workflowExecutor,
		dependencyResolver: dependencyResolver,
	}
}

// ==================== Sync Plan Management ====================

// CreateSyncPlan creates a new sync plan.
func (s *SyncApplicationServiceImpl) CreateSyncPlan(ctx context.Context, req contracts.CreateSyncPlanRequest) (*sync.SyncPlan, error) {
	// Validate data source exists
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Create domain entity
	plan := sync.NewSyncPlan(
		req.Name,
		req.Description,
		req.DataSourceID,
		req.SelectedAPIs,
	)

	// Set optional fields
	if req.DataStoreID != "" {
		plan.SetDataStore(req.DataStoreID)
	}
	if req.CronExpression != nil {
		plan.SetCronExpression(*req.CronExpression)
	}

	// Persist
	if err := s.syncPlanRepo.Create(plan); err != nil {
		return nil, fmt.Errorf("failed to create sync plan: %w", err)
	}

	return plan, nil
}

// GetSyncPlan retrieves a sync plan by ID.
func (s *SyncApplicationServiceImpl) GetSyncPlan(ctx context.Context, id shared.ID) (*sync.SyncPlan, error) {
	plan, err := s.syncPlanRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}
	return plan, nil
}

// UpdateSyncPlan updates a sync plan.
func (s *SyncApplicationServiceImpl) UpdateSyncPlan(ctx context.Context, id shared.ID, req contracts.UpdateSyncPlanRequest) error {
	plan, err := s.syncPlanRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	// Cannot update running plan
	if plan.Status == sync.PlanStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot update a running plan", nil)
	}

	// Apply updates
	if req.Name != nil {
		plan.Name = *req.Name
	}
	if req.Description != nil {
		plan.Description = *req.Description
	}
	if req.DataStoreID != nil {
		plan.SetDataStore(*req.DataStoreID)
	}
	if req.SelectedAPIs != nil {
		plan.SelectedAPIs = *req.SelectedAPIs
		// Reset to draft status if APIs changed
		plan.Status = sync.PlanStatusDraft
		plan.ExecutionGraph = nil
		plan.ResolvedAPIs = nil
	}
	if req.CronExpression != nil {
		plan.SetCronExpression(*req.CronExpression)
	}

	plan.UpdatedAt = shared.Now()

	// Persist
	if err := s.syncPlanRepo.Update(plan); err != nil {
		return fmt.Errorf("failed to update sync plan: %w", err)
	}

	return nil
}

// DeleteSyncPlan deletes a sync plan.
func (s *SyncApplicationServiceImpl) DeleteSyncPlan(ctx context.Context, id shared.ID) error {
	plan, err := s.syncPlanRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	// Cannot delete running plan
	if plan.Status == sync.PlanStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot delete a running plan", nil)
	}

	if err := s.syncPlanRepo.Delete(id); err != nil {
		return fmt.Errorf("failed to delete sync plan: %w", err)
	}

	return nil
}

// ListSyncPlans lists all sync plans.
func (s *SyncApplicationServiceImpl) ListSyncPlans(ctx context.Context) ([]*sync.SyncPlan, error) {
	plans, err := s.syncPlanRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list sync plans: %w", err)
	}
	return plans, nil
}

// ResolveSyncPlan resolves dependencies for a sync plan.
func (s *SyncApplicationServiceImpl) ResolveSyncPlan(ctx context.Context, planID shared.ID) error {
	plan, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	// Get all API metadata for the data source
	allAPIs, err := s.dataSourceRepo.ListAPIMetadataByDataSource(plan.DataSourceID)
	if err != nil {
		return fmt.Errorf("failed to get API metadata: %w", err)
	}

	// Build dependency map
	allAPIDependencies := make(map[string][]sync.ParamDependency)
	for _, api := range allAPIs {
		deps := make([]sync.ParamDependency, len(api.ParamDependencies))
		for i, dep := range api.ParamDependencies {
			deps[i] = sync.ParamDependency{
				ParamName:   dep.ParamName,
				SourceAPI:   dep.SourceAPI,
				SourceField: dep.SourceField,
				IsList:      dep.IsList,
				FilterField: dep.FilterField,
				FilterValue: dep.FilterValue,
			}
		}
		allAPIDependencies[api.Name] = deps
	}

	// Resolve dependencies
	graph, resolvedAPIs, err := s.dependencyResolver.Resolve(plan.SelectedAPIs, allAPIDependencies)
	if err != nil {
		return fmt.Errorf("failed to resolve dependencies: %w", err)
	}

	// Update plan with resolved graph
	plan.SetExecutionGraph(graph, resolvedAPIs)

	// Delete existing tasks
	if err := s.syncPlanRepo.DeleteTasksByPlan(planID); err != nil {
		return fmt.Errorf("failed to delete existing tasks: %w", err)
	}

	// Create SyncTask entities from ExecutionGraph
	sortOrder := 0
	for level, apis := range graph.Levels {
		for _, apiName := range apis {
			taskConfig := graph.TaskConfigs[apiName]
			task := sync.NewSyncTask(apiName, taskConfig.SyncMode, level)
			task.SetDependencies(taskConfig.Dependencies)
			task.SetParamMappings(taskConfig.ParamMappings)
			task.SortOrder = sortOrder
			sortOrder++

			plan.AddTask(task)
			if err := s.syncPlanRepo.AddTask(task); err != nil {
				return fmt.Errorf("failed to create sync task: %w", err)
			}
		}
	}

	// Persist plan
	if err := s.syncPlanRepo.Update(plan); err != nil {
		return fmt.Errorf("failed to update sync plan: %w", err)
	}

	return nil
}

// ==================== Plan Execution ====================

// ExecuteSyncPlan executes a sync plan.
func (s *SyncApplicationServiceImpl) ExecuteSyncPlan(ctx context.Context, planID shared.ID, req contracts.ExecuteSyncPlanRequest) (shared.ID, error) {
	// Get sync plan
	plan, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return "", fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	// Validate plan can be executed
	if plan.Status == sync.PlanStatusRunning {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "plan is already running", nil)
	}
	if plan.Status == sync.PlanStatusDraft {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "plan dependencies not resolved", nil)
	}
	if plan.Status == sync.PlanStatusDisabled {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "plan is disabled", nil)
	}

	// Get tasks
	tasks, err := s.syncPlanRepo.GetTasksByPlan(planID)
	if err != nil {
		return "", fmt.Errorf("failed to get tasks: %w", err)
	}
	if len(tasks) == 0 {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "no tasks found for plan", nil)
	}

	// Filter tasks by sync frequency
	needSyncTasks, skipAPIs := s.filterTasksByFrequency(tasks)
	if len(needSyncTasks) == 0 {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "all tasks are skipped due to sync frequency", nil)
	}

	// Get data source info
	ds, err := s.dataSourceRepo.Get(plan.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Get token
	token, err := s.dataSourceRepo.GetTokenByDataSource(plan.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}
	if token == nil {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "token not configured for data source", nil)
	}

	// Convert to API configs (for future use with advanced workflow execution)
	_ = s.convertToAPIConfigs(plan.ExecutionGraph, needSyncTasks)

	// Execute workflow
	instanceID, err := s.workflowExecutor.ExecuteBatchDataSync(ctx, workflow.BatchDataSyncRequest{
		DataSourceName: ds.Name,
		Token:          token.TokenValue,
		TargetDBPath:   req.TargetDBPath,
		StartDate:      req.StartDate,
		EndDate:        req.EndDate,
		StartTime:      req.StartTime,
		EndTime:        req.EndTime,
		APINames:       s.extractAPINames(needSyncTasks),
		MaxStocks:      0,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute workflow: %w", err)
	}

	// Create sync execution record
	execution := sync.NewSyncExecution(planID, instanceID)
	execution.ExecuteParams = &sync.ExecuteParams{
		TargetDBPath: req.TargetDBPath,
		StartDate:    req.StartDate,
		EndDate:      req.EndDate,
		StartTime:    req.StartTime,
		EndTime:      req.EndTime,
	}
	execution.SyncedAPIs = s.extractAPINames(needSyncTasks)
	execution.SkippedAPIs = skipAPIs
	execution.MarkRunning()

	if err := s.syncPlanRepo.AddPlanExecution(execution); err != nil {
		return "", fmt.Errorf("failed to create sync execution: %w", err)
	}

	// Mark plan as running
	plan.MarkRunning()
	if err := s.syncPlanRepo.Update(plan); err != nil {
		return "", fmt.Errorf("failed to update sync plan status: %w", err)
	}

	return execution.ID, nil
}

// filterTasksByFrequency filters tasks based on sync frequency.
func (s *SyncApplicationServiceImpl) filterTasksByFrequency(tasks []*sync.SyncTask) (needSync []*sync.SyncTask, skipAPIs []string) {
	needSync = make([]*sync.SyncTask, 0)
	skipAPIs = make([]string, 0)

	for _, task := range tasks {
		if task.NeedsSync() {
			needSync = append(needSync, task)
		} else {
			skipAPIs = append(skipAPIs, task.APIName)
		}
	}

	return needSync, skipAPIs
}

// convertToAPIConfigs converts ExecutionGraph and tasks to API configs.
func (s *SyncApplicationServiceImpl) convertToAPIConfigs(graph *sync.ExecutionGraph, tasks []*sync.SyncTask) []workflow.APISyncConfig {
	configs := make([]workflow.APISyncConfig, 0, len(tasks))

	for _, task := range tasks {
		config := workflow.APISyncConfig{
			APIName:  task.APIName,
			SyncMode: task.SyncMode.String(),
		}

		// Add dependencies
		if len(task.Dependencies) > 0 {
			config.Dependencies = task.Dependencies
		}

		// Add params
		if len(task.Params) > 0 {
			config.ExtraParams = task.Params
		}

		configs = append(configs, config)
	}

	return configs
}

// extractAPINames extracts API names from tasks.
func (s *SyncApplicationServiceImpl) extractAPINames(tasks []*sync.SyncTask) []string {
	names := make([]string, len(tasks))
	for i, task := range tasks {
		names[i] = task.APIName
	}
	return names
}

// GetSyncExecution retrieves a sync execution by ID.
func (s *SyncApplicationServiceImpl) GetSyncExecution(ctx context.Context, id shared.ID) (*sync.SyncExecution, error) {
	exec, err := s.syncPlanRepo.GetPlanExecution(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}
	return exec, nil
}

// ListPlanExecutions lists all executions for a sync plan.
func (s *SyncApplicationServiceImpl) ListPlanExecutions(ctx context.Context, planID shared.ID) ([]*sync.SyncExecution, error) {
	execs, err := s.syncPlanRepo.GetExecutionsByPlan(planID)
	if err != nil {
		return nil, fmt.Errorf("failed to list sync executions: %w", err)
	}
	return execs, nil
}

// CancelExecution cancels a running sync execution.
func (s *SyncApplicationServiceImpl) CancelExecution(ctx context.Context, executionID shared.ID) error {
	exec, err := s.syncPlanRepo.GetPlanExecution(executionID)
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

	// Update execution status
	exec.MarkCancelled()
	if err := s.syncPlanRepo.UpdatePlanExecution(exec); err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}

	// Update plan status
	plan, err := s.syncPlanRepo.Get(exec.SyncPlanID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan != nil && plan.Status == sync.PlanStatusRunning {
		plan.MarkCompleted(nil)
		_ = s.syncPlanRepo.Update(plan)
	}

	return nil
}

// ==================== Scheduling ====================

// EnablePlan enables a sync plan and schedules it if it has a cron expression.
func (s *SyncApplicationServiceImpl) EnablePlan(ctx context.Context, planID shared.ID) error {
	plan, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	if err := plan.Enable(); err != nil {
		return err
	}

	// Schedule the plan if cron expression is set
	if plan.CronExpression != nil && *plan.CronExpression != "" {
		// Calculate next run time
		nextRunTime, err := s.cronCalculator.CalculateNextRunTime(*plan.CronExpression, time.Now())
		if err != nil {
			return fmt.Errorf("failed to calculate next run time: %w", err)
		}
		plan.NextExecuteAt = nextRunTime

		// Register with scheduler if available
		if s.planScheduler != nil {
			if err := s.planScheduler.SchedulePlan(planID.String(), *plan.CronExpression); err != nil {
				return fmt.Errorf("failed to schedule plan: %w", err)
			}
		}
	}

	if err := s.syncPlanRepo.Update(plan); err != nil {
		// Rollback scheduler registration on failure
		if s.planScheduler != nil {
			s.planScheduler.UnschedulePlan(planID.String())
		}
		return fmt.Errorf("failed to update sync plan: %w", err)
	}

	return nil
}

// DisablePlan disables a sync plan and unschedules it.
func (s *SyncApplicationServiceImpl) DisablePlan(ctx context.Context, planID shared.ID) error {
	plan, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	if err := plan.Disable(); err != nil {
		return err
	}

	plan.NextExecuteAt = nil

	// Unschedule from scheduler
	if s.planScheduler != nil {
		s.planScheduler.UnschedulePlan(planID.String())
	}

	if err := s.syncPlanRepo.Update(plan); err != nil {
		return fmt.Errorf("failed to update sync plan: %w", err)
	}

	return nil
}

// UpdatePlanSchedule updates the cron schedule for a sync plan.
func (s *SyncApplicationServiceImpl) UpdatePlanSchedule(ctx context.Context, planID shared.ID, cronExpression string) error {
	// Validate cron expression
	if err := s.cronCalculator.ParseCronExpression(cronExpression); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	plan, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	plan.SetCronExpression(cronExpression)

	// Recalculate next run time and reschedule if plan is enabled
	if plan.Status == sync.PlanStatusEnabled {
		nextRunTime, err := s.cronCalculator.CalculateNextRunTime(cronExpression, time.Now())
		if err != nil {
			return fmt.Errorf("failed to calculate next run time: %w", err)
		}
		plan.NextExecuteAt = nextRunTime

		// Reschedule with new expression
		if s.planScheduler != nil {
			if err := s.planScheduler.SchedulePlan(planID.String(), cronExpression); err != nil {
				return fmt.Errorf("failed to reschedule plan: %w", err)
			}
		}
	}

	if err := s.syncPlanRepo.Update(plan); err != nil {
		return fmt.Errorf("failed to update sync plan: %w", err)
	}

	return nil
}

// ==================== Callback Handlers ====================

// HandleExecutionCallback handles execution result callback from workflow.
func (s *SyncApplicationServiceImpl) HandleExecutionCallback(ctx context.Context, req contracts.ExecutionCallbackRequest) error {
	exec, err := s.syncPlanRepo.GetPlanExecution(req.ExecutionID)
	if err != nil {
		return fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}

	// Update execution status
	if req.Success {
		exec.MarkSuccess(req.RecordCount)

		// Update LastSyncedAt for synced tasks
		tasks, err := s.syncPlanRepo.GetTasksByPlan(exec.SyncPlanID)
		if err == nil {
			for _, task := range tasks {
				for _, syncedAPI := range exec.SyncedAPIs {
					if task.APIName == syncedAPI {
						task.MarkSynced()
						_ = s.syncPlanRepo.UpdateTask(task)
						break
					}
				}
			}
		}
	} else {
		errorMsg := "unknown error"
		if req.ErrorMessage != nil {
			errorMsg = *req.ErrorMessage
		}
		exec.MarkFailed(errorMsg)
	}

	if err := s.syncPlanRepo.UpdatePlanExecution(exec); err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	// Update plan status
	plan, err := s.syncPlanRepo.Get(exec.SyncPlanID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan != nil {
		// Get next run time from scheduler or calculate it
		var nextRunAt *time.Time
		if plan.CronExpression != nil && *plan.CronExpression != "" {
			// Try to get from scheduler first (most accurate)
			if s.planScheduler != nil {
				nextRunAt = s.planScheduler.GetNextRunTime(exec.SyncPlanID.String())
			}
			// Fallback to calculation if scheduler not available or plan not scheduled
			if nextRunAt == nil {
				nextRunAt, _ = s.cronCalculator.CalculateNextRunTime(*plan.CronExpression, time.Now())
			}
		}
		plan.MarkCompleted(nextRunAt)
		_ = s.syncPlanRepo.Update(plan)
	}

	return nil
}

// ==================== Built-in Workflow Execution (Legacy) ====================

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
