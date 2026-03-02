// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
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

	// 依赖：用于从 plan 关联的 data store 解析 target_db_path
	dataStoreRepo datastore.QuantDataStoreRepository

	// 依赖：用于执行内建 workflow
	workflowExecutor workflow.WorkflowExecutor

	// 依赖：用于解析依赖关系
	dependencyResolver sync.DependencyResolver

	// 依赖：用于查询 workflow 实例进度（直接对接 Task Engine）
	taskEngineAdapter workflow.TaskEngineAdapter

	// 依赖：Unit of Work 用于事务控制
	uow contracts.UnitOfWork

	// 依赖：用于查询 api_sync_strategies 补充 param_dependencies
	metadataRepo metadata.Repository

	// 可选：用于增量模式下从目标 DuckDB 查询 MAX(列) 得到数据最新日期
	quantDBFactory datastore.QuantDBFactory
}

// NewSyncApplicationService creates a new SyncApplicationService implementation.
func NewSyncApplicationService(
	syncPlanRepo sync.SyncPlanRepository,
	cronCalculator sync.CronScheduleCalculator,
	planScheduler sync.PlanScheduler,
	dataSourceRepo metadata.DataSourceRepository,
	dataStoreRepo datastore.QuantDataStoreRepository,
	workflowExecutor workflow.WorkflowExecutor,
	dependencyResolver sync.DependencyResolver,
	taskEngineAdapter workflow.TaskEngineAdapter,
	uow contracts.UnitOfWork,
	metadataRepo metadata.Repository,
	quantDBFactory datastore.QuantDBFactory,
) contracts.SyncApplicationService {
	return &SyncApplicationServiceImpl{
		syncPlanRepo:       syncPlanRepo,
		cronCalculator:     cronCalculator,
		planScheduler:      planScheduler,
		dataSourceRepo:     dataSourceRepo,
		dataStoreRepo:      dataStoreRepo,
		workflowExecutor:   workflowExecutor,
		dependencyResolver: dependencyResolver,
		taskEngineAdapter:  taskEngineAdapter,
		uow:                uow,
		metadataRepo:       metadataRepo,
		quantDBFactory:     quantDBFactory,
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
	if req.DefaultExecuteParams != nil {
		plan.SetDefaultExecuteParams(req.DefaultExecuteParams)
	}
	plan.SetIncrementalMode(req.IncrementalMode)
	if req.IncrementalStartDateAPI != "" || req.IncrementalStartDateColumn != "" {
		plan.SetIncrementalStartDateSource(req.IncrementalStartDateAPI, req.IncrementalStartDateColumn)
	}

	// Persist
	if err := s.syncPlanRepo.Create(plan); err != nil {
		return nil, fmt.Errorf("failed to create sync plan: %w", err)
	}

	s.reconcileScheduledPlans()
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
		newCron := *req.CronExpression
		if newCron != "" {
			if err := s.cronCalculator.ParseCronExpression(newCron); err != nil {
				return fmt.Errorf("invalid cron expression: %w", err)
			}
		}
		plan.SetCronExpression(newCron)
		// 非 disabled 即纳入 cron：resolved/enabled 等都会同步到调度器
		if plan.Status != sync.PlanStatusDisabled {
			if newCron != "" {
				nextRunTime, err := s.cronCalculator.CalculateNextRunTime(newCron, time.Now())
				if err != nil {
					return fmt.Errorf("failed to calculate next run time: %w", err)
				}
				plan.NextExecuteAt = nextRunTime
				if s.planScheduler != nil {
					if err := s.planScheduler.SchedulePlan(id.String(), newCron); err != nil {
						return fmt.Errorf("failed to reschedule plan: %w", err)
					}
				}
			} else {
				plan.NextExecuteAt = nil
				if s.planScheduler != nil {
					s.planScheduler.UnschedulePlan(id.String())
				}
			}
		}
	}
	if req.DefaultExecuteParams != nil {
		plan.SetDefaultExecuteParams(req.DefaultExecuteParams)
	}
	if req.IncrementalMode != nil {
		plan.SetIncrementalMode(*req.IncrementalMode)
	}
	if req.IncrementalStartDateAPI != nil || req.IncrementalStartDateColumn != nil {
		api, col := "", ""
		if req.IncrementalStartDateAPI != nil {
			api = *req.IncrementalStartDateAPI
		}
		if req.IncrementalStartDateColumn != nil {
			col = *req.IncrementalStartDateColumn
		}
		plan.SetIncrementalStartDateSource(api, col)
	}

	plan.UpdatedAt = shared.Now()

	// Persist
	if err := s.syncPlanRepo.Update(plan); err != nil {
		return fmt.Errorf("failed to update sync plan: %w", err)
	}

	s.reconcileScheduledPlans()
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

	if s.planScheduler != nil {
		s.planScheduler.UnschedulePlan(id.String())
	}
	return nil
}

// ListSyncPlans lists all sync plans.
func (s *SyncApplicationServiceImpl) ListSyncPlans(ctx context.Context) ([]*sync.SyncPlan, error) {
	plans, err := s.syncPlanRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list sync plans: %w", err)
	}

	for _, plan := range plans {
		if plan.Status == sync.PlanStatusRunning {
			s.tryRecoverStalePlan(ctx, plan)
		}
	}

	// 填充最近一次执行状态，供前端展示（启用/就绪/成功/失败等）
	for _, plan := range plans {
		execs, err := s.syncPlanRepo.GetExecutionsByPlan(plan.ID)
		if err != nil || len(execs) == 0 {
			continue
		}
		plan.LastExecutionStatus = &execs[0].Status
	}

	return plans, nil
}

// tryRecoverStalePlan checks whether a running plan's latest execution has actually
// completed in the workflow engine, and if so, marks both execution and plan as completed.
func (s *SyncApplicationServiceImpl) tryRecoverStalePlan(ctx context.Context, plan *sync.SyncPlan) {
	execs, err := s.syncPlanRepo.GetExecutionsByPlan(plan.ID)
	if err != nil || len(execs) == 0 {
		return
	}
	latest := execs[0]
	if latest.Status != sync.ExecStatusRunning && latest.Status != sync.ExecStatusPending {
		// Execution already terminal but plan stuck — just mark plan completed
		var nextRunAt *time.Time
		if plan.CronExpression != nil && *plan.CronExpression != "" {
			if s.planScheduler != nil {
				nextRunAt = s.planScheduler.GetNextRunTime(plan.ID.String())
			}
			if nextRunAt == nil {
				nextRunAt, _ = s.cronCalculator.CalculateNextRunTime(*plan.CronExpression, time.Now())
			}
		}
		plan.MarkCompleted(nextRunAt)
		if updateErr := s.syncPlanRepo.Update(plan); updateErr != nil {
			logrus.Warnf("[ListSyncPlans] Failed to recover plan %s: %v", plan.ID, updateErr)
		} else {
			logrus.Infof("[ListSyncPlans] Recovered stale plan %s (execution already terminal)", plan.ID)
		}
		return
	}

	if s.taskEngineAdapter == nil || latest.WorkflowInstID.IsEmpty() {
		return
	}

	wfStatus, err := s.taskEngineAdapter.GetInstanceStatus(ctx, latest.WorkflowInstID.String())
	if err != nil || wfStatus == nil {
		return
	}

	statusUpper := strings.ToUpper(wfStatus.Status)
	var newExecStatus sync.ExecStatus
	switch statusUpper {
	case "SUCCESS", "COMPLETED":
		newExecStatus = sync.ExecStatusSuccess
	case "FAILED", "ERROR":
		newExecStatus = sync.ExecStatusFailed
	case "TERMINATED", "CANCELLED":
		newExecStatus = sync.ExecStatusCancelled
	default:
		return
	}

	switch newExecStatus {
	case sync.ExecStatusSuccess:
		latest.MarkSuccess(latest.RecordCount)
	case sync.ExecStatusFailed:
		errMsg := ""
		if wfStatus.ErrorMessage != nil {
			errMsg = *wfStatus.ErrorMessage
		}
		latest.MarkFailed(errMsg)
	case sync.ExecStatusCancelled:
		latest.MarkCancelled()
	}
	if updateErr := s.syncPlanRepo.UpdatePlanExecution(latest); updateErr != nil {
		logrus.Warnf("[ListSyncPlans] Failed to update execution %s: %v", latest.ID, updateErr)
		return
	}

	var nextRunAt *time.Time
	if plan.CronExpression != nil && *plan.CronExpression != "" {
		if s.planScheduler != nil {
			nextRunAt = s.planScheduler.GetNextRunTime(plan.ID.String())
		}
		if nextRunAt == nil {
			nextRunAt, _ = s.cronCalculator.CalculateNextRunTime(*plan.CronExpression, time.Now())
		}
	}
	plan.MarkCompleted(nextRunAt)
	if updateErr := s.syncPlanRepo.Update(plan); updateErr != nil {
		logrus.Warnf("[ListSyncPlans] Failed to recover plan %s: %v", plan.ID, updateErr)
	} else {
		logrus.Infof("[ListSyncPlans] Recovered stale plan %s from workflow status %s", plan.ID, wfStatus.Status)
	}
}

// ResolveSyncPlan resolves dependencies for a sync plan.
func (s *SyncApplicationServiceImpl) ResolveSyncPlan(ctx context.Context, planID shared.ID) error {
	// Read operations (no transaction needed)
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

	// Fix A: Supplement missing param_dependencies from api_sync_strategies.
	// api_metadata.param_dependencies may be empty (e.g. metadata crawl doesn't populate it),
	// but api_sync_strategies has the correct preferred_param / support_date_range info.
	s.supplementDependenciesFromStrategies(ctx, plan.DataSourceID, allAPIs, allAPIDependencies)

	// Resolve dependencies
	graph, resolvedAPIs, err := s.dependencyResolver.Resolve(plan.SelectedAPIs, allAPIDependencies)
	if err != nil {
		return fmt.Errorf("failed to resolve dependencies: %w", err)
	}

	// Update plan with resolved graph
	plan.SetExecutionGraph(graph, resolvedAPIs)

	// Write operations using UoW for transaction
	return s.uow.Do(ctx, func(repos contracts.Repositories) error {
		// Delete existing tasks
		if err := repos.SyncPlanRepo().DeleteTasksByPlan(planID); err != nil {
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
				if err := repos.SyncPlanRepo().AddTask(task); err != nil {
					return fmt.Errorf("failed to create sync task: %w", err)
				}
			}
		}

		// Persist plan
		if err := repos.SyncPlanRepo().Update(plan); err != nil {
			return fmt.Errorf("failed to update sync plan: %w", err)
		}

		return nil
	})
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

	if plan.DataStoreID == "" {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "sync plan has no data store configured", nil)
	}
	dataStore, err := s.dataStoreRepo.Get(plan.DataStoreID)
	if err != nil {
		return "", fmt.Errorf("failed to get data store: %w", err)
	}
	if dataStore == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}
	if strings.TrimSpace(dataStore.StoragePath) == "" {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "data store has no storage path", nil)
	}
	targetDBPath := dataStore.StoragePath

	// 合并请求与计划默认参数：仅传计划 id 时使用 default_execute_params；target 始终来自 data store
	eff := struct {
		TargetDBPath string
		StartDate    string
		EndDate      string
		StartTime    string
		EndTime      string
	}{TargetDBPath: targetDBPath, StartDate: req.StartDate, EndDate: req.EndDate, StartTime: req.StartTime, EndTime: req.EndTime}
	if plan.DefaultExecuteParams != nil {
		p := plan.DefaultExecuteParams
		// 非增量模式：若调用方未显式传入日期，则使用计划默认日期范围
		if !plan.IncrementalMode {
			if eff.StartDate == "" {
				eff.StartDate = p.StartDate
			}
			if eff.EndDate == "" {
				eff.EndDate = p.EndDate
			}
		}
		// 无论是否为增量模式，时间段均可从默认参数补充
		if eff.StartTime == "" {
			eff.StartTime = p.StartTime
		}
		if eff.EndTime == "" {
			eff.EndTime = p.EndTime
		}
	}
	// 增量模式且日期未传：用 min(上次成功结束日, 数据最新日期) 作为起始，结束日默认为今天
	requiresDate := s.planRequiresDateRange(plan)
	if requiresDate && plan.IncrementalMode && (eff.StartDate == "" || eff.EndDate == "") {
		var startCandidates []string
		if plan.LastSuccessfulEndDate != nil && *plan.LastSuccessfulEndDate != "" {
			startCandidates = append(startCandidates, *plan.LastSuccessfulEndDate)
		}
		if plan.IncrementalStartDateAPI != nil && plan.IncrementalStartDateColumn != nil &&
			*plan.IncrementalStartDateAPI != "" && *plan.IncrementalStartDateColumn != "" && s.quantDBFactory != nil {
			dataDate := s.getMaxDateFromTargetDB(ctx, targetDBPath, *plan.IncrementalStartDateAPI, *plan.IncrementalStartDateColumn)
			if dataDate != "" {
				startCandidates = append(startCandidates, dataDate)
			}
		}
		if len(startCandidates) > 0 {
			eff.StartDate = startCandidates[0]
			for i := 1; i < len(startCandidates); i++ {
				if startCandidates[i] < eff.StartDate {
					eff.StartDate = startCandidates[i]
				}
			}
		} else if plan.DefaultExecuteParams != nil && eff.StartDate == "" {
			eff.StartDate = plan.DefaultExecuteParams.StartDate
		}
		eff.EndDate = time.Now().Format("20060102")
		logrus.Infof("[ExecuteSyncPlan] IncrementalMode=true for plan %s, resolved date range: %s -> %s (requiresDate=%v)",
			plan.ID, eff.StartDate, eff.EndDate, requiresDate)
	}
	// 仅当计划内任一 API 的参数包含 date/time/dt 等模式时才要求配置日期范围
	if requiresDate && (eff.StartDate == "" || eff.EndDate == "") {
		return "", shared.NewDomainError(shared.ErrCodeValidation,
			"missing date range: set default_execute_params on the plan or pass start_dt, end_dt", nil)
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

	// 使用计划解析出的任务配置（依赖与参数来源），避免工作流仅按 APINames 用默认策略推断
	apiConfigs := s.convertToAPIConfigs(plan.ExecutionGraph, needSyncTasks)

	// Execute workflow (external async operation, not part of transaction)
	instanceID, err := s.workflowExecutor.ExecuteBatchDataSync(ctx, workflow.BatchDataSyncRequest{
		DataSourceID:   plan.DataSourceID,
		DataSourceName: ds.Name,
		Token:          token.TokenValue,
		TargetDBPath:   eff.TargetDBPath,
		StartDate:      eff.StartDate,
		EndDate:        eff.EndDate,
		StartTime:      eff.StartTime,
		EndTime:        eff.EndTime,
		APIConfigs:     apiConfigs,
		MaxStocks:      0,
		CommonDataAPIs: ds.CommonDataAPIs,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute workflow: %w", err)
	}

	// Create sync execution record and update plan status using UoW
	var executionID shared.ID
	err = s.uow.Do(ctx, func(repos contracts.Repositories) error {
		// Verify plan exists in transaction (for foreign key constraint)
		txPlan, err := repos.SyncPlanRepo().Get(planID)
		if err != nil {
			return fmt.Errorf("failed to get plan in transaction: %w", err)
		}
		if txPlan == nil {
			return fmt.Errorf("plan not found in transaction: %s", planID)
		}

		// Create sync execution record (use effective params including target from data store)
		execution := sync.NewSyncExecution(planID, instanceID)
		execution.ExecuteParams = &sync.ExecuteParams{
			TargetDBPath: eff.TargetDBPath,
			StartDate:    eff.StartDate,
			EndDate:      eff.EndDate,
			StartTime:    eff.StartTime,
			EndTime:      eff.EndTime,
		}
		execution.SyncedAPIs = s.extractAPINames(needSyncTasks)
		execution.SkippedAPIs = skipAPIs
		execution.MarkRunning()

		if err := repos.SyncPlanRepo().AddPlanExecution(execution); err != nil {
			return fmt.Errorf("failed to create sync execution: %w", err)
		}

		// Mark plan as running (use the plan from transaction)
		txPlan.MarkRunning()
		if err := repos.SyncPlanRepo().Update(txPlan); err != nil {
			return fmt.Errorf("failed to update sync plan status: %w", err)
		}

		executionID = execution.ID
		return nil
	})

	if err != nil {
		return "", err
	}

	return executionID, nil
}

// supplementDependenciesFromStrategies supplements allAPIDependencies with data from api_sync_strategies.
// When api_metadata.param_dependencies is empty, we infer ParamDependency from the strategy's
// preferred_param and support_date_range to ensure the DependencyResolver assigns the correct SyncMode.
func (s *SyncApplicationServiceImpl) supplementDependenciesFromStrategies(
	ctx context.Context,
	dataSourceID shared.ID,
	allAPIs []*metadata.APIMetadata,
	allAPIDependencies map[string][]sync.ParamDependency,
) {
	if s.metadataRepo == nil {
		return
	}

	apiNames := make([]string, 0, len(allAPIs))
	for _, api := range allAPIs {
		apiNames = append(apiNames, api.Name)
	}

	strategies, err := s.metadataRepo.ListAPISyncStrategiesByAPINames(ctx, dataSourceID, apiNames)
	if err != nil {
		logrus.Warnf("[ResolveSyncPlan] Failed to query api_sync_strategies: %v", err)
		return
	}

	for _, strategy := range strategies {
		if len(allAPIDependencies[strategy.APIName]) > 0 {
			continue
		}
		deps := strategyToParamDependencies(strategy)
		if len(deps) > 0 {
			allAPIDependencies[strategy.APIName] = deps
			logrus.Infof("[ResolveSyncPlan] Supplemented param_dependencies for %s from api_sync_strategies (preferred_param=%s, support_date_range=%v)",
				strategy.APIName, strategy.PreferredParam, strategy.SupportDateRange)
		}
	}
}

// safeSQLIdentifier 仅允许 [a-zA-Z0-9_]，用于表名/列名防注入；合法则返回 true。
func safeSQLIdentifier(name string) bool {
	return regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString(name)
}

// syncQuoteIdentifier 双引号包裹并转义内部双引号（DuckDB 标识符），仅用于 getMaxDateFromTargetDB。
func syncQuoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}

// getMaxDateFromTargetDB 在目标 DuckDB 上执行 SELECT MAX(column) FROM table，返回日期字符串（20060102），失败或空表返回 ""。
func (s *SyncApplicationServiceImpl) getMaxDateFromTargetDB(ctx context.Context, targetDBPath, tableName, columnName string) string {
	if targetDBPath == "" || !safeSQLIdentifier(tableName) || !safeSQLIdentifier(columnName) {
		return ""
	}
	qdb, err := s.quantDBFactory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: targetDBPath,
	})
	if err != nil {
		logrus.Warnf("[getMaxDateFromTargetDB] Create QuantDB failed: %v", err)
		return ""
	}
	if err := qdb.Connect(ctx); err != nil {
		logrus.Warnf("[getMaxDateFromTargetDB] Connect failed: %v", err)
		_ = qdb.Close()
		return ""
	}
	defer func() { _ = qdb.Close() }()
	sql := fmt.Sprintf("SELECT MAX(%s) AS mx FROM %s", syncQuoteIdentifier(columnName), syncQuoteIdentifier(tableName))
	rows, err := qdb.Query(ctx, sql)
	if err != nil {
		logrus.Warnf("[getMaxDateFromTargetDB] Query failed: %v", err)
		return ""
	}
	if len(rows) == 0 {
		return ""
	}
	v, ok := rows[0]["mx"]
	if !ok || v == nil {
		return ""
	}
	// 归一化为 20060102：可能是 string "20060102"/"2006-01-02" 或 time.Time
	switch val := v.(type) {
	case string:
		val = strings.TrimSpace(val)
		if val == "" {
			return ""
		}
		// 2006-01-02 -> 20060102
		if strings.Contains(val, "-") {
			val = strings.ReplaceAll(val, "-", "")
		}
		if len(val) >= 8 {
			return val[:8]
		}
		return val
	case time.Time:
		return val.Format("20060102")
	case *time.Time:
		if val == nil {
			return ""
		}
		return val.Format("20060102")
	default:
		s := strings.TrimSpace(fmt.Sprintf("%v", v))
		if strings.Contains(s, "-") {
			s = strings.ReplaceAll(s, "-", "")
		}
		if len(s) >= 8 {
			return s[:8]
		}
		return s
	}
}

// planRequiresDateRange 根据计划内 API 的请求参数是否包含 date/time/dt 等模式，判断是否必须配置日期范围。
// 若计划仅同步如 stock_basic 等无日期参数的 API，返回 false，执行时不会强制要求 start_dt/end_dt。
func (s *SyncApplicationServiceImpl) planRequiresDateRange(plan *sync.SyncPlan) bool {
	apiNames := plan.ResolvedAPIs
	if len(apiNames) == 0 {
		apiNames = plan.SelectedAPIs
	}
	if len(apiNames) == 0 {
		return false
	}
	allAPIs, err := s.dataSourceRepo.ListAPIMetadataByDataSource(plan.DataSourceID)
	if err != nil || len(allAPIs) == 0 {
		return true // 无法获取元数据时保守要求日期，避免执行时报错
	}
	paramNamesByAPI := make(map[string][]string)
	for _, api := range allAPIs {
		names := make([]string, 0, len(api.RequestParams))
		for _, p := range api.RequestParams {
			names = append(names, p.Name)
		}
		paramNamesByAPI[api.Name] = names
	}
	return sync.PlanRequiresDateRange(apiNames, paramNamesByAPI)
}

// strategyToParamDependencies converts an APISyncStrategy to ParamDependency entries.
// Mapping rules:
//   - preferred_param=trade_date, !support_date_range → template mode (IsList=true), iterate over trade dates
//   - preferred_param=trade_date, support_date_range  → direct mode (IsList=false), pass date range
//   - preferred_param=ts_code                         → template mode (IsList=true), iterate over stock codes
//   - preferred_param=none                            → no dependencies needed
func strategyToParamDependencies(strategy *metadata.APISyncStrategy) []sync.ParamDependency {
	switch strategy.PreferredParam {
	case metadata.SyncParamTradeDate:
		return []sync.ParamDependency{{
			ParamName:   "trade_date",
			SourceAPI:   "trade_cal",
			SourceField: "cal_date",
			IsList:      !strategy.SupportDateRange,
		}}
	case metadata.SyncParamTsCode:
		return []sync.ParamDependency{{
			ParamName:   "ts_code",
			SourceAPI:   "stock_basic",
			SourceField: "ts_code",
			IsList:      true,
		}}
	default:
		return nil
	}
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
// 从 SyncTask.ParamMappings 填充 ParamKey、UpstreamTask、UpstreamParams，使工作流按计划依赖与参数执行。
func (s *SyncApplicationServiceImpl) convertToAPIConfigs(graph *sync.ExecutionGraph, tasks []*sync.SyncTask) []workflow.APISyncConfig {
	configs := make([]workflow.APISyncConfig, 0, len(tasks))

	for _, task := range tasks {
		config := workflow.APISyncConfig{
			APIName:  task.APIName,
			SyncMode: task.SyncMode.String(),
		}

		if len(task.Dependencies) > 0 {
			config.Dependencies = task.Dependencies
		}
		if len(task.Params) > 0 {
			config.ExtraParams = task.Params
		}

		// 从 ParamMappings 填充模板/直接任务的参数来源
		if len(task.ParamMappings) > 0 {
			first := task.ParamMappings[0]
			config.ParamKey = first.ParamName
			config.UpstreamTask = first.SourceTask
			if task.SyncMode == sync.TaskSyncModeDirect {
				// direct 模式：构建 upstream_params 供 SyncAPIData 的 resolveUpstreamParams 使用
				config.UpstreamParams = make(map[string]interface{})
				for _, m := range task.ParamMappings {
					extractedField := m.SourceField
					if extractedField == "cal_date" {
						extractedField = "cal_dates"
					}
					if extractedField == "ts_code" {
						extractedField = "ts_codes"
					}
					selectVal := m.Select
					if selectVal == "" {
						selectVal = "last"
					}
					config.UpstreamParams[m.ParamName] = map[string]interface{}{
						"task_name":       m.SourceTask,
						"extracted_field": extractedField,
						"select":          selectVal,
					}
				}
			}
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

// GetPlanSummary returns the latest execution summary for a sync plan (or nil if never executed).
func (s *SyncApplicationServiceImpl) GetPlanSummary(ctx context.Context, planID shared.ID) (*contracts.PlanSummary, error) {
	_, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync plan: %w", err)
	}
	execs, total, err := s.syncPlanRepo.GetExecutionsByPlanPaged(planID, 1, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest execution: %w", err)
	}
	if total == 0 || len(execs) == 0 {
		return nil, nil
	}
	latest := execs[0]
	return &contracts.PlanSummary{
		ExecutionID:  latest.ID,
		Status:       latest.Status,
		StartedAt:    latest.StartedAt,
		FinishedAt:   latest.FinishedAt,
		RecordCount:  latest.RecordCount,
		ErrorMessage: latest.ErrorMessage,
		SyncedAPIs:   latest.SyncedAPIs,
		SkippedAPIs:  latest.SkippedAPIs,
	}, nil
}

// ListPlanExecutionHistory returns paginated execution history for a sync plan.
func (s *SyncApplicationServiceImpl) ListPlanExecutionHistory(ctx context.Context, planID shared.ID, limit, offset int) ([]*sync.SyncExecution, int, error) {
	_, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get sync plan: %w", err)
	}
	return s.syncPlanRepo.GetExecutionsByPlanPaged(planID, limit, offset)
}

// CancelExecution cancels a running sync execution.
func (s *SyncApplicationServiceImpl) CancelExecution(ctx context.Context, executionID shared.ID) error {
	// Read operations (no transaction needed)
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

	// Tell Task Engine to terminate the workflow first
	if s.taskEngineAdapter != nil && !exec.WorkflowInstID.IsEmpty() {
		if err := s.taskEngineAdapter.CancelInstance(ctx, exec.WorkflowInstID.String()); err != nil {
			return fmt.Errorf("failed to cancel workflow: %w", err)
		}
	}

	// Get plan for status check
	plan, err := s.syncPlanRepo.Get(exec.SyncPlanID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}

	// Write operations using UoW for transaction
	return s.uow.Do(ctx, func(repos contracts.Repositories) error {
		// Update execution status
		exec.MarkCancelled()
		if err := repos.SyncPlanRepo().UpdatePlanExecution(exec); err != nil {
			return fmt.Errorf("failed to update execution status: %w", err)
		}

		// Update plan status if needed
		if plan != nil && plan.Status == sync.PlanStatusRunning {
			plan.MarkCompleted(nil)
			if err := repos.SyncPlanRepo().Update(plan); err != nil {
				return fmt.Errorf("failed to update sync plan status: %w", err)
			}
		}

		return nil
	})
}

// PauseExecution pauses a running sync execution by pausing the workflow instance in Task Engine.
func (s *SyncApplicationServiceImpl) PauseExecution(ctx context.Context, executionID shared.ID) error {
	exec, err := s.syncPlanRepo.GetPlanExecution(executionID)
	if err != nil {
		return fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}
	if exec.Status != sync.ExecStatusRunning && exec.Status != sync.ExecStatusPending {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "execution is not running", nil)
	}
	if exec.WorkflowInstID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "execution has no workflow instance", nil)
	}
	if s.taskEngineAdapter == nil {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "task engine not available", nil)
	}
	return s.taskEngineAdapter.PauseInstance(ctx, exec.WorkflowInstID.String())
}

// ResumeExecution resumes a paused sync execution.
func (s *SyncApplicationServiceImpl) ResumeExecution(ctx context.Context, executionID shared.ID) error {
	exec, err := s.syncPlanRepo.GetPlanExecution(executionID)
	if err != nil {
		return fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}
	if exec.Status != sync.ExecStatusRunning && exec.Status != sync.ExecStatusPending {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "execution is not running or paused", nil)
	}
	if exec.WorkflowInstID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "execution has no workflow instance", nil)
	}
	if s.taskEngineAdapter == nil {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "task engine not available", nil)
	}
	return s.taskEngineAdapter.ResumeInstance(ctx, exec.WorkflowInstID.String())
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

	// 非 disabled 即纳入 cron
	if plan.Status != sync.PlanStatusDisabled {
		nextRunTime, err := s.cronCalculator.CalculateNextRunTime(cronExpression, time.Now())
		if err != nil {
			return fmt.Errorf("failed to calculate next run time: %w", err)
		}
		plan.NextExecuteAt = nextRunTime
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

// reconcileScheduledPlans 以 DB 为准全量同步调度器：应被调度的全部注册，不应被调度的从调度器移除。
// 在 CreateSyncPlan / UpdateSyncPlan 持久化后调用；Disable/Delete 已单独 UnschedulePlan。
func (s *SyncApplicationServiceImpl) reconcileScheduledPlans() {
	if s.planScheduler == nil {
		return
	}
	plans, err := s.syncPlanRepo.GetSchedulablePlans()
	if err != nil {
		logrus.Warnf("[SyncPlan] reconcileScheduledPlans: get schedulable plans failed: %v", err)
		return
	}
	schedulableIDs := make(map[string]struct{})
	for _, p := range plans {
		if p.CronExpression != nil && *p.CronExpression != "" {
			id := p.ID.String()
			schedulableIDs[id] = struct{}{}
			if err := s.planScheduler.SchedulePlan(id, *p.CronExpression); err != nil {
				logrus.Warnf("[SyncPlan] reconcileScheduledPlans: schedule plan %s failed: %v", id, err)
				continue
			}
		}
	}
	for _, id := range s.planScheduler.GetScheduledPlanIDs() {
		if _, ok := schedulableIDs[id]; !ok {
			s.planScheduler.UnschedulePlan(id)
		}
	}
}

// ==================== Callback Handlers ====================

// HandleExecutionCallback handles execution result callback from workflow.
func (s *SyncApplicationServiceImpl) HandleExecutionCallback(ctx context.Context, req contracts.ExecutionCallbackRequest) error {
	// Read operations (no transaction needed)
	exec, err := s.syncPlanRepo.GetPlanExecution(req.ExecutionID)
	if err != nil {
		return fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}

	// Get plan for status update
	plan, err := s.syncPlanRepo.Get(exec.SyncPlanID)
	if err != nil {
		return fmt.Errorf("failed to get sync plan: %w", err)
	}

	// Get tasks for updating LastSyncedAt
	tasks, err := s.syncPlanRepo.GetTasksByPlan(exec.SyncPlanID)
	if err != nil {
		return fmt.Errorf("failed to get tasks: %w", err)
	}

	// 若工作流报告失败但明细表里所有任务都成功，以明细为准，避免「全部成功却显示失败」
	effectiveSuccess := req.Success
	effectiveRecordCount := req.RecordCount
	if !req.Success {
		details, derr := s.syncPlanRepo.GetExecutionDetailsByExecutionID(exec.ID)
		if derr == nil && len(details) > 0 {
			allSuccess := true
			var sum int64
			for _, d := range details {
				if d.Status != "success" {
					allSuccess = false
					break
				}
				sum += d.RecordCount
			}
			if allSuccess {
				effectiveSuccess = true
				effectiveRecordCount = sum
				logrus.Infof("[HandleExecutionCallback] workflow reported failed but all %d detail rows are success, overriding to success (record_count=%d)", len(details), sum)
			}
		}
	}

	// Write operations using UoW for transaction
	return s.uow.Do(ctx, func(repos contracts.Repositories) error {
		// Update execution status
		if effectiveSuccess {
			// 纠正为成功时保留工作流原始错误信息，供前端展示警告、排查引擎问题
			if !req.Success && req.ErrorMessage != nil {
				exec.WorkflowErrorMessage = req.ErrorMessage
			}
			exec.MarkSuccess(effectiveRecordCount)

			// Update LastSyncedAt for synced tasks
			for _, task := range tasks {
				for _, syncedAPI := range exec.SyncedAPIs {
					if task.APIName == syncedAPI {
						task.MarkSynced()
						if err := repos.SyncPlanRepo().UpdateTask(task); err != nil {
							return fmt.Errorf("failed to update task: %w", err)
						}
						break
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

		if err := repos.SyncPlanRepo().UpdatePlanExecution(exec); err != nil {
			return fmt.Errorf("failed to update execution: %w", err)
		}

		// Update plan status
		if plan != nil {
			if effectiveSuccess && plan.IncrementalMode && exec.ExecuteParams != nil && exec.ExecuteParams.EndDate != "" {
				plan.SetLastSuccessfulEndDate(exec.ExecuteParams.EndDate)
			}
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
			if err := repos.SyncPlanRepo().Update(plan); err != nil {
				return fmt.Errorf("failed to update sync plan: %w", err)
			}
		}

		return nil
	})
}

// HandleExecutionCallbackByWorkflowInstance looks up execution by workflow instance ID, then invokes HandleExecutionCallback.
func (s *SyncApplicationServiceImpl) HandleExecutionCallbackByWorkflowInstance(ctx context.Context, workflowInstID string, success bool, recordCount int64, errMsg *string) error {
	exec, err := s.syncPlanRepo.GetExecutionByWorkflowInstID(workflowInstID)
	if err != nil {
		return fmt.Errorf("failed to get sync execution by workflow inst id: %w", err)
	}
	if exec == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found for workflow "+workflowInstID, nil)
	}

	// 若回调未携带 recordCount（当前 task-engine handler 传 0），则从明细表汇总一份，确保概览不为 0。
	if recordCount == 0 {
		if rows, derr := s.syncPlanRepo.GetExecutionDetailsByExecutionID(exec.ID); derr == nil && len(rows) > 0 {
			var sum int64
			for _, r := range rows {
				if r != nil && r.Status == "success" {
					sum += r.RecordCount
				}
			}
			recordCount = sum
		}
	}
	return s.HandleExecutionCallback(ctx, contracts.ExecutionCallbackRequest{
		ExecutionID:  exec.ID,
		Success:      success,
		RecordCount:  recordCount,
		ErrorMessage: errMsg,
	})
}

// ==================== Progress Query ====================

// GetExecutionProgress retrieves aggregated progress for a specific sync execution.
func (s *SyncApplicationServiceImpl) GetExecutionProgress(ctx context.Context, executionID shared.ID) (*contracts.SyncExecutionProgress, error) {
	exec, err := s.syncPlanRepo.GetPlanExecution(executionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync execution: %w", err)
	}
	if exec == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}

	var wfStatus *workflow.WorkflowStatus
	if s.taskEngineAdapter != nil && !exec.WorkflowInstID.IsEmpty() {
		wfStatus, err = s.taskEngineAdapter.GetInstanceStatus(ctx, exec.WorkflowInstID.String())
		if err != nil && !shared.IsNotFoundError(err) {
			// 非致命错误：交给调用方决定是否容忍
			return nil, fmt.Errorf("failed to get workflow status: %w", err)
		}
		// Debug: Log workflow status details
		if wfStatus != nil && wfStatus.TaskCount == 0 {
			logrus.Warnf("[DEBUG] GetExecutionProgress: executionID=%s, workflowInstID=%s, taskCount=0, progress=%.2f%%, status=%s",
				executionID, exec.WorkflowInstID, wfStatus.Progress, wfStatus.Status)
		}
	} else {
		// Debug: Log why we skipped getting workflow status
		logrus.Warnf("[DEBUG] GetExecutionProgress: skipped workflow status - adapter=%v, workflowInstID=%s, isEmpty=%v",
			s.taskEngineAdapter != nil, exec.WorkflowInstID, exec.WorkflowInstID.IsEmpty())
	}

	progress := &contracts.SyncExecutionProgress{
		ExecutionID:        exec.ID,
		PlanID:             exec.SyncPlanID,
		WorkflowInstanceID: exec.WorkflowInstID,
		Status:             exec.Status,
		RecordCount:        exec.RecordCount,
		ErrorMessage:       exec.ErrorMessage,
		StartedAt:          exec.StartedAt,
		FinishedAt:         exec.FinishedAt,
	}

	// Merge workflow status if available
	if wfStatus != nil {
		progress.Progress = wfStatus.Progress
		progress.TaskCount = wfStatus.TaskCount
		progress.CompletedTask = wfStatus.CompletedTask
		progress.FailedTask = wfStatus.FailedTask
		progress.RunningCount = wfStatus.RunningCount
		progress.PendingCount = wfStatus.PendingCount
		progress.RunningTaskIDs = wfStatus.RunningTaskIDs
		progress.PendingTaskIDs = wfStatus.PendingTaskIDs
		// When task engine returns 0 task count (e.g. dynamic workflow), use execution's SyncedAPIs as expected count
		if progress.TaskCount == 0 && len(exec.SyncedAPIs) > 0 {
			progress.TaskCount = len(exec.SyncedAPIs)
		}
		// 引擎暂停时在进度中体现，便于前端显示「暂停」和「恢复」按钮
		if strings.ToUpper(wfStatus.Status) == "PAUSED" {
			progress.Status = sync.ExecStatusPaused
		}

		// Prefer workflow FinishedAt if execution.FinishedAt is nil
		if progress.FinishedAt == nil && wfStatus.FinishedAt != nil {
			progress.FinishedAt = wfStatus.FinishedAt
		}

		// If execution error is empty, but workflow has error, use it
		if (progress.ErrorMessage == nil || *progress.ErrorMessage == "") && wfStatus.ErrorMessage != nil {
			progress.ErrorMessage = wfStatus.ErrorMessage
		}

		// Sync workflow terminal status to SyncExecution if not already terminal
		// This ensures SyncExecution.Status reflects workflow completion
		if exec.Status == sync.ExecStatusRunning || exec.Status == sync.ExecStatusPending {
			var newStatus sync.ExecStatus
			statusUpper := strings.ToUpper(wfStatus.Status)
			switch statusUpper {
			case "SUCCESS", "COMPLETED":
				newStatus = sync.ExecStatusSuccess
			case "FAILED", "ERROR":
				newStatus = sync.ExecStatusFailed
			case "TERMINATED", "CANCELLED":
				newStatus = sync.ExecStatusCancelled
			}

			// 若工作流报告 Failed 但明细表里全部任务成功，以明细为准，改为 Success
			if newStatus == sync.ExecStatusFailed {
				details, derr := s.syncPlanRepo.GetExecutionDetailsByExecutionID(executionID)
				if derr == nil && len(details) > 0 {
					allSuccess := true
					var sum int64
					for _, d := range details {
						if d.Status != "success" {
							allSuccess = false
							break
						}
						sum += d.RecordCount
					}
					if allSuccess {
						newStatus = sync.ExecStatusSuccess
						if wfStatus.ErrorMessage != nil {
							exec.WorkflowErrorMessage = wfStatus.ErrorMessage
						}
						logrus.Infof("[SyncExecution] workflow reported failed but all %d detail rows are success, overriding to success (executionID=%s)", len(details), executionID)
					}
				}
			}

			if newStatus != "" && newStatus != exec.Status {
				logrus.Infof("[SyncExecution] Auto-syncing status from workflow: executionID=%s, oldStatus=%s, newStatus=%s, workflowStatus=%s",
					executionID, exec.Status, newStatus, wfStatus.Status)

				// Update execution entity
				switch newStatus {
				case sync.ExecStatusSuccess:
					recordCount := exec.RecordCount
					if recordCount == 0 {
						details, _ := s.syncPlanRepo.GetExecutionDetailsByExecutionID(executionID)
						for _, d := range details {
							recordCount += d.RecordCount
						}
					}
					exec.MarkSuccess(recordCount)
				case sync.ExecStatusFailed:
					errMsg := ""
					if wfStatus.ErrorMessage != nil {
						errMsg = *wfStatus.ErrorMessage
					}
					exec.MarkFailed(errMsg)
				case sync.ExecStatusCancelled:
					exec.MarkCancelled()
				}

				// Persist to database
				if updateErr := s.syncPlanRepo.UpdatePlanExecution(exec); updateErr != nil {
					logrus.Warnf("[SyncExecution] Failed to auto-sync status: %v", updateErr)
				} else {
					// Update progress response with new status
					progress.Status = newStatus
					progress.FinishedAt = exec.FinishedAt
					if newStatus == sync.ExecStatusFailed && exec.ErrorMessage != nil {
						progress.ErrorMessage = exec.ErrorMessage
					}
					// When execution is synced to terminal from workflow, also mark plan completed
					// so plan status does not stay "running" if DataSyncCompleteHandler was never invoked
					plan, planErr := s.syncPlanRepo.Get(exec.SyncPlanID)
					if planErr == nil && plan != nil && plan.Status == sync.PlanStatusRunning {
						var nextRunAt *time.Time
						if plan.CronExpression != nil && *plan.CronExpression != "" {
							if s.planScheduler != nil {
								nextRunAt = s.planScheduler.GetNextRunTime(exec.SyncPlanID.String())
							}
							if nextRunAt == nil {
								nextRunAt, _ = s.cronCalculator.CalculateNextRunTime(*plan.CronExpression, time.Now())
							}
						}
						plan.MarkCompleted(nextRunAt)
						if updatePlanErr := s.syncPlanRepo.Update(plan); updatePlanErr != nil {
							logrus.Warnf("[SyncExecution] Failed to mark plan completed after auto-sync: %v", updatePlanErr)
						} else {
							logrus.Infof("[SyncExecution] Plan %s marked completed after auto-sync from workflow", exec.SyncPlanID)
						}
					}
				}
			}
		}
	}

	return progress, nil
}

// GetPlanProgress retrieves aggregated progress for the latest execution of a sync plan.
func (s *SyncApplicationServiceImpl) GetPlanProgress(ctx context.Context, planID shared.ID) (*contracts.SyncExecutionProgress, error) {
	execs, err := s.syncPlanRepo.GetExecutionsByPlan(planID)
	if err != nil {
		return nil, fmt.Errorf("failed to list sync executions for plan: %w", err)
	}

	if len(execs) == 0 {
		// Plan has never been executed - return a pending progress
		return &contracts.SyncExecutionProgress{
			PlanID: planID,
			Status: sync.ExecStatusPending,
		}, nil
	}

	// DAO 已按 started_at DESC 排序，第一条即为最新执行
	latest := execs[0]
	return s.GetExecutionProgress(ctx, latest.ID)
}

// RecordTaskResult 记录单次同步任务结果，供 DataSyncSuccess/DataSyncFailure Handler 调用。
func (s *SyncApplicationServiceImpl) RecordTaskResult(ctx context.Context, workflowInstID, apiName, taskID string, recordCount int64, success bool, errorMessage string) error {
	exec, err := s.syncPlanRepo.GetExecutionByWorkflowInstID(workflowInstID)
	if err != nil {
		return fmt.Errorf("get execution by workflow inst id: %w", err)
	}
	if exec == nil {
		return nil // 找不到执行记录时静默跳过，避免 handler 报错影响引擎
	}
	status := "success"
	if !success {
		status = "failed"
	}
	detail := &sync.SyncExecutionDetail{
		ID:           shared.NewID(),
		ExecutionID:  exec.ID,
		TaskID:       taskID,
		APIName:      apiName,
		RecordCount:  recordCount,
		Status:       status,
		FinishedAt:   ptrTimestamp(shared.Now()),
	}
	if errorMessage != "" {
		detail.ErrorMessage = &errorMessage
	}
	return s.syncPlanRepo.AddExecutionDetail(detail)
}

func ptrTimestamp(t shared.Timestamp) *shared.Timestamp { return &t }

// GetExecutionDetail 返回某次执行的统计与明细。
func (s *SyncApplicationServiceImpl) GetExecutionDetail(ctx context.Context, executionID shared.ID) (*contracts.ExecutionDetail, error) {
	exec, err := s.syncPlanRepo.GetPlanExecution(executionID)
	if err != nil {
		return nil, fmt.Errorf("get sync execution: %w", err)
	}
	if exec == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync execution not found", nil)
	}
	rows, err := s.syncPlanRepo.GetExecutionDetailsByExecutionID(executionID)
	if err != nil {
		return nil, fmt.Errorf("get execution details: %w", err)
	}
	totalTasks := len(rows)
	var successCount, failedCount int
	var totalRows int64
	for _, r := range rows {
		totalRows += r.RecordCount
		if r.Status == "success" {
			successCount++
		} else {
			failedCount++
		}
	}
	errorRate := 0.0
	if totalTasks > 0 {
		errorRate = float64(failedCount) / float64(totalTasks)
	}
	// 按 API 聚合
	apiMap := make(map[string]*contracts.ApiSyncStat)
	for _, r := range rows {
		st, ok := apiMap[r.APIName]
		if !ok {
			st = &contracts.ApiSyncStat{APIName: r.APIName}
			apiMap[r.APIName] = st
		}
		st.TaskCount++
		st.TotalRows += r.RecordCount
		if r.Status == "success" {
			st.SuccessCount++
		} else {
			st.FailedCount++
			if r.ErrorMessage != nil && *r.ErrorMessage != "" {
				st.ErrorMessages = append(st.ErrorMessages, *r.ErrorMessage)
			}
		}
	}
	apiStats := make([]contracts.ApiSyncStat, 0, len(apiMap))
	for _, st := range apiMap {
		if st.TaskCount > 0 {
			st.ErrorRate = float64(st.FailedCount) / float64(st.TaskCount)
		}
		apiStats = append(apiStats, *st)
	}
	out := &contracts.ExecutionDetail{
		ExecutionID:          exec.ID,
		PlanID:               exec.SyncPlanID,
		Status:               exec.Status,
		RecordCount:          totalRows,
		ErrorMessage:         exec.ErrorMessage,
		WorkflowErrorMessage: exec.WorkflowErrorMessage,
		StartedAt:            exec.StartedAt,
		FinishedAt:           exec.FinishedAt,
		TotalTasks:           totalTasks,
		SuccessCount:         successCount,
		FailedCount:          failedCount,
		ErrorRate:            errorRate,
		ApiStats:             apiStats,
		DetailRows:   rows,
	}
	return out, nil
}

// ==================== Built-in Workflow Execution (Legacy) ====================
