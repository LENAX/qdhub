// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"regexp"
	"sync/atomic"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/internal/infrastructure/taskengine/jobs"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

const planIDRealtimeNews = "realtime-news"
const newsForceBackfillCheckInterval = 10 * time.Minute

var lastNewsForcedBackfillCheckUnix int64

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

	// 可选：取消实时执行时刷满批 tick 写队列尾批
	quantDBWriteQueue datastore.QuantDBWriteQueue

	// 多实时数据源：production 时双 workflow，仅当前选中源写 Store；失败时切换
	realtimeEnv             string
	realtimeSourceSelector  *realtimestore.RealtimeSourceSelector
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
	quantDBWriteQueue datastore.QuantDBWriteQueue,
	realtimeEnv string,
	realtimeSourceSelector *realtimestore.RealtimeSourceSelector,
) contracts.SyncApplicationService {
	if realtimeEnv == "" {
		realtimeEnv = "development"
	}
	return &SyncApplicationServiceImpl{
		syncPlanRepo:            syncPlanRepo,
		cronCalculator:          cronCalculator,
		planScheduler:           planScheduler,
		dataSourceRepo:          dataSourceRepo,
		dataStoreRepo:           dataStoreRepo,
		workflowExecutor:        workflowExecutor,
		dependencyResolver:      dependencyResolver,
		taskEngineAdapter:       taskEngineAdapter,
		uow:                     uow,
		metadataRepo:            metadataRepo,
		quantDBFactory:          quantDBFactory,
		quantDBWriteQueue:       quantDBWriteQueue,
		realtimeEnv:             realtimeEnv,
		realtimeSourceSelector:  realtimeSourceSelector,
	}
}

// ==================== Realtime multi-source helpers ====================

// execSyncedAPIToSource 从 SyncedAPIs 推断数据源：ts_realtime_mkt_tick -> tushare_ws，realtime_quote -> sina。
func execSyncedAPIToSource(syncedAPIs []string) string {
	for _, api := range syncedAPIs {
		if api == "ts_realtime_mkt_tick" {
			return realtimestore.SourceTushareWS
		}
		if api == "realtime_quote" {
			return realtimestore.SourceSina
		}
	}
	return ""
}

// execCorrespondsToCurrentActive 判断该执行是否对应当前选中的实时数据源（生产+实时且 exec 的 API 对应 current source）。
func (s *SyncApplicationServiceImpl) execCorrespondsToCurrentActive(plan *sync.SyncPlan, exec *sync.SyncExecution) bool {
	if plan == nil || exec == nil || s.realtimeSourceSelector == nil {
		return false
	}
	if plan.Mode != sync.PlanModeRealtime || s.realtimeEnv != "production" {
		return false
	}
	src := execSyncedAPIToSource(exec.SyncedAPIs)
	if src == "" {
		return false
	}
	return s.realtimeSourceSelector.CurrentSource() == src
}

// otherRealtimeSource 返回另一实时源（用于故障切换）。
func otherRealtimeSource(source string) string {
	if realtimestore.SinaRealtimeDisabled && realtimestore.TushareWSRealtimeDisabled {
		return ""
	}
	if realtimestore.SinaRealtimeDisabled {
		if source == realtimestore.SourceTushareProxy && !realtimestore.TushareWSRealtimeDisabled {
			return realtimestore.SourceTushareWS
		}
		if source == realtimestore.SourceTushareWS {
			return realtimestore.SourceTushareProxy
		}
		return ""
	}
	if realtimestore.TushareWSRealtimeDisabled {
		if source == realtimestore.SourceTushareWS {
			return ""
		}
		if source == realtimestore.SourceSina {
			return realtimestore.SourceTushareProxy
		}
		if source == realtimestore.SourceTushareProxy {
			return realtimestore.SourceSina
		}
		return ""
	}
	if source == realtimestore.SourceTushareWS {
		return realtimestore.SourceSina
	}
	if source == realtimestore.SourceSina {
		return realtimestore.SourceTushareWS
	}
	return ""
}

func containsString(slice []string, s string) bool {
	for _, x := range slice {
		if x == s {
			return true
		}
	}
	return false
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

	// PlanMode：默认 batch，realtime 时校验 SelectedAPIs 必须在白名单内
	mode := req.PlanMode
	if !mode.IsValid() {
		mode = sync.PlanModeBatch
	}
	if mode == sync.PlanModeRealtime {
		if err := sync.ValidateRealtimePlanAPIs(req.SelectedAPIs); err != nil {
			return nil, shared.NewDomainError(shared.ErrCodeValidation, err.Error(), nil)
		}
	}

	// Create domain entity
	plan := sync.NewSyncPlan(
		req.Name,
		req.Description,
		req.DataSourceID,
		req.SelectedAPIs,
	)
	plan.Mode = mode

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
	plan.SetScheduleWindow(req.ScheduleStartCron, req.ScheduleEndCron)
	plan.SetSchedulePauseWindow(req.SchedulePauseStartCron, req.SchedulePauseEndCron)
	plan.SetPullIntervalSeconds(req.PullIntervalSeconds)

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
	if req.PlanMode != nil {
		mode := *req.PlanMode
		if !mode.IsValid() {
			mode = sync.PlanModeBatch
		}
		plan.Mode = mode
	}
	if req.ScheduleStartCron != nil || req.ScheduleEndCron != nil {
		start, end := "", ""
		if req.ScheduleStartCron != nil {
			start = *req.ScheduleStartCron
		}
		if req.ScheduleEndCron != nil {
			end = *req.ScheduleEndCron
		}
		plan.SetScheduleWindow(start, end)
	}
	if req.SchedulePauseStartCron != nil || req.SchedulePauseEndCron != nil {
		pauseStart, pauseEnd := "", ""
		if req.SchedulePauseStartCron != nil {
			pauseStart = *req.SchedulePauseStartCron
		}
		if req.SchedulePauseEndCron != nil {
			pauseEnd = *req.SchedulePauseEndCron
		}
		plan.SetSchedulePauseWindow(pauseStart, pauseEnd)
	}
	if req.PullIntervalSeconds != nil {
		plan.SetPullIntervalSeconds(*req.PullIntervalSeconds)
	}

	// 实时计划约束：仅能绑定一个实时 API，不能混入批量/历史 API
	if plan.Mode == sync.PlanModeRealtime {
		if err := sync.ValidateRealtimePlanAPIs(plan.SelectedAPIs); err != nil {
			return shared.NewDomainError(shared.ErrCodeValidation, err.Error(), nil)
		}
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

	var newExecStatus sync.ExecStatus
	if workflow.IsSuccess(wfStatus.Status) {
		newExecStatus = sync.ExecStatusSuccess
	} else if workflow.IsFailed(wfStatus.Status) {
		newExecStatus = sync.ExecStatusFailed
	} else if workflow.IsTerminated(wfStatus.Status) {
		newExecStatus = sync.ExecStatusCancelled
	} else {
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
	var dualRealtimeSecondInstanceID shared.ID // 生产双实时 workflow 时由 realtime 分支赋值
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
	// SyncPlan 增量模式：不依赖 sync_checkpoint 表。由用户配置「某表 + 某日期列」，
	// 执行时取 min(上次成功结束日 LastSuccessfulEndDate, 表中 MAX(日期列)) 作为起始日，结束日为今天。
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
	// 日期范围校验仅针对批量模式：实时计划不做日期校验
	if plan.Mode != sync.PlanModeRealtime && requiresDate && (eff.StartDate == "" || eff.EndDate == "") {
		return "", shared.NewDomainError(shared.ErrCodeValidation,
			"missing date range: set default_execute_params on the plan or pass start_dt, end_dt", nil)
	}

	isNewsRealtime := string(plan.ID) == planIDRealtimeNews

	tasks, err := s.syncPlanRepo.GetTasksByPlan(planID)
	if err != nil {
		return "", fmt.Errorf("failed to get tasks: %w", err)
	}
	if len(tasks) == 0 && !isNewsRealtime {
		return "", shared.NewDomainError(shared.ErrCodeInvalidState, "no tasks found for plan", nil)
	}

	var needSyncTasks []*sync.SyncTask
	var skipAPIs []string
	if !isNewsRealtime {
		needSyncTasks, skipAPIs = s.filterTasksByFrequency(tasks)
		if len(needSyncTasks) == 0 {
			return "", shared.NewDomainError(shared.ErrCodeInvalidState, "all tasks are skipped due to sync frequency", nil)
		}
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

	var instanceID shared.ID
	if isNewsRealtime {
		params := map[string]interface{}{
			"data_source_name": ds.Name,
			"token":            token.TokenValue,
			"target_db_path":   targetDBPath,
		}
		id, err := s.workflowExecutor.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameNewsRealtimeSync, params)
		if err != nil {
			return "", fmt.Errorf("failed to execute news realtime sync workflow: %w", err)
		}
		instanceID = id
	} else if plan.Mode == sync.PlanModeRealtime {
		// 实时同步：不校验、不依赖日期范围；实时 API 通常即最新数据，时间范围仅用于补充历史
		apiNames := s.extractAPINames(needSyncTasks)
		tsCodes, indexCodes := s.loadTsCodesFromTargetDB(ctx, eff.TargetDBPath)
		pullSecs := plan.PullIntervalSeconds
		if pullSecs <= 0 {
			pullSecs = 60
		}
		// 从 api_sync_strategies 加载各 API 的 fixed_params，供流式工作流使用（如 ts_realtime_mkt_tick 的 topic/codes）
		var fixedParamsByAPI map[string]map[string]interface{}
		if strategies, listErr := s.metadataRepo.ListAPISyncStrategiesByAPINames(ctx, plan.DataSourceID, apiNames); listErr == nil && len(strategies) > 0 {
			fixedParamsByAPI = make(map[string]map[string]interface{}, len(strategies))
			for _, st := range strategies {
				if st.FixedParams != nil && len(st.FixedParams) > 0 {
					m := make(map[string]interface{}, len(st.FixedParams))
					for k, v := range st.FixedParams {
						m[k] = v
					}
					fixedParamsByAPI[st.APIName] = m
				}
			}
		}
		baseReq := workflow.RealtimeDataSyncRequest{
			DataSourceName:   ds.Name,
			Token:            token.TokenValue,
			TargetDBPath:     eff.TargetDBPath,
			APINames:         apiNames,
			DataSourceID:     plan.DataSourceID,
			TsCodes:          tsCodes,
			IndexCodes:       indexCodes,
			PullIntervalSecs: pullSecs,
			FixedParamsByAPI: fixedParamsByAPI,
		}
		// 生产环境且计划含 ts_realtime_mkt_tick：仅启动 ts_proxy/WS tick；新浪并行 workflow 由 SinaRealtimeDisabled 关闭。
		if s.realtimeEnv == "production" && containsString(apiNames, "ts_realtime_mkt_tick") {
			reqWS := baseReq
			reqWS.APINames = []string{"ts_realtime_mkt_tick"}
			id1, err1 := s.workflowExecutor.ExecuteRealtimeDataSync(ctx, reqWS)
			if err1 != nil {
				return "", fmt.Errorf("failed to execute realtime WS workflow: %w", err1)
			}
			instanceID = id1
			if !realtimestore.SinaRealtimeDisabled {
				reqSina := baseReq
				reqSina.APINames = []string{"realtime_quote"}
				id2, err2 := s.workflowExecutor.ExecuteRealtimeDataSync(ctx, reqSina)
				if err2 != nil {
					return "", fmt.Errorf("failed to execute realtime Sina workflow: %w", err2)
				}
				dualRealtimeSecondInstanceID = id2
			}
		} else {
			var err error
			instanceID, err = s.workflowExecutor.ExecuteRealtimeDataSync(ctx, baseReq)
			if err != nil {
				return "", fmt.Errorf("failed to execute realtime workflow: %w", err)
			}
		}
	} else {
		// 批量工作流：用当前 DB 策略重新解析执行图，避免 api_sync_strategies 已改（如 moneyflow 改为 trade_date）后仍用旧图导致传 ts_code
		freshGraph, _, resolveErr := s.resolveExecutionGraphForPlan(ctx, plan)
		if resolveErr != nil {
			return "", fmt.Errorf("resolve execution graph for run: %w", resolveErr)
		}
		apiConfigs := s.convertToAPIConfigsFromGraph(freshGraph, needSyncTasks)
		instanceID, err = s.workflowExecutor.ExecuteBatchDataSync(ctx, workflow.BatchDataSyncRequest{
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
	}

	// Create sync execution record(s) and update plan status using UoW
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

		baseParams := &sync.ExecuteParams{
			TargetDBPath: eff.TargetDBPath,
			StartDate:    eff.StartDate,
			EndDate:      eff.EndDate,
			StartTime:    eff.StartTime,
			EndTime:      eff.EndTime,
		}
		if !dualRealtimeSecondInstanceID.IsEmpty() {
			// 双实时 workflow：创建两个 execution
			exec1 := sync.NewSyncExecution(planID, instanceID)
			exec1.ExecuteParams = baseParams
			exec1.SyncedAPIs = []string{"ts_realtime_mkt_tick"}
			exec1.SkippedAPIs = skipAPIs
			exec1.MarkRunning()
			if err := repos.SyncPlanRepo().AddPlanExecution(exec1); err != nil {
				return fmt.Errorf("failed to create sync execution (WS): %w", err)
			}
			exec2 := sync.NewSyncExecution(planID, dualRealtimeSecondInstanceID)
			exec2.ExecuteParams = baseParams
			exec2.SyncedAPIs = []string{"realtime_quote"}
			exec2.SkippedAPIs = skipAPIs
			exec2.MarkRunning()
			if err := repos.SyncPlanRepo().AddPlanExecution(exec2); err != nil {
				return fmt.Errorf("failed to create sync execution (Sina): %w", err)
			}
			executionID = exec1.ID
		} else {
			// 单 execution
			execution := sync.NewSyncExecution(planID, instanceID)
			execution.ExecuteParams = baseParams
			execution.SyncedAPIs = s.extractAPINames(needSyncTasks)
			execution.SkippedAPIs = skipAPIs
			execution.MarkRunning()
			if err := repos.SyncPlanRepo().AddPlanExecution(execution); err != nil {
				return fmt.Errorf("failed to create sync execution: %w", err)
			}
			executionID = execution.ID
		}

		// Mark plan as running (use the plan from transaction)
		txPlan.MarkRunning()
		if err := repos.SyncPlanRepo().Update(txPlan); err != nil {
			return fmt.Errorf("failed to update sync plan status: %w", err)
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	return executionID, nil
}

// resolveExecutionGraphForPlan 用当前 DB 中的 api_sync_strategies 与 api_metadata 重新解析执行图，不落库。
// 用于执行时拿到最新策略（如 moneyflow 已改为 trade_date），避免使用计划中缓存的旧 ExecutionGraph。
func (s *SyncApplicationServiceImpl) resolveExecutionGraphForPlan(ctx context.Context, plan *sync.SyncPlan) (*sync.ExecutionGraph, []string, error) {
	allAPIs, err := s.dataSourceRepo.ListAPIMetadataByDataSource(plan.DataSourceID)
	if err != nil {
		return nil, nil, fmt.Errorf("list API metadata: %w", err)
	}
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
	s.supplementDependenciesFromStrategies(ctx, plan.DataSourceID, allAPIs, allAPIDependencies)
	return s.dependencyResolver.Resolve(plan.SelectedAPIs, allAPIDependencies)
}

// supplementDependenciesFromStrategies 用 api_sync_strategies 的 preferred_param/support_date_range
// 覆盖或补充 allAPIDependencies，使同步计划按用户配置的策略执行（如 moneyflow 改为 trade_date 后按日期同步）。
// 若某 API 在 api_sync_strategies 中有配置且 preferred_param 非 none，则以其为准，覆盖 api_metadata.param_dependencies。
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
		deps := strategyToParamDependencies(strategy.APIName, strategy)
		if len(deps) > 0 {
			allAPIDependencies[strategy.APIName] = deps
			logrus.Infof("[ResolveSyncPlan] param_dependencies for %s from api_sync_strategies (preferred_param=%s, support_date_range=%v)",
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

// loadTsCodesFromTargetDB 从目标 DuckDB 读取 stock_basic.ts_code 与 index_basic.ts_code，供实时工作流分片用。
// 若 quantDBFactory 为空或表不存在则返回 nil；调用方不依赖非空列表也可运行（仅无 ts_code 的 API 会生成任务）。
func (s *SyncApplicationServiceImpl) loadTsCodesFromTargetDB(ctx context.Context, targetDBPath string) (tsCodes, indexCodes []string) {
	if targetDBPath == "" || s.quantDBFactory == nil {
		return nil, nil
	}
	qdb, err := s.quantDBFactory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: targetDBPath,
	})
	if err != nil {
		logrus.Warnf("[loadTsCodesFromTargetDB] Create QuantDB failed: %v", err)
		return nil, nil
	}
	if err := qdb.Connect(ctx); err != nil {
		logrus.Warnf("[loadTsCodesFromTargetDB] Connect failed: %v", err)
		_ = qdb.Close()
		return nil, nil
	}
	defer func() { _ = qdb.Close() }()

	if ok, _ := qdb.TableExists(ctx, "stock_basic"); ok {
		rows, err := qdb.Query(ctx, `SELECT ts_code FROM stock_basic`)
		if err == nil {
			for _, row := range rows {
				if v, ok := row["ts_code"]; ok && v != nil {
					if str, ok := v.(string); ok && str != "" {
						tsCodes = append(tsCodes, strings.TrimSpace(str))
					}
				}
			}
		}
	}
	if ok, _ := qdb.TableExists(ctx, "index_basic"); ok {
		rows, err := qdb.Query(ctx, `SELECT ts_code FROM index_basic`)
		if err == nil {
			for _, row := range rows {
				if v, ok := row["ts_code"]; ok && v != nil {
					if str, ok := v.(string); ok && str != "" {
						indexCodes = append(indexCodes, strings.TrimSpace(str))
					}
				}
			}
		}
	}
	return tsCodes, indexCodes
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
//   - preferred_param=ts_code                         → template mode (IsList=true)；股票来自 stock_basic，指数类 index_* 来自 index_basic
//   - preferred_param=index_code                      → 同 index_basic.ts_code，参数名为 index_code（如 index_weight）
//   - preferred_param=none                            → no dependencies needed
func strategyToParamDependencies(apiName string, strategy *metadata.APISyncStrategy) []sync.ParamDependency {
	switch strategy.PreferredParam {
	case metadata.SyncParamTradeDate:
		return []sync.ParamDependency{{
			ParamName:   "trade_date",
			SourceAPI:   "trade_cal",
			SourceField: "cal_date",
			IsList:      !strategy.SupportDateRange,
		}}
	case metadata.SyncParamTsCode:
		sourceAPI := "stock_basic"
		if strings.HasPrefix(apiName, "index_") {
			sourceAPI = "index_basic"
		}
		paramName := "ts_code"
		// Tushare index_weight 请求参数为 index_code，列表仍来自 index_basic.ts_code（与 batch_data_sync APIParamName 一致）
		if apiName == "index_weight" {
			paramName = "index_code"
		}
		return []sync.ParamDependency{{
			ParamName:   paramName,
			SourceAPI:   sourceAPI,
			SourceField: "ts_code",
			IsList:      true,
		}}
	case metadata.SyncParamIndexCode:
		return []sync.ParamDependency{{
			ParamName:   "index_code",
			SourceAPI:   "index_basic",
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
	return s.convertToAPIConfigsFromGraph(graph, tasks)
}

// convertToAPIConfigsFromGraph 从 ExecutionGraph.TaskConfigs 为给定 tasks 生成 API 配置。
// 使用 graph 中的 TaskConfigs（可来自当前 DB 策略的重新解析），保证执行时使用最新 preferred_param/依赖。
func (s *SyncApplicationServiceImpl) convertToAPIConfigsFromGraph(graph *sync.ExecutionGraph, tasks []*sync.SyncTask) []workflow.APISyncConfig {
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

		// 优先从 graph.TaskConfigs 取参数映射（反映当前 DB 策略），若无则回退到 task.ParamMappings
		var paramMappings []sync.ParamMapping
		if graph != nil && graph.TaskConfigs != nil {
			if tc := graph.TaskConfigs[task.APIName]; tc != nil {
				paramMappings = tc.ParamMappings
				config.SyncMode = tc.SyncMode.String()
				if len(tc.Dependencies) > 0 {
					config.Dependencies = tc.Dependencies
				}
			}
		}
		if len(paramMappings) == 0 {
			paramMappings = task.ParamMappings
		}

		if len(paramMappings) > 0 {
			first := paramMappings[0]
			config.ParamKey = first.ParamName
			config.UpstreamTask = first.SourceTask
			mode := sync.TaskSyncMode(config.SyncMode)
			if mode == sync.TaskSyncModeDirect {
				config.UpstreamParams = make(map[string]interface{})
				for _, m := range paramMappings {
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

// ReconcileRunningWindow 运行时段协调：对配置了 schedule_start_cron/schedule_end_cron 的 realtime 计划，
// 判断当前是否在时段内，在则自动启动、不在则自动停止。
func (s *SyncApplicationServiceImpl) ReconcileRunningWindow(ctx context.Context) error {
	plans, err := s.syncPlanRepo.GetEnabledPlans()
	if err != nil {
		return fmt.Errorf("get enabled plans: %w", err)
	}
	now := time.Now()
	for _, plan := range plans {
		if plan.Mode != sync.PlanModeRealtime || plan.ScheduleStartCron == nil || plan.ScheduleEndCron == nil {
			continue
		}
		inWindow, err := isInScheduleWindow(now, *plan.ScheduleStartCron, *plan.ScheduleEndCron)
		if err != nil {
			logrus.Warnf("[ReconcileRunningWindow] plan %s cron parse error: %v", plan.ID, err)
			continue
		}
		// 若配置了午休/暂停窗口，在该时段内视为不在运行窗口
		if inWindow && plan.SchedulePauseStartCron != nil && plan.SchedulePauseEndCron != nil {
			inPause, errPause := isInScheduleWindow(now, *plan.SchedulePauseStartCron, *plan.SchedulePauseEndCron)
			if errPause != nil {
				logrus.Warnf("[ReconcileRunningWindow] plan %s pause cron parse error: %v", plan.ID, errPause)
			} else if inPause {
				inWindow = false
			}
		}
		running, err := s.syncPlanRepo.GetRunningExecutionByPlanID(plan.ID)
		if err != nil {
			logrus.Warnf("[ReconcileRunningWindow] get running execution plan %s: %v", plan.ID, err)
			continue
		}
		if inWindow && running == nil {
			_, err = s.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{})
			if err != nil {
				logrus.Warnf("[ReconcileRunningWindow] start plan %s: %v", plan.ID, err)
			} else {
				logrus.Infof("[ReconcileRunningWindow] started plan %s (in window)", plan.ID)
			}
		} else if !inWindow && running != nil {
			if err := s.CancelExecution(ctx, running.ID); err != nil {
				logrus.Warnf("[ReconcileRunningWindow] cancel execution %s plan %s: %v", running.ID, plan.ID, err)
			} else {
				logrus.Infof("[ReconcileRunningWindow] stopped plan %s (outside window)", plan.ID)
			}
		}
	}
	return nil
}

// isInScheduleWindow 判断 now 是否在 startCron 与 endCron 构成的时段内（last_start > last_end 表示在时段内）。
func isInScheduleWindow(now time.Time, startCron, endCron string) (bool, error) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.DowOptional)
	startSched, err := parser.Parse(startCron)
	if err != nil {
		return false, err
	}
	endSched, err := parser.Parse(endCron)
	if err != nil {
		return false, err
	}
	lastStart := lastCronRunBefore(startSched, now)
	lastEnd := lastCronRunBefore(endSched, now)
	return lastStart.After(lastEnd), nil
}

func lastCronRunBefore(sched cron.Schedule, t time.Time) time.Time {
	from := t.Add(-366 * 24 * time.Hour)
	last := from
	for n := sched.Next(from); !n.After(t); n = sched.Next(n) {
		last = n
	}
	return last
}

// newsBackfillLagThreshold 新闻数据落后超过此值则打 WARN 并视为需要补齐（与 sync_jobs.newsCatchUpThreshold 一致）。
const newsBackfillLagThreshold = 5 * time.Minute

// checkNewsDataLag 检测新闻表最新数据与当前时间的滞后；若超过阈值打 WARN。不改变执行流程。
func (s *SyncApplicationServiceImpl) checkNewsDataLag(ctx context.Context, targetDBPath string) {
	if s.quantDBFactory == nil || targetDBPath == "" {
		return
	}
	db, err := s.quantDBFactory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: targetDBPath,
	})
	if err != nil {
		return
	}
	if err := db.Connect(ctx); err != nil {
		return
	}
	defer db.Close()
	exists, err := db.TableExists(ctx, "news")
	if err != nil || !exists {
		return
	}
	rows, err := db.Query(ctx, `SELECT max(datetime) AS max_dt FROM news`)
	if err != nil || len(rows) == 0 {
		return
	}
	raw := rows[0]["max_dt"]
	if raw == nil {
		raw = rows[0]["MAX_DT"]
	}
	if raw == nil {
		return
	}
	var maxTime time.Time
	switch v := raw.(type) {
	case time.Time:
		maxTime = v
	case *time.Time:
		if v != nil {
			maxTime = *v
		} else {
			return
		}
	case string:
		t, err := time.Parse("2006-01-02 15:04:05", v)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05Z07:00", v)
		}
		if err != nil {
			return
		}
		maxTime = t
	default:
		return
	}
	lag := time.Since(maxTime)
	if lag > newsBackfillLagThreshold {
		logrus.Warnf("[NewsBackfill] 新闻数据落后 %v（最新: %s），本次将执行追平", lag.Round(time.Second), maxTime.Format("2006-01-02 15:04:05"))
	}
}

// ExecuteNewsRealtimeOnce 直接提交一次轻量级新闻实时同步工作流（不走 ExecuteSyncPlan 的状态管理）。
func (s *SyncApplicationServiceImpl) ExecuteNewsRealtimeOnce(ctx context.Context) error {
	now := time.Now()
	forceBackfillCheck := false
	lastUnix := atomic.LoadInt64(&lastNewsForcedBackfillCheckUnix)
	if lastUnix == 0 || now.Sub(time.Unix(lastUnix, 0)) >= newsForceBackfillCheckInterval {
		if atomic.CompareAndSwapInt64(&lastNewsForcedBackfillCheckUnix, lastUnix, now.Unix()) {
			forceBackfillCheck = true
		}
	}

	plan, err := s.syncPlanRepo.Get(shared.ID(planIDRealtimeNews))
	if err != nil || plan == nil {
		return fmt.Errorf("load news plan: %w", err)
	}
	ds, err := s.dataSourceRepo.Get(plan.DataSourceID)
	if err != nil || ds == nil {
		return fmt.Errorf("load data source: %w", err)
	}
	token, err := s.dataSourceRepo.GetTokenByDataSource(plan.DataSourceID)
	if err != nil || token == nil {
		return fmt.Errorf("load token: %w", err)
	}
	dataStore, err := s.dataStoreRepo.Get(plan.DataStoreID)
	if err != nil || dataStore == nil {
		return fmt.Errorf("load data store: %w", err)
	}
	// 异步检测 lag，不阻塞 workflow 提交，避免 DuckDB Connect 争用导致“等很久也不启动”
	go func(path string) {
		lagCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.checkNewsDataLag(lagCtx, path)
	}(dataStore.StoragePath)
	params := map[string]interface{}{
		"data_source_name": ds.Name,
		"token":            token.TokenValue,
		"target_db_path":   dataStore.StoragePath,
		"force_backfill_check": forceBackfillCheck,
	}
	_, err = s.workflowExecutor.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameNewsRealtimeSync, params)
	return err
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

	// ts_realtime_mkt_tick 在 Job 内按 1000 条/30s 聚合；停流后若无新事件则尾批不会自动落库
	if s.quantDBFactory != nil {
		flushCtx, cancelFlush := context.WithTimeout(ctx, 90*time.Second)
		if _, err := jobs.FlushPendingTushareTickBatches(flushCtx, s.quantDBFactory, s.quantDBWriteQueue); err != nil {
			logrus.Warnf("[CancelExecution] FlushPendingTushareTickBatches: %v", err)
		}
		cancelFlush()
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
			// 生产实时且当前失败执行对应当前选中源：记录错误、切换到另一源，不标记 plan 完成
			if s.realtimeSourceSelector != nil && s.execCorrespondsToCurrentActive(plan, exec) {
				src := execSyncedAPIToSource(exec.SyncedAPIs)
				s.realtimeSourceSelector.RecordSourceError(src, errorMsg)
				if other := otherRealtimeSource(src); other != "" {
					s.realtimeSourceSelector.SwitchTo(other)
				}
			}
		}

		if err := repos.SyncPlanRepo().UpdatePlanExecution(exec); err != nil {
			return fmt.Errorf("failed to update execution: %w", err)
		}

		// Update plan status（当前选中源失败且已切换时不再 MarkCompleted，另一源继续运行）
		if plan != nil {
			if effectiveSuccess && plan.IncrementalMode && exec.ExecuteParams != nil && exec.ExecuteParams.EndDate != "" {
				plan.SetLastSuccessfulEndDate(exec.ExecuteParams.EndDate)
			}
			skipMarkCompleted := !effectiveSuccess && s.execCorrespondsToCurrentActive(plan, exec)
			if !skipMarkCompleted {
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
				if err := repos.SyncPlanRepo().Update(plan); err != nil {
					return fmt.Errorf("failed to update sync plan: %w", err)
				}
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
				if r != nil && workflow.IsSuccess(r.Status) {
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

	// Load plan once so we can expose schedule window (ScheduleStartCron/ScheduleEndCron) via progress DTO.
	plan, err := s.syncPlanRepo.Get(exec.SyncPlanID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync plan for execution: %w", err)
	}
	if plan == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found for execution", nil)
	}

	var wfStatus *workflow.WorkflowStatus
	instanceNotFound := false
	if s.taskEngineAdapter != nil && !exec.WorkflowInstID.IsEmpty() {
		wfStatus, err = s.taskEngineAdapter.GetInstanceStatus(ctx, exec.WorkflowInstID.String())
		if err != nil {
			if shared.IsNotFoundError(err) {
				instanceNotFound = true
				logrus.Warnf("[SyncExecution] Workflow instance %s not found when querying progress for execution %s", exec.WorkflowInstID, executionID)
			} else {
				// 非致命错误：交给调用方决定是否容忍
				return nil, fmt.Errorf("failed to get workflow status: %w", err)
			}
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
		ExecutionID:            exec.ID,
		PlanID:                 exec.SyncPlanID,
		WorkflowInstanceID:     exec.WorkflowInstID,
		ScheduleStartCron:      plan.ScheduleStartCron,
		ScheduleEndCron:        plan.ScheduleEndCron,
		SchedulePauseStartCron: plan.SchedulePauseStartCron,
		SchedulePauseEndCron:   plan.SchedulePauseEndCron,
		Status:                 exec.Status,
		RecordCount:            exec.RecordCount,
		ErrorMessage:           exec.ErrorMessage,
		StartedAt:              exec.StartedAt,
		FinishedAt:             exec.FinishedAt,
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

		if exec.Status == sync.ExecStatusRunning || exec.Status == sync.ExecStatusPending {
			var newStatus sync.ExecStatus
			if workflow.IsSuccess(wfStatus.Status) {
				newStatus = sync.ExecStatusSuccess
			} else if workflow.IsFailed(wfStatus.Status) {
				newStatus = sync.ExecStatusFailed
			} else if workflow.IsTerminated(wfStatus.Status) {
				newStatus = sync.ExecStatusCancelled
			}

			// 若工作流报告 Failed 但明细表里全部任务成功，以明细为准，改为 Success
			if newStatus == sync.ExecStatusFailed {
				details, derr := s.syncPlanRepo.GetExecutionDetailsByExecutionID(executionID)
				if derr == nil && len(details) > 0 {
					allSuccess := true
					var sum int64
					for _, d := range details {
						if !workflow.IsSuccess(d.Status) {
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
					exec.WorkflowErrorMessage = wfStatus.ErrorMessage
					if s.realtimeSourceSelector != nil && s.execCorrespondsToCurrentActive(plan, exec) {
						src := execSyncedAPIToSource(exec.SyncedAPIs)
						s.realtimeSourceSelector.RecordSourceError(src, errMsg)
						if other := otherRealtimeSource(src); other != "" {
							s.realtimeSourceSelector.SwitchTo(other)
						}
					}
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
					// (skip when production realtime current-active failure: other source keeps running)
					planForStatus, planErr := s.syncPlanRepo.Get(exec.SyncPlanID)
					if planErr == nil && planForStatus != nil && planForStatus.Status == sync.PlanStatusRunning {
						skipMark := newStatus == sync.ExecStatusFailed && s.execCorrespondsToCurrentActive(planForStatus, exec)
						if !skipMark {
							var nextRunAt *time.Time
							if planForStatus.CronExpression != nil && *planForStatus.CronExpression != "" {
								if s.planScheduler != nil {
									nextRunAt = s.planScheduler.GetNextRunTime(exec.SyncPlanID.String())
								}
								if nextRunAt == nil {
									nextRunAt, _ = s.cronCalculator.CalculateNextRunTime(*planForStatus.CronExpression, time.Now())
								}
							}
							planForStatus.MarkCompleted(nextRunAt)
							if updatePlanErr := s.syncPlanRepo.Update(planForStatus); updatePlanErr != nil {
								logrus.Warnf("[SyncExecution] Failed to mark plan completed after auto-sync: %v", updatePlanErr)
							} else {
								logrus.Infof("[SyncExecution] Plan %s marked completed after auto-sync from workflow", exec.SyncPlanID)
							}
						}
					}
				}
			}
		}
	} else if instanceNotFound && (exec.Status == sync.ExecStatusRunning || exec.Status == sync.ExecStatusPending) {
		// 工作流实例不存在：说明引擎侧已清理或历史数据缺失，避免执行永久停留在 running 状态
		logrus.Infof("[SyncExecution] Auto-marking execution %s as failed because workflow instance %s was not found", executionID, exec.WorkflowInstID)

		errMsg := "workflow instance not found during progress query"
		exec.MarkFailed(errMsg)
		planForFail, _ := s.syncPlanRepo.Get(exec.SyncPlanID)
		if planForFail != nil && s.realtimeSourceSelector != nil && s.execCorrespondsToCurrentActive(planForFail, exec) {
			src := execSyncedAPIToSource(exec.SyncedAPIs)
			s.realtimeSourceSelector.RecordSourceError(src, errMsg)
			if other := otherRealtimeSource(src); other != "" {
				s.realtimeSourceSelector.SwitchTo(other)
			}
		}

		if updateErr := s.syncPlanRepo.UpdatePlanExecution(exec); updateErr != nil {
			logrus.Warnf("[SyncExecution] Failed to persist auto-failed execution %s: %v", executionID, updateErr)
		} else {
			progress.Status = exec.Status
			progress.FinishedAt = exec.FinishedAt
			if exec.ErrorMessage != nil {
				progress.ErrorMessage = exec.ErrorMessage
			}

			// 当执行被自动标记为终态时，也需要把计划从 running 拉回正常状态（生产实时当前源失败且已切换时不标记）
			planForFail, planErr := s.syncPlanRepo.Get(exec.SyncPlanID)
			if planErr == nil && planForFail != nil && planForFail.Status == sync.PlanStatusRunning {
				skipMark := s.execCorrespondsToCurrentActive(planForFail, exec)
				if !skipMark {
					var nextRunAt *time.Time
					if planForFail.CronExpression != nil && *planForFail.CronExpression != "" {
						if s.planScheduler != nil {
							nextRunAt = s.planScheduler.GetNextRunTime(exec.SyncPlanID.String())
						}
						if nextRunAt == nil {
							nextRunAt, _ = s.cronCalculator.CalculateNextRunTime(*planForFail.CronExpression, time.Now())
						}
					}
					planForFail.MarkCompleted(nextRunAt)
					if updatePlanErr := s.syncPlanRepo.Update(planForFail); updatePlanErr != nil {
						logrus.Warnf("[SyncExecution] Failed to mark plan completed after auto-fail: %v", updatePlanErr)
					} else {
						logrus.Infof("[SyncExecution] Plan %s marked completed after auto-fail (workflow instance missing)", exec.SyncPlanID)
					}
				}
			}
		}
	}

	return progress, nil
}

// GetPlanProgress retrieves aggregated progress for the latest execution of a sync plan.
func (s *SyncApplicationServiceImpl) GetPlanProgress(ctx context.Context, planID shared.ID) (*contracts.SyncExecutionProgress, error) {
	// Always load plan so we can expose current schedule window config even when it has never been executed.
	plan, err := s.syncPlanRepo.Get(planID)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync plan: %w", err)
	}
	if plan == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "sync plan not found", nil)
	}

	execs, err := s.syncPlanRepo.GetExecutionsByPlan(planID)
	if err != nil {
		return nil, fmt.Errorf("failed to list sync executions for plan: %w", err)
	}

	if len(execs) == 0 {
		// Plan has never been executed - return a pending progress
		return &contracts.SyncExecutionProgress{
			PlanID:                 planID,
			Status:                 sync.ExecStatusPending,
			ScheduleStartCron:      plan.ScheduleStartCron,
			ScheduleEndCron:        plan.ScheduleEndCron,
			SchedulePauseStartCron: plan.SchedulePauseStartCron,
			SchedulePauseEndCron:   plan.SchedulePauseEndCron,
		}, nil
	}

	// DAO 已按 started_at DESC 排序，第一条即为最新执行
	latest := execs[0]
	return s.GetExecutionProgress(ctx, latest.ID)
}

// ReconcileRunningExecutions scans running executions and tries to sync their status
// with the underlying workflow engine. Intended to be invoked on server startup
// to clean up stale "running" executions after crashes or restarts.
func (s *SyncApplicationServiceImpl) ReconcileRunningExecutions(ctx context.Context) error {
	logrus.Info("[SyncExecution] Reconciling running sync executions on startup...")

	plans, err := s.syncPlanRepo.GetByStatus(sync.PlanStatusRunning)
	if err != nil {
		return fmt.Errorf("failed to load running sync plans for reconcile: %w", err)
	}

	for _, plan := range plans {
		execs, err := s.syncPlanRepo.GetExecutionsByPlan(plan.ID)
		if err != nil {
			logrus.Warnf("[SyncExecution] reconcile: failed to list executions for plan %s: %v", plan.ID, err)
			continue
		}
		for _, exec := range execs {
			if exec.Status != sync.ExecStatusRunning && exec.Status != sync.ExecStatusPending {
				continue
			}

			// 极端情况：执行记录没有工作流实例 ID，只能直接标记为失败，避免永久占用“运行中”状态
			if exec.WorkflowInstID.IsEmpty() {
				logrus.Warnf("[SyncExecution] reconcile: execution %s has empty workflow instance id, marking as failed", exec.ID)
				errMsg := "workflow instance id is empty during reconcile"
				exec.MarkFailed(errMsg)
				if s.realtimeSourceSelector != nil && s.execCorrespondsToCurrentActive(plan, exec) {
					src := execSyncedAPIToSource(exec.SyncedAPIs)
					s.realtimeSourceSelector.RecordSourceError(src, errMsg)
					if other := otherRealtimeSource(src); other != "" {
						s.realtimeSourceSelector.SwitchTo(other)
					}
				}
				if updateErr := s.syncPlanRepo.UpdatePlanExecution(exec); updateErr != nil {
					logrus.Warnf("[SyncExecution] reconcile: failed to update execution %s: %v", exec.ID, updateErr)
					continue
				}

				// 若计划仍标记为 running，则一并标记为 completed（生产实时当前源失败且已切换时不标记）
				if plan.Status == sync.PlanStatusRunning && !s.execCorrespondsToCurrentActive(plan, exec) {
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
					if updatePlanErr := s.syncPlanRepo.Update(plan); updatePlanErr != nil {
						logrus.Warnf("[SyncExecution] reconcile: failed to update plan %s after marking completed: %v", plan.ID, updatePlanErr)
					}
				}
				continue
			}

			// 实时计划：重启后 goroutine 已消亡，引擎持久化状态可能仍为 running；
			// 直接标记为 cancelled 并重置计划状态，允许后续 connect/schedule 重新启动。
			if plan.Mode == sync.PlanModeRealtime {
				logrus.Infof("[SyncExecution] reconcile: cancelling stale realtime execution %s (plan %s)", exec.ID, plan.ID)
				exec.MarkCancelled()
				if updateErr := s.syncPlanRepo.UpdatePlanExecution(exec); updateErr != nil {
					logrus.Warnf("[SyncExecution] reconcile: failed to update execution %s: %v", exec.ID, updateErr)
				}
				continue
			}

			// 其余情况复用 GetExecutionProgress 的自动同步逻辑（含终态映射与 Plan.MarkCompleted）
			if _, err := s.GetExecutionProgress(ctx, exec.ID); err != nil {
				logrus.Warnf("[SyncExecution] reconcile: failed to sync execution %s progress: %v", exec.ID, err)
			}
		}

		// 实时计划：若 plan 仍为 running 但所有 execution 已清理，重置为 enabled
		if plan.Mode == sync.PlanModeRealtime && plan.Status == sync.PlanStatusRunning {
			plan.Status = sync.PlanStatusEnabled
			if updateErr := s.syncPlanRepo.Update(plan); updateErr != nil {
				logrus.Warnf("[SyncExecution] reconcile: failed to reset realtime plan %s to enabled: %v", plan.ID, updateErr)
			} else {
				logrus.Infof("[SyncExecution] reconcile: reset realtime plan %s to enabled (ready for re-connect)", plan.ID)
			}
		}
	}

	return nil
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
		ID:          shared.NewID(),
		ExecutionID: exec.ID,
		TaskID:      taskID,
		APIName:     apiName,
		RecordCount: recordCount,
		Status:      status,
		FinishedAt:  ptrTimestamp(shared.Now()),
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
		if workflow.IsSuccess(r.Status) {
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
		if workflow.IsSuccess(r.Status) {
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
		DetailRows:           rows,
	}
	return out, nil
}

// ==================== Built-in Workflow Execution (Legacy) ====================
