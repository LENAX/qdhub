// Package scheduler provides cron scheduling infrastructure.
package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// ScheduledPlanExecutor implements JobHandler for executing scheduled sync plans.
// SyncService is injected via SetSyncService after container init to break cycle.
type ScheduledPlanExecutor struct {
	syncSvc contracts.SyncApplicationService
}

// NewScheduledPlanExecutor creates a new ScheduledPlanExecutor.
func NewScheduledPlanExecutor() *ScheduledPlanExecutor {
	return &ScheduledPlanExecutor{}
}

// SetSyncService injects the sync application service (called after container init).
func (e *ScheduledPlanExecutor) SetSyncService(svc contracts.SyncApplicationService) {
	e.syncSvc = svc
}

// ExecuteScheduledJob implements JobHandler. Runs when cron triggers for a plan.
func (e *ScheduledPlanExecutor) ExecuteScheduledJob(ctx context.Context, jobID string) error {
	if e.syncSvc == nil {
		return fmt.Errorf("scheduled plan executor: sync service not injected")
	}

	planID := shared.ID(jobID)
	plan, err := e.syncSvc.GetSyncPlan(ctx, planID)
	if err != nil {
		return fmt.Errorf("get sync plan %s: %w", jobID, err)
	}
	if plan == nil {
		return fmt.Errorf("sync plan not found: %s", jobID)
	}

	req := e.buildExecuteRequest(plan)
	if req == nil {
		logrus.Warnf("[ScheduledPlanExecutor] Plan %s has no default execute params, skipping scheduled run", jobID)
		return fmt.Errorf("plan %s has no default_execute_params configured for scheduled runs", jobID)
	}

	_, err = e.syncSvc.ExecuteSyncPlan(ctx, planID, *req)
	if err != nil {
		return fmt.Errorf("execute sync plan %s: %w", jobID, err)
	}

	logrus.Infof("[ScheduledPlanExecutor] Scheduled run started for plan %s", jobID)
	return nil
}

// buildExecuteRequest builds ExecuteSyncPlanRequest for scheduled run.
// If incremental mode is on and LastSuccessfulEndDate is set, uses [LastSuccessfulEndDate, today];
// otherwise uses DefaultExecuteParams. Returns nil if no valid date range can be built.
func (e *ScheduledPlanExecutor) buildExecuteRequest(plan *sync.SyncPlan) *contracts.ExecuteSyncPlanRequest {
	if plan.DefaultExecuteParams == nil {
		return nil
	}
	p := plan.DefaultExecuteParams

	// 增量模式：上次成功 EndDate -> 当前日期
	if plan.IncrementalMode && plan.LastSuccessfulEndDate != nil && *plan.LastSuccessfulEndDate != "" {
		today := time.Now().Format("20060102")
		return &contracts.ExecuteSyncPlanRequest{
			StartDate: *plan.LastSuccessfulEndDate,
			EndDate:   today,
			StartTime: p.StartTime,
			EndTime:   p.EndTime,
		}
	}

	// 非增量或首次：使用默认日期范围
	if p.StartDate == "" || p.EndDate == "" {
		return nil
	}
	return &contracts.ExecuteSyncPlanRequest{
		StartDate:    p.StartDate,
		EndDate:      p.EndDate,
		StartTime:    p.StartTime,
		EndTime:      p.EndTime,
	}
}
