// Package scheduler provides cron scheduling infrastructure.
package scheduler

import (
	"context"
	"fmt"

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

// buildExecuteRequest builds ExecuteSyncPlanRequest from plan.DefaultExecuteParams.
// Returns nil if DefaultExecuteParams is missing or incomplete (no StartDate/EndDate).
func (e *ScheduledPlanExecutor) buildExecuteRequest(plan *sync.SyncPlan) *contracts.ExecuteSyncPlanRequest {
	if plan.DefaultExecuteParams == nil {
		return nil
	}
	p := plan.DefaultExecuteParams
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
