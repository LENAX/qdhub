// Package scheduler provides cron scheduling infrastructure.
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"

	domainSync "qdhub/internal/domain/sync"
)

// CronScheduler manages cron-based job scheduling.
// It wraps robfig/cron to provide domain-specific scheduling functionality.
type CronScheduler struct {
	cron       *cron.Cron
	mu         sync.RWMutex
	entryIDs   map[string]cron.EntryID // jobID -> entryID
	jobHandler JobHandler
}

// JobHandler defines the callback interface for scheduled job execution.
type JobHandler interface {
	// ExecuteScheduledJob is called when a scheduled job should run.
	ExecuteScheduledJob(ctx context.Context, jobID string) error
}

// NewCronScheduler creates a new CronScheduler.
func NewCronScheduler(handler JobHandler) *CronScheduler {
	// Create cron with seconds field support and recover from panics
	c := cron.New(
		cron.WithSeconds(),
		cron.WithChain(
			cron.Recover(cron.DefaultLogger),
		),
	)

	return &CronScheduler{
		cron:       c,
		entryIDs:   make(map[string]cron.EntryID),
		jobHandler: handler,
	}
}

// Start starts the cron scheduler.
func (s *CronScheduler) Start() {
	s.cron.Start()
}

// Stop stops the cron scheduler and waits for running jobs to complete.
func (s *CronScheduler) Stop() context.Context {
	return s.cron.Stop()
}

// SchedulePlan schedules a sync plan with the given cron expression.
// If the plan is already scheduled, it will be rescheduled with the new expression.
func (s *CronScheduler) SchedulePlan(planID string, cronExpr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove existing schedule if any
	if existingID, exists := s.entryIDs[planID]; exists {
		s.cron.Remove(existingID)
		delete(s.entryIDs, planID)
	}

	// Add new schedule
	entryID, err := s.cron.AddFunc(cronExpr, func() {
		if s.jobHandler != nil {
			logrus.Infof("[CronScheduler] Triggering scheduled run for plan %s", planID)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()
			if err := s.jobHandler.ExecuteScheduledJob(ctx, planID); err != nil {
				logrus.Warnf("[CronScheduler] Failed to execute plan %s: %v", planID, err)
			}
		}
	})
	if err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}

	s.entryIDs[planID] = entryID
	logrus.Infof("[CronScheduler] Plan %s scheduled with cron %q", planID, cronExpr)
	return nil
}

// UnschedulePlan removes a plan from the scheduler.
func (s *CronScheduler) UnschedulePlan(planID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, exists := s.entryIDs[planID]; exists {
		s.cron.Remove(entryID)
		delete(s.entryIDs, planID)
	}
}

// GetScheduledPlanIDs returns IDs of plans currently registered in the scheduler.
func (s *CronScheduler) GetScheduledPlanIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.entryIDs))
	for id := range s.entryIDs {
		ids = append(ids, id)
	}
	return ids
}

// IsScheduled returns true if the plan is currently scheduled.
func (s *CronScheduler) IsScheduled(planID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.entryIDs[planID]
	return exists
}

// GetNextRunTime returns the next scheduled run time for a plan.
func (s *CronScheduler) GetNextRunTime(planID string) *time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if entryID, exists := s.entryIDs[planID]; exists {
		entry := s.cron.Entry(entryID)
		if entry.Valid() {
			next := entry.Next
			return &next
		}
	}
	return nil
}

// GetScheduledPlanCount returns the number of scheduled plans.
func (s *CronScheduler) GetScheduledPlanCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entryIDs)
}

// ==================== CronParser ====================

// CronParser provides cron expression parsing and validation using robfig/cron.
type CronParser struct {
	parser cron.Parser
}

// NewCronParser creates a new CronParser.
func NewCronParser() *CronParser {
	return &CronParser{
		parser: cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
	}
}

// Parse parses a cron expression and returns an error if invalid.
func (p *CronParser) Parse(cronExpr string) error {
	_, err := p.parser.Parse(cronExpr)
	return err
}

// CalculateNextRunTime calculates the next run time from the given time.
func (p *CronParser) CalculateNextRunTime(cronExpr string, fromTime time.Time) (*time.Time, error) {
	schedule, err := p.parser.Parse(cronExpr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}

	next := schedule.Next(fromTime)
	return &next, nil
}

// ==================== Ensure interfaces ====================

var _ domainSync.PlanScheduler = (*CronScheduler)(nil)
var _ domainSync.CronScheduleCalculator = (*CronSchedulerCalculatorAdapter)(nil)

// CronSchedulerCalculatorAdapter adapts CronParser to domain CronScheduleCalculator interface.
type CronSchedulerCalculatorAdapter struct {
	parser *CronParser
}

// NewCronSchedulerCalculatorAdapter creates a new adapter.
func NewCronSchedulerCalculatorAdapter() *CronSchedulerCalculatorAdapter {
	return &CronSchedulerCalculatorAdapter{
		parser: NewCronParser(),
	}
}

// CalculateNextRunTime implements CronScheduleCalculator.
func (a *CronSchedulerCalculatorAdapter) CalculateNextRunTime(cronExpr string, fromTime time.Time) (*time.Time, error) {
	return a.parser.CalculateNextRunTime(cronExpr, fromTime)
}

// ParseCronExpression implements CronScheduleCalculator.
func (a *CronSchedulerCalculatorAdapter) ParseCronExpression(cronExpr string) error {
	return a.parser.Parse(cronExpr)
}
