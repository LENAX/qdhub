// Package scheduler provides cron scheduling infrastructure.
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

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

// ScheduleJob schedules a sync job with the given cron expression.
// If the job is already scheduled, it will be rescheduled with the new expression.
func (s *CronScheduler) ScheduleJob(jobID string, cronExpr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove existing schedule if any
	if existingID, exists := s.entryIDs[jobID]; exists {
		s.cron.Remove(existingID)
		delete(s.entryIDs, jobID)
	}

	// Add new schedule
	entryID, err := s.cron.AddFunc(cronExpr, func() {
		if s.jobHandler != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()
			if err := s.jobHandler.ExecuteScheduledJob(ctx, jobID); err != nil {
				// Log error but don't panic - the job will run again on next schedule
				fmt.Printf("[CronScheduler] Failed to execute job %s: %v\n", jobID, err)
			}
		}
	})
	if err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}

	s.entryIDs[jobID] = entryID
	return nil
}

// UnscheduleJob removes a job from the scheduler.
func (s *CronScheduler) UnscheduleJob(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, exists := s.entryIDs[jobID]; exists {
		s.cron.Remove(entryID)
		delete(s.entryIDs, jobID)
	}
}

// IsScheduled returns true if the job is currently scheduled.
func (s *CronScheduler) IsScheduled(jobID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.entryIDs[jobID]
	return exists
}

// GetNextRunTime returns the next scheduled run time for a job.
func (s *CronScheduler) GetNextRunTime(jobID string) *time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if entryID, exists := s.entryIDs[jobID]; exists {
		entry := s.cron.Entry(entryID)
		if entry.Valid() {
			next := entry.Next
			return &next
		}
	}
	return nil
}

// GetScheduledJobCount returns the number of scheduled jobs.
func (s *CronScheduler) GetScheduledJobCount() int {
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

var _ domainSync.JobScheduler = (*CronScheduler)(nil)
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
