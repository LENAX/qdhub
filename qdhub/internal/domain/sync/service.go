// Package sync contains the sync domain services.
package sync

import (
	"context"
	"time"
)

// ==================== 领域服务接口（纯业务逻辑）====================

// SyncJobValidator defines domain service for sync job validation.
// Implementation: sync/service_impl.go
type SyncJobValidator interface {
	// ValidateSyncJob validates sync job configuration.
	ValidateSyncJob(job *SyncJob) error

	// ValidateJobParams validates job parameters structure.
	ValidateJobParams(params map[string]interface{}, paramRules []ParamRule) error

	// ValidateCronExpression validates cron expression format.
	ValidateCronExpression(cronExpr string) error
}

// CronScheduleCalculator defines domain service for cron schedule calculation.
// Implementation: infrastructure/scheduler/cron_scheduler.go (CronSchedulerCalculatorAdapter)
type CronScheduleCalculator interface {
	// CalculateNextRunTime calculates the next run time based on cron expression.
	CalculateNextRunTime(cronExpr string, fromTime time.Time) (*time.Time, error)

	// ParseCronExpression parses cron expression and validates it.
	ParseCronExpression(cronExpr string) error
}

// ==================== 调度器接口（基础设施层实现）====================

// JobScheduler defines the interface for scheduling sync jobs.
// Implementation: infrastructure/scheduler/cron_scheduler.go
type JobScheduler interface {
	// Start starts the scheduler.
	Start()

	// Stop stops the scheduler.
	Stop() context.Context

	// ScheduleJob schedules a job with the given cron expression.
	ScheduleJob(jobID string, cronExpr string) error

	// UnscheduleJob removes a job from the scheduler.
	UnscheduleJob(jobID string)

	// IsScheduled returns true if the job is currently scheduled.
	IsScheduled(jobID string) bool

	// GetNextRunTime returns the next scheduled run time for a job.
	GetNextRunTime(jobID string) *time.Time
}
