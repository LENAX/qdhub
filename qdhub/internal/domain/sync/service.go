// Package sync contains the sync domain services.
package sync

import (
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
// Implementation: sync/service_impl.go
type CronScheduleCalculator interface {
	// CalculateNextRunTime calculates the next run time based on cron expression.
	CalculateNextRunTime(cronExpr string, fromTime time.Time) (*time.Time, error)

	// ParseCronExpression parses cron expression and validates it.
	ParseCronExpression(cronExpr string) error
}
