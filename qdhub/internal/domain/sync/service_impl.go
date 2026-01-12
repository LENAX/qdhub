// Package sync contains the sync domain service implementations.
package sync

import (
	"fmt"
	"strings"
	"time"

	"qdhub/internal/domain/shared"
)

// ==================== SyncJobValidator 实现 ====================

type syncJobValidatorImpl struct{}

// NewSyncJobValidator creates a new SyncJobValidator.
func NewSyncJobValidator() SyncJobValidator {
	return &syncJobValidatorImpl{}
}

// ValidateSyncJob validates sync job configuration.
func (v *syncJobValidatorImpl) ValidateSyncJob(job *SyncJob) error {
	if job == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "sync job cannot be nil", nil)
	}

	if job.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "sync job ID cannot be empty", nil)
	}

	if strings.TrimSpace(job.Name) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "sync job name cannot be empty", nil)
	}

	if job.APIMetadataID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "API metadata ID cannot be empty", nil)
	}

	if job.DataStoreID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "data store ID cannot be empty", nil)
	}

	if job.WorkflowDefID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow definition ID cannot be empty", nil)
	}

	// Validate sync mode
	if job.Mode != SyncModeBatch && job.Mode != SyncModeRealtime {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid sync mode: %s", job.Mode), nil)
	}

	// Validate cron expression if set
	if job.CronExpression != nil && *job.CronExpression != "" {
		if err := v.ValidateCronExpression(*job.CronExpression); err != nil {
			return err
		}
	}

	// Validate job status
	validStatuses := []JobStatus{JobStatusEnabled, JobStatusDisabled, JobStatusRunning}
	isValidStatus := false
	for _, status := range validStatuses {
		if job.Status == status {
			isValidStatus = true
			break
		}
	}
	if !isValidStatus {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid job status: %s", job.Status), nil)
	}

	// Validate param rules
	for i, rule := range job.ParamRules {
		if strings.TrimSpace(rule.ParamName) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("param rule[%d] param name cannot be empty", i), nil)
		}
		if strings.TrimSpace(rule.RuleType) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("param rule[%d] rule type cannot be empty", i), nil)
		}
	}

	return nil
}

// ValidateJobParams validates job parameters structure.
func (v *syncJobValidatorImpl) ValidateJobParams(params map[string]interface{}, paramRules []ParamRule) error {
	if params == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "job parameters cannot be nil", nil)
	}

	// Check if all required parameters are present
	for _, rule := range paramRules {
		if rule.RuleType == "required" {
			if _, exists := params[rule.ParamName]; !exists {
				return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("required parameter '%s' is missing", rule.ParamName), nil)
			}
		}
	}

	return nil
}

// ValidateCronExpression validates cron expression format.
func (v *syncJobValidatorImpl) ValidateCronExpression(cronExpr string) error {
	if strings.TrimSpace(cronExpr) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "cron expression cannot be empty", nil)
	}

	// Basic validation: cron expression should have 5 or 6 fields
	// Format: "second minute hour day month weekday" or "minute hour day month weekday"
	fields := strings.Fields(cronExpr)
	if len(fields) < 5 || len(fields) > 6 {
		return shared.NewDomainError(shared.ErrCodeValidation, "cron expression must have 5 or 6 fields", nil)
	}

	// Additional validation can be added here (e.g., validate each field range)
	return nil
}

// ==================== CronScheduleCalculator 实现 ====================

type cronScheduleCalculatorImpl struct{}

// NewCronScheduleCalculator creates a new CronScheduleCalculator.
func NewCronScheduleCalculator() CronScheduleCalculator {
	return &cronScheduleCalculatorImpl{}
}

// CalculateNextRunTime calculates the next run time based on cron expression.
// Note: This is a simplified implementation. In production, use a proper cron library.
func (c *cronScheduleCalculatorImpl) CalculateNextRunTime(cronExpr string, fromTime time.Time) (*time.Time, error) {
	if err := c.ParseCronExpression(cronExpr); err != nil {
		return nil, err
	}

	// Simplified implementation: just add 1 hour as placeholder
	// In real implementation, parse cron expression and calculate exact next time
	// TODO: Integrate with a proper cron library (e.g., github.com/robfig/cron)
	nextTime := fromTime.Add(1 * time.Hour)
	return &nextTime, nil
}

// ParseCronExpression parses cron expression and validates it.
func (c *cronScheduleCalculatorImpl) ParseCronExpression(cronExpr string) error {
	if strings.TrimSpace(cronExpr) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "cron expression cannot be empty", nil)
	}

	fields := strings.Fields(cronExpr)
	if len(fields) < 5 || len(fields) > 6 {
		return shared.NewDomainError(shared.ErrCodeValidation, "cron expression must have 5 or 6 fields", nil)
	}

	// TODO: Add more detailed validation for each field
	// - Validate numeric ranges
	// - Validate special characters (*, /, -, ,)
	// - Validate field-specific constraints

	return nil
}
