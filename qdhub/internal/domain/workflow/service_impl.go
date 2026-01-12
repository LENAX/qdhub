// Package workflow contains the workflow domain service implementations.
package workflow

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"qdhub/internal/domain/shared"
)

// ==================== WorkflowValidator 实现 ====================

type workflowValidatorImpl struct{}

// NewWorkflowValidator creates a new WorkflowValidator.
func NewWorkflowValidator() WorkflowValidator {
	return &workflowValidatorImpl{}
}

// ValidateWorkflowDefinition validates workflow definition.
func (v *workflowValidatorImpl) ValidateWorkflowDefinition(definition *WorkflowDefinition) error {
	if definition == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow definition cannot be nil", nil)
	}

	if definition.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow definition ID cannot be empty", nil)
	}

	if strings.TrimSpace(definition.Name) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow name cannot be empty", nil)
	}

	// Validate category
	validCategories := []WfCategory{WfCategoryMetadata, WfCategorySync, WfCategoryCustom}
	isValidCategory := false
	for _, cat := range validCategories {
		if definition.Category == cat {
			isValidCategory = true
			break
		}
	}
	if !isValidCategory {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid workflow category: %s", definition.Category), nil)
	}

	// Validate definition YAML
	if err := v.ValidateDefinitionYAML(definition.DefinitionYAML); err != nil {
		return err
	}

	// Validate version
	if definition.Version < 1 {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow version must be at least 1", nil)
	}

	// Validate status
	if definition.Status != WfDefStatusEnabled && definition.Status != WfDefStatusDisabled {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid workflow status: %s", definition.Status), nil)
	}

	return nil
}

// ValidateWorkflowInstance validates workflow instance.
func (v *workflowValidatorImpl) ValidateWorkflowInstance(instance *WorkflowInstance) error {
	if instance == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow instance cannot be nil", nil)
	}

	if instance.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow instance ID cannot be empty", nil)
	}

	if instance.WorkflowDefID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow definition ID cannot be empty", nil)
	}

	if strings.TrimSpace(instance.EngineInstanceID) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "engine instance ID cannot be empty", nil)
	}

	// Validate trigger type
	validTriggerTypes := []TriggerType{TriggerTypeManual, TriggerTypeCron, TriggerTypeEvent}
	isValidTrigger := false
	for _, tt := range validTriggerTypes {
		if instance.TriggerType == tt {
			isValidTrigger = true
			break
		}
	}
	if !isValidTrigger {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid trigger type: %s", instance.TriggerType), nil)
	}

	// Validate status
	validStatuses := []WfInstStatus{
		WfInstStatusPending, WfInstStatusRunning, WfInstStatusPaused,
		WfInstStatusSuccess, WfInstStatusFailed, WfInstStatusCancelled,
	}
	isValidStatus := false
	for _, status := range validStatuses {
		if instance.Status == status {
			isValidStatus = true
			break
		}
	}
	if !isValidStatus {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid instance status: %s", instance.Status), nil)
	}

	// Validate progress range
	if instance.Progress < 0 || instance.Progress > 100 {
		return shared.NewDomainError(shared.ErrCodeValidation, "progress must be between 0 and 100", nil)
	}

	return nil
}

// ValidateDefinitionYAML validates workflow definition YAML format.
func (v *workflowValidatorImpl) ValidateDefinitionYAML(yamlContent string) error {
	if strings.TrimSpace(yamlContent) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow definition YAML cannot be empty", nil)
	}

	// Try to parse YAML to check syntax
	var temp interface{}
	if err := yaml.Unmarshal([]byte(yamlContent), &temp); err != nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "invalid YAML syntax", err)
	}

	// TODO: Add more specific validation for workflow structure
	// - Check required fields (name, tasks, etc.)
	// - Validate task dependencies
	// - Check for circular dependencies

	return nil
}

// ValidateTriggerParams validates trigger parameters.
func (v *workflowValidatorImpl) ValidateTriggerParams(triggerType TriggerType, params map[string]interface{}) error {
	if params == nil {
		return nil // Parameters are optional
	}

	// Validate based on trigger type
	switch triggerType {
	case TriggerTypeCron:
		// For cron trigger, validate cron expression if present
		if cronExpr, ok := params["cron_expression"]; ok {
			if expr, isString := cronExpr.(string); isString {
				if strings.TrimSpace(expr) == "" {
					return shared.NewDomainError(shared.ErrCodeValidation, "cron expression cannot be empty", nil)
				}
			}
		}
	case TriggerTypeEvent:
		// For event trigger, validate event type
		if eventType, ok := params["event_type"]; ok {
			if et, isString := eventType.(string); isString {
				if strings.TrimSpace(et) == "" {
					return shared.NewDomainError(shared.ErrCodeValidation, "event type cannot be empty", nil)
				}
			}
		}
	case TriggerTypeManual:
		// Manual trigger doesn't require specific parameters
	}

	return nil
}

// ==================== ProgressCalculator 实现 ====================

type progressCalculatorImpl struct{}

// NewProgressCalculator creates a new ProgressCalculator.
func NewProgressCalculator() ProgressCalculator {
	return &progressCalculatorImpl{}
}

// CalculateProgress calculates workflow progress based on task instances.
func (p *progressCalculatorImpl) CalculateProgress(tasks []TaskInstance) float64 {
	if len(tasks) == 0 {
		return 0.0
	}

	completedTasks := 0
	for _, task := range tasks {
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusSkipped {
			completedTasks++
		}
	}

	progress := float64(completedTasks) / float64(len(tasks)) * 100.0
	return progress
}

// EstimateRemainingTime estimates remaining time based on current progress.
func (p *progressCalculatorImpl) EstimateRemainingTime(instance *WorkflowInstance) *int64 {
	if instance == nil {
		return nil
	}

	// If workflow is completed or cancelled, no remaining time
	if instance.Status == WfInstStatusSuccess ||
		instance.Status == WfInstStatusFailed ||
		instance.Status == WfInstStatusCancelled {
		zero := int64(0)
		return &zero
	}

	// If progress is 0, cannot estimate
	if instance.Progress == 0 {
		return nil
	}

	// Calculate elapsed time
	elapsed := time.Since(instance.StartedAt.ToTime())
	elapsedSeconds := int64(elapsed.Seconds())

	// Estimate total time based on current progress
	estimatedTotal := float64(elapsedSeconds) / (instance.Progress / 100.0)
	remaining := int64(estimatedTotal) - elapsedSeconds

	if remaining < 0 {
		zero := int64(0)
		return &zero
	}

	return &remaining
}
