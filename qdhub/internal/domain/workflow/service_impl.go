// Package workflow contains the workflow domain service implementations.
package workflow

import (
	"fmt"
	"strings"

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

	if definition.ID() == "" {
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
	status := definition.Status()
	if status != WfDefStatusEnabled && status != WfDefStatusDisabled {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid workflow status: %s", status), nil)
	}

	return nil
}

// ValidateWorkflowInstance validates workflow instance.
// Note: Task Engine WorkflowInstance has a simpler structure than qdhub's original design.
func (v *workflowValidatorImpl) ValidateWorkflowInstance(instance *WorkflowInstance) error {
	if instance == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow instance cannot be nil", nil)
	}

	if instance.ID == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow instance ID cannot be empty", nil)
	}

	if instance.WorkflowID == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "workflow definition ID cannot be empty", nil)
	}

	// Validate status (Task Engine uses string status)
	validStatuses := []string{"Ready", "Running", "Paused", "Success", "Failed", "Terminated"}
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
		// Task Engine uses string status
		if task.Status == "Success" || task.Status == "Skipped" {
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
	// Task Engine uses string status
	if instance.Status == "Success" ||
		instance.Status == "Failed" ||
		instance.Status == "Terminated" {
		zero := int64(0)
		return &zero
	}

	// Task Engine WorkflowInstance doesn't have Progress field
	// Progress needs to be calculated from task instances
	// For now, return nil (cannot estimate without task instances and progress)
	return nil
}
