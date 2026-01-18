// Package workflow contains the workflow domain services.
package workflow

import (
	"context"

	"qdhub/internal/domain/shared"
)

// ==================== 领域服务接口（纯业务逻辑）====================

// WorkflowValidator defines domain service for workflow validation.
// Implementation: workflow/service_impl.go
type WorkflowValidator interface {
	// ValidateWorkflowDefinition validates workflow definition.
	ValidateWorkflowDefinition(definition *WorkflowDefinition) error

	// ValidateWorkflowInstance validates workflow instance.
	ValidateWorkflowInstance(instance *WorkflowInstance) error

	// ValidateDefinitionYAML validates workflow definition YAML format.
	ValidateDefinitionYAML(yamlContent string) error

	// ValidateTriggerParams validates trigger parameters.
	ValidateTriggerParams(triggerType TriggerType, params map[string]interface{}) error
}

// ProgressCalculator defines domain service for progress calculation.
// Implementation: workflow/service_impl.go
type ProgressCalculator interface {
	// CalculateProgress calculates workflow progress based on task instances.
	CalculateProgress(tasks []TaskInstance) float64

	// EstimateRemainingTime estimates remaining time based on current progress.
	EstimateRemainingTime(instance *WorkflowInstance) *int64
}

// ==================== 数据传输对象 ====================

// WorkflowStatus represents the detailed status of a workflow instance.
type WorkflowStatus struct {
	InstanceID    string // Task Engine uses string ID
	Status        string // Task Engine status string
	Progress      float64
	TaskCount     int
	CompletedTask int
	FailedTask    int
	StartedAt     shared.Timestamp
	FinishedAt    *shared.Timestamp
	ErrorMessage  *string
}

// ==================== 外部依赖接口（领域定义，基础设施实现）====================

// WorkflowExecutor defines the interface for executing built-in workflows.
// This is a domain service interface that abstracts workflow execution.
// Implementation: infrastructure/taskengine/
//
// Purpose: This interface allows domain services and application services
// to execute workflows without directly depending on WorkflowApplicationService,
// following the Dependency Inversion Principle.
type WorkflowExecutor interface {
	// ExecuteBuiltInWorkflow executes a built-in workflow by its API name.
	// Returns the workflow instance ID.
	ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error)
}

// TaskEngineAdapter defines the interface for Task Engine integration.
// Implementation: infrastructure/taskengine/
type TaskEngineAdapter interface {
	// SubmitWorkflow submits a workflow to Task Engine.
	SubmitWorkflow(ctx context.Context, definition *WorkflowDefinition, params map[string]interface{}) (string, error)

	// PauseInstance pauses a workflow instance.
	PauseInstance(ctx context.Context, engineInstanceID string) error

	// ResumeInstance resumes a workflow instance.
	ResumeInstance(ctx context.Context, engineInstanceID string) error

	// CancelInstance cancels a workflow instance.
	CancelInstance(ctx context.Context, engineInstanceID string) error

	// GetInstanceStatus retrieves instance status from Task Engine.
	GetInstanceStatus(ctx context.Context, engineInstanceID string) (*WorkflowStatus, error)

	// RegisterWorkflow registers a workflow definition with Task Engine.
	RegisterWorkflow(ctx context.Context, definition *WorkflowDefinition) error

	// UnregisterWorkflow unregisters a workflow definition.
	UnregisterWorkflow(ctx context.Context, definitionID string) error

	// GetTaskInstances retrieves all task instances for a workflow instance.
	GetTaskInstances(ctx context.Context, engineInstanceID string) ([]*TaskInstance, error)

	// RetryTask retries a failed task instance.
	RetryTask(ctx context.Context, taskInstanceID string) error
}
