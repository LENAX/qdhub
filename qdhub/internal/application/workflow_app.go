// Package application contains application services that orchestrate use cases.
package application

import (
	"context"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// WorkflowApplicationService defines application service for workflow management.
// Responsibilities:
//   - Orchestrate complete workflow use cases
//   - Manage transactions
//   - Integrate with Task Engine
//   - Coordinate workflow lifecycle
type WorkflowApplicationService interface {
	// ==================== Workflow Definition Management ====================

	// CreateWorkflowDefinition creates a new workflow definition.
	CreateWorkflowDefinition(ctx context.Context, req CreateWorkflowDefinitionRequest) (*workflow.WorkflowDefinition, error)

	// GetWorkflowDefinition retrieves a workflow definition by ID.
	GetWorkflowDefinition(ctx context.Context, id shared.ID) (*workflow.WorkflowDefinition, error)

	// UpdateWorkflowDefinition updates a workflow definition.
	UpdateWorkflowDefinition(ctx context.Context, id shared.ID, req UpdateWorkflowDefinitionRequest) error

	// DeleteWorkflowDefinition deletes a workflow definition.
	// Will fail if there are running instances.
	DeleteWorkflowDefinition(ctx context.Context, id shared.ID) error

	// ListWorkflowDefinitions lists all workflow definitions.
	ListWorkflowDefinitions(ctx context.Context, category *workflow.WfCategory) ([]*workflow.WorkflowDefinition, error)

	// EnableWorkflow enables a workflow definition.
	EnableWorkflow(ctx context.Context, id shared.ID) error

	// DisableWorkflow disables a workflow definition.
	DisableWorkflow(ctx context.Context, id shared.ID) error

	// ==================== Workflow Execution ====================

	// ExecuteWorkflow executes a workflow definition.
	// This is a complex use case involving:
	//   1. Validate workflow is enabled
	//   2. Create workflow instance
	//   3. Submit to Task Engine
	//   4. Register callbacks
	//   5. Return instance ID
	ExecuteWorkflow(ctx context.Context, req ExecuteWorkflowRequest) (shared.ID, error)

	// GetWorkflowInstance retrieves a workflow instance by ID.
	GetWorkflowInstance(ctx context.Context, id shared.ID) (*workflow.WorkflowInstance, error)

	// ListWorkflowInstances lists all instances for a workflow definition.
	ListWorkflowInstances(ctx context.Context, workflowDefID shared.ID, status *workflow.WfInstStatus) ([]*workflow.WorkflowInstance, error)

	// GetWorkflowStatus retrieves detailed status of a workflow instance.
	GetWorkflowStatus(ctx context.Context, instanceID shared.ID) (*workflow.WorkflowStatus, error)

	// ==================== Instance Control ====================

	// PauseWorkflow pauses a running workflow instance.
	PauseWorkflow(ctx context.Context, instanceID shared.ID) error

	// ResumeWorkflow resumes a paused workflow instance.
	ResumeWorkflow(ctx context.Context, instanceID shared.ID) error

	// CancelWorkflow cancels a workflow instance.
	CancelWorkflow(ctx context.Context, instanceID shared.ID) error

	// ==================== Synchronization ====================

	// SyncWithEngine synchronizes workflow instance status with Task Engine.
	// This is useful for recovering state after system restart.
	SyncWithEngine(ctx context.Context, instanceID shared.ID) error

	// SyncAllInstances synchronizes all active instances with Task Engine.
	SyncAllInstances(ctx context.Context) error

	// ==================== Task Instance Management ====================

	// GetTaskInstances retrieves all task instances for a workflow instance.
	GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error)

	// RetryTask retries a failed task instance.
	RetryTask(ctx context.Context, taskInstanceID shared.ID) error
}

// ==================== Request/Response DTOs ====================

// CreateWorkflowDefinitionRequest represents a request to create a workflow definition.
type CreateWorkflowDefinitionRequest struct {
	Name           string
	Description    string
	Category       workflow.WfCategory
	DefinitionYAML string
	IsSystem       bool
}

// UpdateWorkflowDefinitionRequest represents a request to update a workflow definition.
type UpdateWorkflowDefinitionRequest struct {
	Name           *string
	Description    *string
	DefinitionYAML *string
}

// ExecuteWorkflowRequest represents a request to execute a workflow.
type ExecuteWorkflowRequest struct {
	WorkflowDefID shared.ID
	TriggerType   workflow.TriggerType
	TriggerParams map[string]interface{}
}
