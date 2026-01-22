// Package contracts defines application service interfaces and DTOs.
package contracts

import (
	"context"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// WorkflowApplicationService defines application service for workflow management.
// Responsibilities:
//   - Query workflow instances and status
//   - Control workflow execution (cancel)
//   - Query task instances
type WorkflowApplicationService interface {
	// ==================== Workflow Instance Query ====================

	// GetWorkflowInstance retrieves a workflow instance by ID.
	GetWorkflowInstance(ctx context.Context, id shared.ID) (*workflow.WorkflowInstance, error)

	// ListWorkflowInstances lists all instances for a workflow definition.
	ListWorkflowInstances(ctx context.Context, workflowDefID shared.ID, status *workflow.WfInstStatus) ([]*workflow.WorkflowInstance, error)

	// GetWorkflowStatus retrieves detailed status of a workflow instance.
	GetWorkflowStatus(ctx context.Context, instanceID shared.ID) (*workflow.WorkflowStatus, error)

	// ==================== Instance Control ====================

	// CancelWorkflow cancels a workflow instance.
	CancelWorkflow(ctx context.Context, instanceID shared.ID) error

	// ==================== Task Instance Management ====================

	// GetTaskInstances retrieves all task instances for a workflow instance.
	GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error)
}

