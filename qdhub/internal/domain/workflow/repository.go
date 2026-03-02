// Package workflow contains the workflow domain repository interfaces.
package workflow

import "qdhub/internal/domain/shared"

// WorkflowDefinitionRepository defines the repository interface for WorkflowDefinition aggregate.
// Following DDD principles, this repository handles both the aggregate root (WorkflowDefinition)
// and its child entities (WorkflowInstance) to maintain aggregate boundaries.
// Uses Task Engine types directly with string IDs.
//
// Embeds shared.StringIDRepository[WorkflowDefinition] to inherit common CRUD operations.
type WorkflowDefinitionRepository interface {
	// Embed base repository for common CRUD operations (string ID version)
	shared.StringIDRepository[WorkflowDefinition]

	// ==================== Child Entity Operations (WorkflowInstance) ====================

	// AddInstance adds a new WorkflowInstance to a WorkflowDefinition.
	AddInstance(inst *WorkflowInstance) error

	// GetInstance retrieves a WorkflowInstance by ID.
	GetInstance(id string) (*WorkflowInstance, error)

	// GetInstancesByDef retrieves all WorkflowInstances for a WorkflowDefinition.
	GetInstancesByDef(workflowDefID string) ([]*WorkflowInstance, error)

	// UpdateInstance updates a WorkflowInstance.
	UpdateInstance(inst *WorkflowInstance) error

	// DeleteInstance deletes a WorkflowInstance by ID.
	DeleteInstance(id string) error
}
