// Package workflow contains the workflow domain repository interfaces.
package workflow

import "qdhub/internal/domain/shared"

// WorkflowDefinitionRepository defines the repository interface for WorkflowDefinition aggregate.
type WorkflowDefinitionRepository interface {
	Create(def *WorkflowDefinition) error
	Get(id shared.ID) (*WorkflowDefinition, error)
	Update(def *WorkflowDefinition) error
	Delete(id shared.ID) error
	List() ([]*WorkflowDefinition, error)
}

// WorkflowInstanceRepository defines the repository interface for WorkflowInstance.
type WorkflowInstanceRepository interface {
	Create(inst *WorkflowInstance) error
	Get(id shared.ID) (*WorkflowInstance, error)
	GetByWorkflowDef(workflowDefID shared.ID) ([]*WorkflowInstance, error)
	Update(inst *WorkflowInstance) error
	Delete(id shared.ID) error
}
