// Package workflow contains the workflow domain repository interfaces.
package workflow

// WorkflowDefinitionRepository defines the repository interface for WorkflowDefinition aggregate.
// Uses Task Engine types directly.
type WorkflowDefinitionRepository interface {
	Create(def *WorkflowDefinition) error
	Get(id string) (*WorkflowDefinition, error)
	Update(def *WorkflowDefinition) error
	Delete(id string) error
	List() ([]*WorkflowDefinition, error)
}

// WorkflowInstanceRepository defines the repository interface for WorkflowInstance.
// Uses Task Engine types directly.
type WorkflowInstanceRepository interface {
	Create(inst *WorkflowInstance) error
	Get(id string) (*WorkflowInstance, error)
	GetByWorkflowDef(workflowDefID string) ([]*WorkflowInstance, error)
	Update(inst *WorkflowInstance) error
	Delete(id string) error
}
