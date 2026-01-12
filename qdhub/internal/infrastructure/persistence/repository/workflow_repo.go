package repository

import (
	"fmt"

	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
)

// WorkflowDefinitionRepositoryImpl implements workflow.WorkflowDefinitionRepository using Task Engine storage.
// This is a wrapper around the Task Engine implementation to maintain compatibility with the existing interface.
type WorkflowDefinitionRepositoryImpl struct {
	taskEngineRepo *WorkflowDefinitionRepositoryTaskEngineImpl
}

// NewWorkflowDefinitionRepository creates a new WorkflowDefinitionRepositoryImpl using Task Engine storage.
func NewWorkflowDefinitionRepository(db *persistence.DB) (*WorkflowDefinitionRepositoryImpl, error) {
	dsn := extractDSNFromDB(db)
	
	taskEngineRepo, err := NewWorkflowDefinitionRepositoryTaskEngine(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create task engine repository: %w", err)
	}

	return &WorkflowDefinitionRepositoryImpl{
		taskEngineRepo: taskEngineRepo,
	}, nil
}

// extractDSNFromDB extracts DSN from the DB connection.
func extractDSNFromDB(db *persistence.DB) string {
	// DSN is now stored in the DB struct
	return db.DSN()
}

// Create creates a new workflow definition with its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Create(def *workflow.WorkflowDefinition) error {
	return r.taskEngineRepo.Create(def)
}

// Get retrieves a workflow definition by ID with its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Get(id string) (*workflow.WorkflowDefinition, error) {
	return r.taskEngineRepo.Get(id)
}

// Update updates a workflow definition.
func (r *WorkflowDefinitionRepositoryImpl) Update(def *workflow.WorkflowDefinition) error {
	return r.taskEngineRepo.Update(def)
}

// Delete deletes a workflow definition and its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Delete(id string) error {
	return r.taskEngineRepo.Delete(id)
}

// List retrieves all workflow definitions (without aggregated entities for performance).
func (r *WorkflowDefinitionRepositoryImpl) List() ([]*workflow.WorkflowDefinition, error) {
	return r.taskEngineRepo.List()
}

// WorkflowInstanceRepositoryImpl implements workflow.WorkflowInstanceRepository using Task Engine storage.
type WorkflowInstanceRepositoryImpl struct {
	taskEngineRepo *WorkflowInstanceRepositoryTaskEngineImpl
}

// NewWorkflowInstanceRepository creates a new WorkflowInstanceRepositoryImpl using Task Engine storage.
func NewWorkflowInstanceRepository(db *persistence.DB) (*WorkflowInstanceRepositoryImpl, error) {
	dsn := extractDSNFromDB(db)
	
	taskEngineRepo, err := NewWorkflowInstanceRepositoryTaskEngine(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create task engine repository: %w", err)
	}

	return &WorkflowInstanceRepositoryImpl{
		taskEngineRepo: taskEngineRepo,
	}, nil
}

// Create creates a new workflow instance with its task instances.
func (r *WorkflowInstanceRepositoryImpl) Create(inst *workflow.WorkflowInstance) error {
	return r.taskEngineRepo.Create(inst)
}

// Get retrieves a workflow instance by ID with its task instances.
func (r *WorkflowInstanceRepositoryImpl) Get(id string) (*workflow.WorkflowInstance, error) {
	return r.taskEngineRepo.Get(id)
}

// GetByWorkflowDef retrieves all workflow instances for a workflow definition.
func (r *WorkflowInstanceRepositoryImpl) GetByWorkflowDef(workflowDefID string) ([]*workflow.WorkflowInstance, error) {
	return r.taskEngineRepo.GetByWorkflowDef(workflowDefID)
}

// Update updates a workflow instance.
func (r *WorkflowInstanceRepositoryImpl) Update(inst *workflow.WorkflowInstance) error {
	return r.taskEngineRepo.Update(inst)
}

// Delete deletes a workflow instance and its task instances.
func (r *WorkflowInstanceRepositoryImpl) Delete(id string) error {
	return r.taskEngineRepo.Delete(id)
}
