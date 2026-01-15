// Package repository provides repository implementations using Task Engine storage.
package repository

import (
	"context"
	"fmt"

	"github.com/LENAX/task-engine/pkg/storage"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"

	qdhubworkflow "qdhub/internal/domain/workflow"
)

// WorkflowDefinitionRepositoryTaskEngineImpl implements workflow.WorkflowDefinitionRepository using Task Engine storage.
type WorkflowDefinitionRepositoryTaskEngineImpl struct {
	aggregateRepo storage.WorkflowAggregateRepository
}

// NewWorkflowDefinitionRepositoryTaskEngine creates a new WorkflowDefinitionRepositoryTaskEngineImpl.
func NewWorkflowDefinitionRepositoryTaskEngine(dsn string) (*WorkflowDefinitionRepositoryTaskEngineImpl, error) {
	repo, err := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create task engine repository: %w", err)
	}

	return &WorkflowDefinitionRepositoryTaskEngineImpl{
		aggregateRepo: repo,
	}, nil
}

// Create creates a new workflow definition with its aggregated entities.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Create(def *qdhubworkflow.WorkflowDefinition) error {
	ctx := context.Background()

	// Directly use Task Engine Workflow (embedded in WorkflowDefinition)
	if err := r.aggregateRepo.SaveWorkflow(ctx, def.Workflow); err != nil {
		return fmt.Errorf("failed to save workflow: %w", err)
	}

	return nil
}

// Get retrieves a workflow definition by ID with its aggregated entities.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Get(id string) (*qdhubworkflow.WorkflowDefinition, error) {
	ctx := context.Background()

	// Get workflow with tasks from task engine
	teWorkflow, err := r.aggregateRepo.GetWorkflowWithTasks(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}
	if teWorkflow == nil {
		return nil, nil
	}

	// Wrap Task Engine Workflow in WorkflowDefinition
	// Note: qdhub-specific fields (Category, DefinitionYAML, etc.) need to be loaded separately
	// For now, we create a minimal WorkflowDefinition wrapper
	def := &qdhubworkflow.WorkflowDefinition{
		Workflow: teWorkflow,
		// TODO: Load qdhub-specific fields from a separate table or extend the schema
	}

	// Note: Instances field is not part of Task Engine Workflow, so we don't populate it here
	// If needed, it can be loaded separately using ListWorkflowInstances

	return def, nil
}

// Update updates a workflow definition.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Update(def *qdhubworkflow.WorkflowDefinition) error {
	ctx := context.Background()

	// Directly use Task Engine Workflow
	if err := r.aggregateRepo.SaveWorkflow(ctx, def.Workflow); err != nil {
		return fmt.Errorf("failed to update workflow: %w", err)
	}

	return nil
}

// Delete deletes a workflow definition and its aggregated entities.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Delete(id string) error {
	ctx := context.Background()

	if err := r.aggregateRepo.DeleteWorkflow(ctx, id); err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}

	return nil
}

// List retrieves all workflow definitions (without aggregated entities for performance).
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) List() ([]*qdhubworkflow.WorkflowDefinition, error) {
	ctx := context.Background()

	teWorkflows, err := r.aggregateRepo.ListWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}

	result := make([]*qdhubworkflow.WorkflowDefinition, 0, len(teWorkflows))
	for _, teWorkflow := range teWorkflows {
		// Wrap Task Engine Workflow in WorkflowDefinition
		def := &qdhubworkflow.WorkflowDefinition{
			Workflow: teWorkflow,
			// TODO: Load qdhub-specific fields
		}
		result = append(result, def)
	}

	return result, nil
}

// ==================== Child Entity Operations (WorkflowInstance) ====================

// AddInstance adds a new WorkflowInstance to a WorkflowDefinition.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) AddInstance(inst *qdhubworkflow.WorkflowInstance) error {
	ctx := context.Background()

	// Get workflow first to use StartWorkflow
	teWorkflow, err := r.aggregateRepo.GetWorkflow(ctx, inst.WorkflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow: %w", err)
	}
	if teWorkflow == nil {
		return fmt.Errorf("workflow not found: %s", inst.WorkflowID)
	}

	// Use StartWorkflow to create instance
	teInstance, err := r.aggregateRepo.StartWorkflow(ctx, teWorkflow)
	if err != nil {
		return fmt.Errorf("failed to start workflow: %w", err)
	}

	// Update instance ID if needed
	if inst.ID != teInstance.ID {
		inst.ID = teInstance.ID
	}

	return nil
}

// GetInstance retrieves a WorkflowInstance by ID.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) GetInstance(id string) (*qdhubworkflow.WorkflowInstance, error) {
	ctx := context.Background()

	teInstance, taskInstances, err := r.aggregateRepo.GetWorkflowInstanceWithTasks(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if teInstance == nil {
		return nil, nil
	}

	// Directly return Task Engine WorkflowInstance (it's a type alias)
	_ = taskInstances // TODO: Handle task instances if needed

	return teInstance, nil
}

// GetInstancesByDef retrieves all WorkflowInstances for a WorkflowDefinition.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) GetInstancesByDef(workflowDefID string) ([]*qdhubworkflow.WorkflowInstance, error) {
	ctx := context.Background()

	teInstances, err := r.aggregateRepo.ListWorkflowInstances(ctx, workflowDefID)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow instances: %w", err)
	}

	result := make([]*qdhubworkflow.WorkflowInstance, 0, len(teInstances))
	for _, inst := range teInstances {
		result = append(result, inst)
	}

	return result, nil
}

// UpdateInstance updates a WorkflowInstance.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) UpdateInstance(inst *qdhubworkflow.WorkflowInstance) error {
	ctx := context.Background()

	if err := r.aggregateRepo.UpdateWorkflowInstanceStatus(ctx, inst.ID, inst.Status); err != nil {
		return fmt.Errorf("failed to update workflow instance status: %w", err)
	}

	return nil
}

// DeleteInstance deletes a WorkflowInstance by ID.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) DeleteInstance(id string) error {
	ctx := context.Background()

	if err := r.aggregateRepo.DeleteWorkflowInstance(ctx, id); err != nil {
		return fmt.Errorf("failed to delete workflow instance: %w", err)
	}

	return nil
}
