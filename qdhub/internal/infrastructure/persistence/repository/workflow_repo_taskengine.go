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

// WorkflowInstanceRepositoryTaskEngineImpl implements workflow.WorkflowInstanceRepository using Task Engine storage.
type WorkflowInstanceRepositoryTaskEngineImpl struct {
	aggregateRepo storage.WorkflowAggregateRepository
}

// NewWorkflowInstanceRepositoryTaskEngine creates a new WorkflowInstanceRepositoryTaskEngineImpl.
func NewWorkflowInstanceRepositoryTaskEngine(dsn string) (*WorkflowInstanceRepositoryTaskEngineImpl, error) {
	repo, err := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create task engine repository: %w", err)
	}

	return &WorkflowInstanceRepositoryTaskEngineImpl{
		aggregateRepo: repo,
	}, nil
}

// Create creates a new workflow instance with its task instances.
func (r *WorkflowInstanceRepositoryTaskEngineImpl) Create(inst *qdhubworkflow.WorkflowInstance) error {
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
	// Note: StartWorkflow creates a new instance each time, so we need to handle this differently
	// For now, we'll create task instances manually after getting the instance ID
	teInstance, err := r.aggregateRepo.StartWorkflow(ctx, teWorkflow)
	if err != nil {
		return fmt.Errorf("failed to start workflow: %w", err)
	}

	// Update instance ID if needed
	if inst.ID != teInstance.ID {
		// If we need to preserve the original ID, we would need to update the instance
		// For now, we'll use the task engine generated ID
		inst.ID = teInstance.ID
	}

	// Save task instances
	// Note: Task instances should be created by Task Engine during StartWorkflow
	// If we need to add additional task instances, we can do so here

	return nil
}

// Get retrieves a workflow instance by ID with its task instances.
func (r *WorkflowInstanceRepositoryTaskEngineImpl) Get(id string) (*qdhubworkflow.WorkflowInstance, error) {
	ctx := context.Background()

	teInstance, taskInstances, err := r.aggregateRepo.GetWorkflowInstanceWithTasks(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if teInstance == nil {
		return nil, nil
	}

	// Directly return Task Engine WorkflowInstance (it's a type alias)
	// Note: Task instances are returned separately, so we need to handle them if needed
	_ = taskInstances // TODO: Handle task instances if needed

	return teInstance, nil
}

// GetByWorkflowDef retrieves all workflow instances for a workflow definition.
func (r *WorkflowInstanceRepositoryTaskEngineImpl) GetByWorkflowDef(workflowDefID string) ([]*qdhubworkflow.WorkflowInstance, error) {
	ctx := context.Background()

	teInstances, err := r.aggregateRepo.ListWorkflowInstances(ctx, workflowDefID)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow instances: %w", err)
	}

	// Convert to slice of pointers
	result := make([]*qdhubworkflow.WorkflowInstance, 0, len(teInstances))
	for _, inst := range teInstances {
		result = append(result, inst)
	}

	return result, nil
}

// Update updates a workflow instance.
func (r *WorkflowInstanceRepositoryTaskEngineImpl) Update(inst *qdhubworkflow.WorkflowInstance) error {
	ctx := context.Background()

	// Update status
	if err := r.aggregateRepo.UpdateWorkflowInstanceStatus(ctx, inst.ID, inst.Status); err != nil {
		return fmt.Errorf("failed to update workflow instance status: %w", err)
	}

	// Note: Task Engine doesn't provide a direct way to update all fields of WorkflowInstance
	// We can only update status. Other fields should be managed through Task Engine's API

	return nil
}

// Delete deletes a workflow instance and its task instances.
func (r *WorkflowInstanceRepositoryTaskEngineImpl) Delete(id string) error {
	ctx := context.Background()

	if err := r.aggregateRepo.DeleteWorkflowInstance(ctx, id); err != nil {
		return fmt.Errorf("failed to delete workflow instance: %w", err)
	}

	return nil
}
