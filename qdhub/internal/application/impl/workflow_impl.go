// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// WorkflowApplicationServiceImpl implements WorkflowApplicationService.
type WorkflowApplicationServiceImpl struct {
	workflowDefRepo   workflow.WorkflowDefinitionRepository
	taskEngineAdapter workflow.TaskEngineAdapter
}

// NewWorkflowApplicationService creates a new WorkflowApplicationService implementation.
func NewWorkflowApplicationService(
	workflowDefRepo workflow.WorkflowDefinitionRepository,
	taskEngineAdapter workflow.TaskEngineAdapter,
) contracts.WorkflowApplicationService {
	return &WorkflowApplicationServiceImpl{
		workflowDefRepo:   workflowDefRepo,
		taskEngineAdapter: taskEngineAdapter,
	}
}

// ==================== Workflow Instance Query ====================

// GetWorkflowInstance retrieves a workflow instance by ID.
func (s *WorkflowApplicationServiceImpl) GetWorkflowInstance(ctx context.Context, id shared.ID) (*workflow.WorkflowInstance, error) {
	inst, err := s.workflowDefRepo.GetInstance(id.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if inst == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "workflow instance not found", nil)
	}
	return inst, nil
}

// ListWorkflowInstances lists all instances for a workflow definition.
func (s *WorkflowApplicationServiceImpl) ListWorkflowInstances(ctx context.Context, workflowDefID shared.ID, status *workflow.WfInstStatus) ([]*workflow.WorkflowInstance, error) {
	instances, err := s.workflowDefRepo.GetInstancesByDef(workflowDefID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow instances: %w", err)
	}

	// Filter by status if specified
	if status != nil {
		// Map qdhub status to Task Engine status
		teStatus := mapQDHubStatusToTaskEngine(*status)
		filtered := make([]*workflow.WorkflowInstance, 0)
		for _, inst := range instances {
			if inst.Status == teStatus {
				filtered = append(filtered, inst)
			}
		}
		return filtered, nil
	}

	return instances, nil
}

// GetWorkflowStatus retrieves detailed status of a workflow instance.
func (s *WorkflowApplicationServiceImpl) GetWorkflowStatus(ctx context.Context, instanceID shared.ID) (*workflow.WorkflowStatus, error) {
	status, err := s.taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}
	return status, nil
}

// ==================== Instance Control ====================

func getWorkflowInstance(repo workflow.WorkflowDefinitionRepository, instanceID shared.ID) (*workflow.WorkflowInstance, error) {
	instSlice, err := repo.GetInstancesByDef(instanceID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if len(instSlice) == 0 {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "workflow instance not found", nil)
	}
	return instSlice[0], nil
}

// CancelWorkflow cancels a workflow instance.
func (s *WorkflowApplicationServiceImpl) CancelWorkflow(ctx context.Context, instanceID shared.ID) error {
	inst, err := getWorkflowInstance(s.workflowDefRepo, instanceID)
	if err != nil {
		return err
	}

	// Can only cancel non-terminal instances
	if workflow.WfInstStatus(inst.Status) == workflow.WfInstStatusSuccess || workflow.WfInstStatus(inst.Status) == workflow.WfInstStatusFailed || workflow.WfInstStatus(inst.Status) == workflow.WfInstStatusCancelled {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "instance is already in terminal state", nil)
	}

	if err := s.taskEngineAdapter.CancelInstance(ctx, instanceID.String()); err != nil {
		return fmt.Errorf("failed to cancel workflow instance: %w", err)
	}

	return nil
}

// ==================== Task Instance Management ====================

// GetTaskInstances retrieves all task instances for a workflow instance.
func (s *WorkflowApplicationServiceImpl) GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error) {
	return s.taskEngineAdapter.GetTaskInstances(ctx, workflowInstID.String())
}

// ==================== Helper Functions ====================

// mapQDHubStatusToTaskEngine maps qdhub WfInstStatus to Task Engine status string.
func mapQDHubStatusToTaskEngine(status workflow.WfInstStatus) string {
	switch status {
	case workflow.WfInstStatusPending:
		return "Ready"
	case workflow.WfInstStatusRunning:
		return "Running"
	case workflow.WfInstStatusPaused:
		return "Paused"
	case workflow.WfInstStatusSuccess:
		return "Success"
	case workflow.WfInstStatusFailed:
		return "Failed"
	case workflow.WfInstStatusCancelled:
		return "Terminated"
	default:
		return string(status)
	}
}
