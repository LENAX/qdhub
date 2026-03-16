// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"strings"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// workflowInstanceStatusMatches 判断实例状态是否与期望的 WfInstStatus 一致（大小写不敏感）
func workflowInstanceStatusMatches(instStatus string, want workflow.WfInstStatus) bool {
	switch want {
	case workflow.WfInstStatusPending:
		return workflow.IsPending(instStatus)
	case workflow.WfInstStatusRunning:
		return workflow.IsRunning(instStatus)
	case workflow.WfInstStatusPaused:
		return workflow.IsPaused(instStatus)
	case workflow.WfInstStatusSuccess:
		return workflow.IsSuccess(instStatus)
	case workflow.WfInstStatusFailed:
		return workflow.IsFailed(instStatus)
	case workflow.WfInstStatusCancelled:
		return workflow.IsTerminated(instStatus)
	default:
		return strings.EqualFold(instStatus, string(want))
	}
}

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

	// Filter by status if specified（状态比较大小写不敏感）
	if status != nil {
		filtered := make([]*workflow.WorkflowInstance, 0)
		for _, inst := range instances {
			if workflowInstanceStatusMatches(inst.Status, *status) {
				filtered = append(filtered, inst)
			}
		}
		return filtered, nil
	}

	return instances, nil
}

// GetWorkflowStatus retrieves detailed status of a workflow instance.
// 当状态为终态时同步到数据库，便于前端与列表 API 看到最新状态与错误信息。
func (s *WorkflowApplicationServiceImpl) GetWorkflowStatus(ctx context.Context, instanceID shared.ID) (*workflow.WorkflowStatus, error) {
	status, err := s.taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if status != nil && workflow.IsTerminal(status.Status) {
		inst, getErr := s.workflowDefRepo.GetInstance(instanceID.String())
		if getErr == nil && inst != nil {
			inst.Status = status.Status
			if status.FinishedAt != nil {
				t := status.FinishedAt.ToTime()
				inst.EndTime = &t
			}
			if status.ErrorMessage != nil {
				inst.ErrorMessage = *status.ErrorMessage
			}
			_ = s.workflowDefRepo.UpdateInstance(inst)
		}
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
