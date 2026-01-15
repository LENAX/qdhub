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
	workflowValidator workflow.WorkflowValidator
	progressCalc      workflow.ProgressCalculator
}

// NewWorkflowApplicationService creates a new WorkflowApplicationService implementation.
func NewWorkflowApplicationService(
	workflowDefRepo workflow.WorkflowDefinitionRepository,
	taskEngineAdapter workflow.TaskEngineAdapter,
) contracts.WorkflowApplicationService {
	return &WorkflowApplicationServiceImpl{
		workflowDefRepo:   workflowDefRepo,
		taskEngineAdapter: taskEngineAdapter,
		workflowValidator: workflow.NewWorkflowValidator(),
		progressCalc:      workflow.NewProgressCalculator(),
	}
}

// ==================== Workflow Definition Management ====================

// CreateWorkflowDefinition creates a new workflow definition.
func (s *WorkflowApplicationServiceImpl) CreateWorkflowDefinition(ctx context.Context, req contracts.CreateWorkflowDefinitionRequest) (*workflow.WorkflowDefinition, error) {
	// Create domain entity
	def := workflow.NewWorkflowDefinition(
		req.Name,
		req.Description,
		req.Category,
		req.DefinitionYAML,
		req.IsSystem,
	)

	// Validate
	if err := s.workflowValidator.ValidateWorkflowDefinition(def); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Register with Task Engine
	if err := s.taskEngineAdapter.RegisterWorkflow(ctx, def); err != nil {
		return nil, fmt.Errorf("failed to register workflow with task engine: %w", err)
	}

	// Persist
	if err := s.workflowDefRepo.Create(def); err != nil {
		// Try to unregister from Task Engine on failure
		_ = s.taskEngineAdapter.UnregisterWorkflow(ctx, def.ID())
		return nil, fmt.Errorf("failed to create workflow definition: %w", err)
	}

	return def, nil
}

// GetWorkflowDefinition retrieves a workflow definition by ID.
func (s *WorkflowApplicationServiceImpl) GetWorkflowDefinition(ctx context.Context, id shared.ID) (*workflow.WorkflowDefinition, error) {
	def, err := s.workflowDefRepo.Get(id.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if def == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}
	return def, nil
}

// UpdateWorkflowDefinition updates a workflow definition.
func (s *WorkflowApplicationServiceImpl) UpdateWorkflowDefinition(ctx context.Context, id shared.ID, req contracts.UpdateWorkflowDefinitionRequest) error {
	def, err := s.workflowDefRepo.Get(id.String())
	if err != nil {
		return fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if def == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}

	// Check for running instances
	instances, err := s.workflowDefRepo.GetInstancesByDef(id.String())
	if err != nil {
		return fmt.Errorf("failed to check running instances: %w", err)
	}
	for _, inst := range instances {
		if inst.Status == "Running" || inst.Status == "Ready" {
			return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot update workflow with running instances", nil)
		}
	}

	// Apply updates
	if req.Name != nil {
		def.Workflow.Name = *req.Name
	}
	if req.Description != nil {
		def.Workflow.Description = *req.Description
	}
	if req.DefinitionYAML != nil {
		def.UpdateDefinition(*req.DefinitionYAML)
	}

	// Validate
	if err := s.workflowValidator.ValidateWorkflowDefinition(def); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Update in Task Engine
	if err := s.taskEngineAdapter.RegisterWorkflow(ctx, def); err != nil {
		return fmt.Errorf("failed to update workflow in task engine: %w", err)
	}

	// Persist
	if err := s.workflowDefRepo.Update(def); err != nil {
		return fmt.Errorf("failed to update workflow definition: %w", err)
	}

	return nil
}

// DeleteWorkflowDefinition deletes a workflow definition.
func (s *WorkflowApplicationServiceImpl) DeleteWorkflowDefinition(ctx context.Context, id shared.ID) error {
	def, err := s.workflowDefRepo.Get(id.String())
	if err != nil {
		return fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if def == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}

	// Check for running instances
	instances, err := s.workflowDefRepo.GetInstancesByDef(id.String())
	if err != nil {
		return fmt.Errorf("failed to check running instances: %w", err)
	}
	for _, inst := range instances {
		if inst.Status == "Running" || inst.Status == "Ready" {
			return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot delete workflow with running instances", nil)
		}
	}

	// Unregister from Task Engine
	if err := s.taskEngineAdapter.UnregisterWorkflow(ctx, id.String()); err != nil {
		return fmt.Errorf("failed to unregister workflow from task engine: %w", err)
	}

	// Delete from repository
	if err := s.workflowDefRepo.Delete(id.String()); err != nil {
		return fmt.Errorf("failed to delete workflow definition: %w", err)
	}

	return nil
}

// ListWorkflowDefinitions lists all workflow definitions.
func (s *WorkflowApplicationServiceImpl) ListWorkflowDefinitions(ctx context.Context, category *workflow.WfCategory) ([]*workflow.WorkflowDefinition, error) {
	defs, err := s.workflowDefRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow definitions: %w", err)
	}

	// Filter by category if specified
	if category != nil {
		filtered := make([]*workflow.WorkflowDefinition, 0)
		for _, def := range defs {
			if def.Category == *category {
				filtered = append(filtered, def)
			}
		}
		return filtered, nil
	}

	return defs, nil
}

// EnableWorkflow enables a workflow definition.
func (s *WorkflowApplicationServiceImpl) EnableWorkflow(ctx context.Context, id shared.ID) error {
	def, err := s.workflowDefRepo.Get(id.String())
	if err != nil {
		return fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if def == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}

	def.Enable()

	if err := s.workflowDefRepo.Update(def); err != nil {
		return fmt.Errorf("failed to update workflow definition: %w", err)
	}

	return nil
}

// DisableWorkflow disables a workflow definition.
func (s *WorkflowApplicationServiceImpl) DisableWorkflow(ctx context.Context, id shared.ID) error {
	def, err := s.workflowDefRepo.Get(id.String())
	if err != nil {
		return fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if def == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}

	// Check for running instances
	instances, err := s.workflowDefRepo.GetInstancesByDef(id.String())
	if err != nil {
		return fmt.Errorf("failed to check running instances: %w", err)
	}
	for _, inst := range instances {
		if inst.Status == "Running" {
			return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot disable workflow with running instances", nil)
		}
	}

	def.Disable()

	if err := s.workflowDefRepo.Update(def); err != nil {
		return fmt.Errorf("failed to update workflow definition: %w", err)
	}

	return nil
}

// ==================== Workflow Execution ====================

// ExecuteWorkflow executes a workflow definition.
func (s *WorkflowApplicationServiceImpl) ExecuteWorkflow(ctx context.Context, req contracts.ExecuteWorkflowRequest) (shared.ID, error) {
	// Get workflow definition
	def, err := s.workflowDefRepo.Get(req.WorkflowDefID.String())
	if err != nil {
		return "", fmt.Errorf("failed to get workflow definition: %w", err)
	}
	if def == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "workflow definition not found", nil)
	}

	// Check if workflow can be executed
	if err := def.CanCreateInstance(); err != nil {
		return "", err
	}

	// Validate trigger params
	if err := s.workflowValidator.ValidateTriggerParams(req.TriggerType, req.TriggerParams); err != nil {
		return "", fmt.Errorf("invalid trigger params: %w", err)
	}

	// Submit to Task Engine
	engineInstanceID, err := s.taskEngineAdapter.SubmitWorkflow(ctx, def, req.TriggerParams)
	if err != nil {
		return "", fmt.Errorf("failed to submit workflow: %w", err)
	}

	return shared.ID(engineInstanceID), nil
}

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

// PauseWorkflow pauses a running workflow instance.
func (s *WorkflowApplicationServiceImpl) PauseWorkflow(ctx context.Context, instanceID shared.ID) error {
	inst, err := getWorkflowInstance(s.workflowDefRepo, instanceID)
	if err != nil {
		return err
	}

	if workflow.WfInstStatus(inst.Status) != workflow.WfInstStatusRunning {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "can only pause running instances", nil)
	}

	if err := s.taskEngineAdapter.PauseInstance(ctx, instanceID.String()); err != nil {
		return fmt.Errorf("failed to pause workflow instance: %w", err)
	}

	return nil
}

// ResumeWorkflow resumes a paused workflow instance.
func (s *WorkflowApplicationServiceImpl) ResumeWorkflow(ctx context.Context, instanceID shared.ID) error {
	inst, err := getWorkflowInstance(s.workflowDefRepo, instanceID)
	if err != nil {
		return err
	}

	if workflow.WfInstStatus(inst.Status) != workflow.WfInstStatusPaused {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "can only resume paused instances", nil)
	}

	if err := s.taskEngineAdapter.ResumeInstance(ctx, instanceID.String()); err != nil {
		return fmt.Errorf("failed to resume workflow instance: %w", err)
	}

	return nil
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

// ==================== Synchronization ====================

// SyncWithEngine synchronizes workflow instance status with Task Engine.
func (s *WorkflowApplicationServiceImpl) SyncWithEngine(ctx context.Context, instanceID shared.ID) error {
	// Get status from Task Engine
	status, err := s.taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
	if err != nil {
		return fmt.Errorf("failed to get instance status from task engine: %w", err)
	}

	// Get local instance
	inst, err := s.workflowDefRepo.GetInstance(instanceID.String())
	if err != nil {
		return fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if inst == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "workflow instance not found", nil)
	}

	// Update local status
	inst.Status = status.Status
	if err := s.workflowDefRepo.UpdateInstance(inst); err != nil {
		return fmt.Errorf("failed to update workflow instance: %w", err)
	}

	return nil
}

// SyncAllInstances synchronizes all active instances with Task Engine.
func (s *WorkflowApplicationServiceImpl) SyncAllInstances(ctx context.Context) error {
	// Get all workflow definitions
	defs, err := s.workflowDefRepo.List()
	if err != nil {
		return fmt.Errorf("failed to list workflow definitions: %w", err)
	}

	for _, def := range defs {
		instances, err := s.workflowDefRepo.GetInstancesByDef(def.ID())
		if err != nil {
			continue // Skip on error
		}

		for _, inst := range instances {
			// Only sync active instances
			if inst.Status == "Running" || inst.Status == "Ready" || inst.Status == "Paused" {
				_ = s.SyncWithEngine(ctx, shared.ID(inst.ID))
			}
		}
	}

	return nil
}

// ==================== Task Instance Management ====================

// GetTaskInstances retrieves all task instances for a workflow instance.
func (s *WorkflowApplicationServiceImpl) GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error) {
	// Get status from Task Engine (includes task info)
	status, err := s.taskEngineAdapter.GetInstanceStatus(ctx, workflowInstID.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}

	// Task instances are not directly available from WorkflowStatus
	// We need to get them from the workflow instance repository
	_ = status // Status is retrieved but task instances need different handling

	// For now, return empty slice as task instances are managed by Task Engine
	// In a full implementation, we would query Task Engine for task instances
	return []*workflow.TaskInstance{}, nil
}

// RetryTask retries a failed task instance.
func (s *WorkflowApplicationServiceImpl) RetryTask(ctx context.Context, taskInstanceID shared.ID) error {
	// Task retry is handled by Task Engine
	// This would require adding a RetryTask method to TaskEngineAdapter
	return shared.NewDomainError(shared.ErrCodeInvalidState, "task retry not implemented", nil)
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
