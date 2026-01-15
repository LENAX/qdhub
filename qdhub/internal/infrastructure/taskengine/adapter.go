// Package taskengine provides Task Engine integration for QDHub.
package taskengine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/LENAX/task-engine/pkg/core/engine"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// TaskEngineAdapterImpl implements workflow.TaskEngineAdapter.
type TaskEngineAdapterImpl struct {
	engine *engine.Engine
}

// NewTaskEngineAdapter creates a new TaskEngineAdapter implementation.
func NewTaskEngineAdapter(eng *engine.Engine) workflow.TaskEngineAdapter {
	return &TaskEngineAdapterImpl{
		engine: eng,
	}
}

// SubmitWorkflow submits a workflow to Task Engine.
func (a *TaskEngineAdapterImpl) SubmitWorkflow(ctx context.Context, definition *workflow.WorkflowDefinition, params map[string]interface{}) (string, error) {
	if definition == nil || definition.Workflow == nil {
		return "", fmt.Errorf("workflow definition is nil")
	}

	// Set workflow parameters (serialize non-string values to JSON)
	for key, value := range params {
		var strValue string
		switch v := value.(type) {
		case string:
			strValue = v
		default:
			// Serialize complex types to JSON
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("failed to serialize parameter %s: %w", key, err)
			}
			strValue = string(jsonBytes)
		}
		definition.Workflow.SetParam(key, strValue)
	}

	// Submit workflow and get controller
	controller, err := a.engine.SubmitWorkflow(ctx, definition.Workflow)
	if err != nil {
		return "", fmt.Errorf("failed to submit workflow: %w", err)
	}

	return controller.GetInstanceID(), nil
}

// PauseInstance pauses a workflow instance.
func (a *TaskEngineAdapterImpl) PauseInstance(ctx context.Context, engineInstanceID string) error {
	if err := a.engine.PauseWorkflowInstance(ctx, engineInstanceID); err != nil {
		return fmt.Errorf("failed to pause workflow: %w", err)
	}
	return nil
}

// ResumeInstance resumes a workflow instance.
func (a *TaskEngineAdapterImpl) ResumeInstance(ctx context.Context, engineInstanceID string) error {
	if err := a.engine.ResumeWorkflowInstance(ctx, engineInstanceID); err != nil {
		return fmt.Errorf("failed to resume workflow: %w", err)
	}
	return nil
}

// CancelInstance cancels a workflow instance.
func (a *TaskEngineAdapterImpl) CancelInstance(ctx context.Context, engineInstanceID string) error {
	if err := a.engine.TerminateWorkflowInstance(ctx, engineInstanceID, "cancelled by user"); err != nil {
		return fmt.Errorf("failed to cancel workflow: %w", err)
	}
	return nil
}

// GetInstanceStatus retrieves instance status from Task Engine.
func (a *TaskEngineAdapterImpl) GetInstanceStatus(ctx context.Context, engineInstanceID string) (*workflow.WorkflowStatus, error) {
	// Get status string from engine
	statusStr, err := a.engine.GetWorkflowInstanceStatus(ctx, engineInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance status: %w", err)
	}

	// Get workflow instance with tasks from aggregate repo
	aggregateRepo := a.engine.GetAggregateRepo()
	instance, taskInstances, err := aggregateRepo.GetWorkflowInstanceWithTasks(ctx, engineInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if instance == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "workflow instance not found", nil)
	}

	// Calculate progress
	completedTasks := 0
	failedTasks := 0
	for _, task := range taskInstances {
		switch task.Status {
		case "Success", "Skipped":
			completedTasks++
		case "Failed":
			failedTasks++
		}
	}

	progress := 0.0
	if len(taskInstances) > 0 {
		progress = float64(completedTasks) / float64(len(taskInstances)) * 100
	}

	status := &workflow.WorkflowStatus{
		InstanceID:    instance.ID,
		Status:        statusStr,
		Progress:      progress,
		TaskCount:     len(taskInstances),
		CompletedTask: completedTasks,
		FailedTask:    failedTasks,
		StartedAt:     shared.Timestamp(instance.StartTime),
	}

	if instance.EndTime != nil && !instance.EndTime.IsZero() {
		ts := shared.Timestamp(*instance.EndTime)
		status.FinishedAt = &ts
	}

	if instance.ErrorMessage != "" {
		status.ErrorMessage = &instance.ErrorMessage
	}

	return status, nil
}

// RegisterWorkflow registers a workflow definition with Task Engine.
func (a *TaskEngineAdapterImpl) RegisterWorkflow(ctx context.Context, definition *workflow.WorkflowDefinition) error {
	if definition == nil || definition.Workflow == nil {
		return fmt.Errorf("workflow definition is nil")
	}

	// Register workflow with Task Engine
	if err := a.engine.RegisterWorkflow(ctx, definition.Workflow); err != nil {
		return fmt.Errorf("failed to register workflow: %w", err)
	}

	return nil
}

// UnregisterWorkflow unregisters a workflow definition.
func (a *TaskEngineAdapterImpl) UnregisterWorkflow(ctx context.Context, definitionID string) error {
	// Task Engine doesn't have explicit unregister
	// Workflows are managed through the storage layer
	return nil
}
