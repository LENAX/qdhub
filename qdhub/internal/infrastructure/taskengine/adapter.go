// Package taskengine provides Task Engine integration for QDHub.
package taskengine

import (
	"context"
	"fmt"
	"strings"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/sirupsen/logrus"

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
// Uses Task Engine's native parameter replacement feature to replace placeholders
// (e.g., ${param_name}) with actual values before submission.
//
// IMPORTANT: This method reloads the workflow definition from Task Engine storage
// before parameter replacement to ensure we don't modify the original definition.
// This prevents issues where ReplaceParams would mutate a shared/cached object.
func (a *TaskEngineAdapterImpl) SubmitWorkflow(ctx context.Context, definition *workflow.WorkflowDefinition, params map[string]interface{}) (string, error) {
	if definition == nil || definition.Workflow == nil {
		return "", fmt.Errorf("workflow definition is nil")
	}

	// Get workflow ID from the definition
	workflowID := definition.Workflow.GetID()
	if workflowID == "" {
		return "", fmt.Errorf("workflow ID is empty")
	}

	// Reload workflow from Task Engine storage to get a fresh copy
	// This ensures we don't modify the original definition object
	aggregateRepo := a.engine.GetAggregateRepo()
	workflowCopy, err := aggregateRepo.GetWorkflowWithTasks(ctx, workflowID)
	if err != nil {
		return "", fmt.Errorf("failed to reload workflow for execution: %w", err)
	}
	if workflowCopy == nil {
		return "", fmt.Errorf("workflow not found: %s", workflowID)
	}

	// Use Task Engine's native ReplaceParams method to replace placeholders
	// This will replace placeholders in both Workflow-level and Task-level parameters
	// We do this on the reloaded copy, not the original definition
	if len(params) > 0 {
		if err := workflowCopy.ReplaceParams(params); err != nil {
			return "", fmt.Errorf("failed to replace workflow parameters: %w", err)
		}
	}

	// Submit the reloaded workflow copy (with replaced parameters) to Task Engine
	controller, err := a.engine.SubmitWorkflow(ctx, workflowCopy)
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

	// Calculate progress and determine final status
	// Note: Task Engine may use different case for status (e.g., "FAILED" vs "Failed")
	completedTasks := 0
	failedTasks := 0
	runningTasks := 0
	for _, task := range taskInstances {
		// Normalize status to uppercase for comparison
		status := strings.ToUpper(task.Status)
		switch status {
		case "SUCCESS", "SKIPPED":
			completedTasks++
		case "FAILED":
			failedTasks++
		case "RUNNING", "PENDING":
			runningTasks++
		}
	}

	progress := 0.0
	finalStatus := statusStr

	if len(taskInstances) > 0 {
		// Normal case: calculate progress from task instances
		progress = float64(completedTasks) / float64(len(taskInstances)) * 100

		// Determine final status based on task completion
		totalDone := completedTasks + failedTasks
		if totalDone == len(taskInstances) {
			// All tasks are done
			if failedTasks > 0 {
				finalStatus = "Failed"
			} else {
				finalStatus = "Success"
			}
		} else if failedTasks > 0 && runningTasks == 0 {
			// Some tasks failed and no tasks are running
			finalStatus = "Failed"
		}
	} else {
		// FALLBACK: When taskInstances is empty (e.g., dynamically submitted workflows),
		// use the workflow instance status from Task Engine directly.
		// This handles the case where task instances are not persisted to storage.
		logrus.Warnf("[GetInstanceStatus] instanceID=%s, taskInstances=0, using instance.Status=%s as fallback",
			engineInstanceID, instance.Status)

		// Use instance.Status for both status and progress estimation
		instanceStatusUpper := strings.ToUpper(instance.Status)
		switch instanceStatusUpper {
		case "SUCCESS", "COMPLETED":
			finalStatus = "Success"
			progress = 100.0
		case "FAILED", "ERROR":
			finalStatus = "Failed"
			progress = 100.0
		case "TERMINATED", "CANCELLED":
			finalStatus = "Terminated"
			progress = 100.0
		case "RUNNING", "PENDING":
			finalStatus = "Running"
			// Keep progress at 0 since we can't calculate it without tasks
		default:
			// Use statusStr from engine as final fallback
			finalStatus = statusStr
		}
	}

	status := &workflow.WorkflowStatus{
		InstanceID:    instance.ID,
		Status:        finalStatus,
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

// GetTaskInstances retrieves all task instances for a workflow instance.
func (a *TaskEngineAdapterImpl) GetTaskInstances(ctx context.Context, engineInstanceID string) ([]*workflow.TaskInstance, error) {
	aggregateRepo := a.engine.GetAggregateRepo()
	_, taskInstances, err := aggregateRepo.GetWorkflowInstanceWithTasks(ctx, engineInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task instances: %w", err)
	}

	// Convert to workflow.TaskInstance (which is a type alias for taskenginestorage.TaskInstance)
	// taskInstances is []*taskenginestorage.TaskInstance (slice of pointers)
	// workflow.TaskInstance is a type alias, so we can directly convert
	result := make([]*workflow.TaskInstance, len(taskInstances))
	for i := range taskInstances {
		// taskInstances[i] is *taskenginestorage.TaskInstance
		// workflow.TaskInstance is a type alias, so we can directly convert
		result[i] = (*workflow.TaskInstance)(taskInstances[i])
	}
	return result, nil
}

// RetryTask retries a failed task instance.
// Note: Task Engine may not have a direct retry API, so this may need to be implemented
// through other means (e.g., re-submitting the workflow or specific task).
func (a *TaskEngineAdapterImpl) RetryTask(ctx context.Context, taskInstanceID string) error {
	// TODO: Check if Task Engine provides a RetryTask API
	// For now, return an error indicating it's not implemented
	return fmt.Errorf("task retry not yet implemented in Task Engine")
}

// SubmitDynamicWorkflow submits a dynamically built workflow to Task Engine.
// Unlike SubmitWorkflow, this method accepts a raw workflow object (from Task Engine)
// without requiring a WorkflowDefinition. Use this for workflows that are built
// at execution time (e.g., BatchDataSync with variable API lists).
func (a *TaskEngineAdapterImpl) SubmitDynamicWorkflow(ctx context.Context, wf *workflow.Workflow) (string, error) {
	if wf == nil {
		return "", fmt.Errorf("workflow is nil")
	}

	// Submit the workflow directly to Task Engine
	// The workflow must have all tasks already built with concrete parameters
	controller, err := a.engine.SubmitWorkflow(ctx, wf)
	if err != nil {
		return "", fmt.Errorf("failed to submit dynamic workflow: %w", err)
	}

	return controller.GetInstanceID(), nil
}

// GetFunctionRegistry returns the Task Engine function registry.
// This is needed for dynamically building workflows at execution time.
func (a *TaskEngineAdapterImpl) GetFunctionRegistry() interface{} {
	return a.engine.GetRegistry()
}
