// Package taskengine provides Task Engine integration for QDHub.
package taskengine

import (
	"context"
	"fmt"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage"
	"github.com/sirupsen/logrus"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// TaskEngineAdapterImpl implements workflow.TaskEngineAdapter.
type TaskEngineAdapterImpl struct {
	engine                   *engine.Engine
	defaultMaxConcurrentTask int // 每个工作流实例的最大并发任务数，注册时写入 workflow；0 表示使用 100
}

// NewTaskEngineAdapter creates a new TaskEngineAdapter implementation.
// defaultMaxConcurrentTask 为每个工作流实例的 MaxConcurrentTask，若 <=0 则使用 100（与 task-engine V3 默认 10 区分，提高同步吞吐）。
func NewTaskEngineAdapter(eng *engine.Engine, defaultMaxConcurrentTask int) workflow.TaskEngineAdapter {
	if defaultMaxConcurrentTask <= 0 {
		defaultMaxConcurrentTask = 100
	}
	return &TaskEngineAdapterImpl{
		engine:                   eng,
		defaultMaxConcurrentTask: defaultMaxConcurrentTask,
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
// Prefers engine.GetInstanceProgress (in-memory snapshot) when the instance is running; otherwise uses storage.
func (a *TaskEngineAdapterImpl) GetInstanceStatus(ctx context.Context, engineInstanceID string) (*workflow.WorkflowStatus, error) {
	// Get workflow instance from aggregate repo (for StartedAt, EndTime, ErrorMessage, ID)
	aggregateRepo := a.engine.GetAggregateRepo()
	instance, taskInstances, err := aggregateRepo.GetWorkflowInstanceWithTasks(ctx, engineInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if instance == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "workflow instance not found", nil)
	}

	statusStr, err := a.engine.GetWorkflowInstanceStatus(ctx, engineInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance status: %w", err)
	}

	var progress float64
	var finalStatus string
	var completedTasks, failedTasks, taskCount int
	var snapshotRunning, snapshotPending int
	var hasSnapshot bool

	// Prefer in-memory progress from engine/manager when instance is running (handles dynamic workflows)
	if snapshot, ok := a.engine.GetInstanceProgress(engineInstanceID); ok {
		hasSnapshot = true
		snapshotRunning = snapshot.Running
		snapshotPending = snapshot.Pending
		taskCount = snapshot.Total
		completedTasks = snapshot.Completed
		failedTasks = snapshot.Failed
		if taskCount > 0 {
			progress = float64(completedTasks+failedTasks) / float64(taskCount) * 100
		}
		done := completedTasks + failedTasks
		if taskCount > 0 && done == taskCount {
			if failedTasks > 0 {
				finalStatus = "Failed"
			} else {
				finalStatus = "Success"
			}
			progress = 100.0
		} else if snapshot.Running > 0 || snapshot.Pending > 0 {
			finalStatus = "Running"
		} else {
			finalStatus = statusStr
		}
		if workflow.IsTerminal(statusStr) || workflow.IsPaused(statusStr) {
			finalStatus = statusStr
			progress = 100.0
		}
	} else {
		for _, task := range taskInstances {
			if workflow.IsSuccess(task.Status) || workflow.IsSkipped(task.Status) {
				completedTasks++
			} else if workflow.IsFailed(task.Status) {
				failedTasks++
			}
		}
		taskCount = len(taskInstances)
		finalStatus = statusStr

		if taskCount > 0 {
			progress = float64(completedTasks+failedTasks) / float64(taskCount) * 100
			if completedTasks+failedTasks == taskCount {
				if failedTasks > 0 {
					finalStatus = "Failed"
				} else {
					finalStatus = "Success"
				}
			}
			if workflow.IsTerminal(statusStr) {
				progress = 100.0
			}
		} else {
			if workflow.IsSuccess(instance.Status) {
				finalStatus = "Success"
				progress = 100.0
			} else if workflow.IsFailed(instance.Status) {
				finalStatus = "Failed"
				progress = 100.0
			} else if workflow.IsTerminated(instance.Status) {
				finalStatus = "Terminated"
				progress = 100.0
			} else if workflow.IsRunning(instance.Status) || workflow.IsPending(instance.Status) {
				finalStatus = "Running"
				elapsed := time.Since(instance.StartTime)
				progressPct := elapsed.Seconds() / 60 * 1.5
				if progressPct > 95 {
					progressPct = 95
				}
				progress = progressPct
			} else {
				finalStatus = statusStr
			}
		}
	}

	status := &workflow.WorkflowStatus{
		InstanceID:    instance.ID,
		Status:        finalStatus,
		Progress:      progress,
		TaskCount:     taskCount,
		CompletedTask: completedTasks,
		FailedTask:    failedTasks,
		StartedAt:     shared.Timestamp(instance.StartTime),
	}

	// 从 taskInstances（存储）收集正在运行和挂起的任务 ID。
	// 注意：进度来自引擎内存快照，会实时更新；running/pending IDs 来自存储，Task Engine 可能
	// 未在任务开始时写入 Running、或未在任务完成时及时更新存储，导致 running_ids 常为空、pending_ids 不随完成而减少。
	var runningIDs, pendingIDs []string
	for _, task := range taskInstances {
		if workflow.IsRunning(task.Status) {
			runningIDs = append(runningIDs, task.ID)
		} else if workflow.IsPending(task.Status) {
			pendingIDs = append(pendingIDs, task.ID)
		}
	}
	if len(runningIDs) > 0 {
		status.RunningTaskIDs = runningIDs
	}
	if len(pendingIDs) > 0 {
		status.PendingTaskIDs = pendingIDs
	}

	// 优先使用引擎快照的 running/pending 计数，使 API 与内部状态一致；无快照时用存储的 ID 数量。
	// 当快照 Running=0 但存储有运行中任务时，用存储数量作为 fallback，避免“进度查询与内部 runningCount 不一致”。
	if hasSnapshot {
		status.RunningCount = snapshotRunning
		status.PendingCount = snapshotPending
		if snapshotRunning == 0 && len(runningIDs) > 0 {
			status.RunningCount = len(runningIDs)
		}
		if snapshotPending == 0 && len(pendingIDs) > 0 {
			status.PendingCount = len(pendingIDs)
		}
	} else {
		status.RunningCount = len(runningIDs)
		status.PendingCount = len(pendingIDs)
	}

	if instance.EndTime != nil && !instance.EndTime.IsZero() {
		ts := shared.Timestamp(*instance.EndTime)
		status.FinishedAt = &ts
	}

	if instance.ErrorMessage != "" {
		status.ErrorMessage = &instance.ErrorMessage
	}
	if workflow.IsFailed(status.Status) && (status.ErrorMessage == nil || *status.ErrorMessage == "") {
		if msg := firstFailedTaskErrorMessage(taskInstances); msg != "" {
			status.ErrorMessage = &msg
		}
	}

	// 获取任务进度时打印正在运行/挂起的任务数量；若有内存快照则同时打印引擎侧计数，便于对比存储滞后
	if workflow.IsRunning(status.Status) {
		runningCnt := len(status.RunningTaskIDs)
		pendingCnt := len(status.PendingTaskIDs)
		if snapshot, ok := a.engine.GetInstanceProgress(engineInstanceID); ok {
			logrus.Infof("[Progress] instanceID=%s progress=%.1f%% running=%d pending=%d (storage) | snapshot: running=%d pending=%d (running_task_ids=%v pending_task_ids=%v)",
				engineInstanceID, status.Progress, runningCnt, pendingCnt, snapshot.Running, snapshot.Pending, status.RunningTaskIDs, status.PendingTaskIDs)
		} else {
			logrus.Infof("[Progress] instanceID=%s progress=%.1f%% running=%d pending=%d (running_task_ids=%v pending_task_ids=%v)",
				engineInstanceID, status.Progress, runningCnt, pendingCnt, status.RunningTaskIDs, status.PendingTaskIDs)
		}
	}

	return status, nil
}

// firstFailedTaskErrorMessage 从任务列表中取第一个失败任务的错误信息，供 UI 展示。
func firstFailedTaskErrorMessage(taskInstances []*storage.TaskInstance) string {
	for _, t := range taskInstances {
		if t != nil && workflow.IsFailed(t.Status) && t.ErrorMessage != "" {
			return t.ErrorMessage
		}
	}
	return ""
}

// RegisterWorkflow registers a workflow definition with Task Engine.
func (a *TaskEngineAdapterImpl) RegisterWorkflow(ctx context.Context, definition *workflow.WorkflowDefinition) error {
	if definition == nil || definition.Workflow == nil {
		return fmt.Errorf("workflow definition is nil")
	}
	// 设置每个工作流实例的最大并发任务数，否则 task-engine V3 使用默认 10，导致“运行中: 10”
	if err := definition.Workflow.SetMaxConcurrentTask(a.defaultMaxConcurrentTask); err != nil {
		return fmt.Errorf("failed to set workflow max concurrent task: %w", err)
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
