package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

func TestNewWorkflowDefinition(t *testing.T) {
	wf := workflow.NewWorkflowDefinition("Daily Sync", "每日同步工作流", workflow.WfCategorySync, "name: test\ntasks: []", false)

	if wf.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if wf.Name != "Daily Sync" {
		t.Errorf("Name = %s, expected Daily Sync", wf.Name)
	}
	if wf.Category != workflow.WfCategorySync {
		t.Errorf("Category = %s, expected sync", wf.Category)
	}
	if wf.Version != 1 {
		t.Errorf("Version = %d, expected 1", wf.Version)
	}
	if wf.Status != workflow.WfDefStatusEnabled {
		t.Errorf("Status = %s, expected enabled", wf.Status)
	}
	if wf.IsSystem {
		t.Error("IsSystem should be false")
	}
}

func TestWorkflowDefinition_EnableDisable(t *testing.T) {
	wf := workflow.NewWorkflowDefinition("Test", "", workflow.WfCategoryCustom, "", false)

	wf.Disable()
	if wf.Status != workflow.WfDefStatusDisabled {
		t.Errorf("Status after Disable = %s, expected disabled", wf.Status)
	}
	if wf.IsEnabled() {
		t.Error("IsEnabled should return false")
	}

	wf.Enable()
	if wf.Status != workflow.WfDefStatusEnabled {
		t.Errorf("Status after Enable = %s, expected enabled", wf.Status)
	}
	if !wf.IsEnabled() {
		t.Error("IsEnabled should return true")
	}
}

func TestWorkflowDefinition_UpdateDefinition(t *testing.T) {
	wf := workflow.NewWorkflowDefinition("Test", "", workflow.WfCategoryCustom, "old: yaml", false)
	originalVersion := wf.Version

	wf.UpdateDefinition("new: yaml")

	if wf.DefinitionYAML != "new: yaml" {
		t.Errorf("DefinitionYAML = %s, expected new: yaml", wf.DefinitionYAML)
	}
	if wf.Version != originalVersion+1 {
		t.Errorf("Version = %d, expected %d", wf.Version, originalVersion+1)
	}
}

func TestWorkflowDefinition_CanCreateInstance(t *testing.T) {
	wf := workflow.NewWorkflowDefinition("Test", "", workflow.WfCategoryCustom, "", false)

	// Enabled workflow can create instance
	if err := wf.CanCreateInstance(); err != nil {
		t.Errorf("CanCreateInstance should return nil for enabled workflow, got %v", err)
	}

	// Disabled workflow cannot create instance
	wf.Disable()
	if err := wf.CanCreateInstance(); err == nil {
		t.Error("CanCreateInstance should return error for disabled workflow")
	}
}

func TestNewWorkflowInstance(t *testing.T) {
	wfDefID := shared.NewID()
	params := map[string]interface{}{"key": "value"}

	inst := workflow.NewWorkflowInstance(wfDefID, "engine-123", workflow.TriggerTypeManual, params)

	if inst.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if inst.WorkflowDefID != wfDefID {
		t.Error("WorkflowDefID mismatch")
	}
	if inst.EngineInstanceID != "engine-123" {
		t.Errorf("EngineInstanceID = %s, expected engine-123", inst.EngineInstanceID)
	}
	if inst.TriggerType != workflow.TriggerTypeManual {
		t.Errorf("TriggerType = %s, expected manual", inst.TriggerType)
	}
	if inst.Status != workflow.WfInstStatusPending {
		t.Errorf("Status = %s, expected pending", inst.Status)
	}
	if inst.Progress != 0.0 {
		t.Errorf("Progress = %f, expected 0.0", inst.Progress)
	}
}

func TestWorkflowInstance_StatusTransitions(t *testing.T) {
	inst := workflow.NewWorkflowInstance(shared.NewID(), "engine-1", workflow.TriggerTypeManual, nil)

	// Running
	inst.MarkRunning()
	if inst.Status != workflow.WfInstStatusRunning {
		t.Errorf("Status = %s, expected running", inst.Status)
	}

	// Paused
	inst.MarkPaused()
	if inst.Status != workflow.WfInstStatusPaused {
		t.Errorf("Status = %s, expected paused", inst.Status)
	}

	// Resume to running
	inst.MarkRunning()

	// Success
	inst.MarkSuccess()
	if inst.Status != workflow.WfInstStatusSuccess {
		t.Errorf("Status = %s, expected success", inst.Status)
	}
	if inst.Progress != 100.0 {
		t.Errorf("Progress = %f, expected 100.0", inst.Progress)
	}
	if inst.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestWorkflowInstance_MarkFailed(t *testing.T) {
	inst := workflow.NewWorkflowInstance(shared.NewID(), "engine-1", workflow.TriggerTypeManual, nil)
	inst.MarkRunning()

	inst.MarkFailed("task timeout")

	if inst.Status != workflow.WfInstStatusFailed {
		t.Errorf("Status = %s, expected failed", inst.Status)
	}
	if inst.ErrorMessage == nil {
		t.Fatal("ErrorMessage should not be nil")
	}
	if *inst.ErrorMessage != "task timeout" {
		t.Errorf("ErrorMessage = %s, expected task timeout", *inst.ErrorMessage)
	}
	if inst.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestWorkflowInstance_MarkCancelled(t *testing.T) {
	inst := workflow.NewWorkflowInstance(shared.NewID(), "engine-1", workflow.TriggerTypeManual, nil)
	inst.MarkRunning()

	inst.MarkCancelled()

	if inst.Status != workflow.WfInstStatusCancelled {
		t.Errorf("Status = %s, expected cancelled", inst.Status)
	}
	if inst.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestWorkflowInstance_UpdateProgress(t *testing.T) {
	inst := workflow.NewWorkflowInstance(shared.NewID(), "engine-1", workflow.TriggerTypeManual, nil)
	inst.MarkRunning()

	inst.UpdateProgress(50.0)

	if inst.Progress != 50.0 {
		t.Errorf("Progress = %f, expected 50.0", inst.Progress)
	}
}

func TestWorkflowInstance_JSONMarshaling(t *testing.T) {
	inst := workflow.NewWorkflowInstance(shared.NewID(), "engine-1", workflow.TriggerTypeManual, 
		map[string]interface{}{"param": "value"})

	paramsJSON, err := inst.MarshalTriggerParamsJSON()
	if err != nil {
		t.Fatalf("MarshalTriggerParamsJSON error: %v", err)
	}

	inst2 := workflow.NewWorkflowInstance(shared.NewID(), "engine-2", workflow.TriggerTypeCron, nil)
	if err := inst2.UnmarshalTriggerParamsJSON(paramsJSON); err != nil {
		t.Fatalf("UnmarshalTriggerParamsJSON error: %v", err)
	}

	if len(inst2.TriggerParams) != 1 {
		t.Errorf("Unmarshaled TriggerParams length = %d, expected 1", len(inst2.TriggerParams))
	}
}

func TestNewTaskInstance(t *testing.T) {
	wfInstID := shared.NewID()

	task := workflow.NewTaskInstance(wfInstID, "fetch_data")

	if task.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if task.WorkflowInstID != wfInstID {
		t.Error("WorkflowInstID mismatch")
	}
	if task.TaskName != "fetch_data" {
		t.Errorf("TaskName = %s, expected fetch_data", task.TaskName)
	}
	if task.Status != workflow.TaskStatusPending {
		t.Errorf("Status = %s, expected pending", task.Status)
	}
	if task.RetryCount != 0 {
		t.Errorf("RetryCount = %d, expected 0", task.RetryCount)
	}
}

func TestTaskInstance_StatusTransitions(t *testing.T) {
	task := workflow.NewTaskInstance(shared.NewID(), "task1")

	// Running
	task.MarkRunning()
	if task.Status != workflow.TaskStatusRunning {
		t.Errorf("Status = %s, expected running", task.Status)
	}
	if task.StartedAt == nil {
		t.Error("StartedAt should not be nil")
	}

	// Success
	task.MarkSuccess()
	if task.Status != workflow.TaskStatusSuccess {
		t.Errorf("Status = %s, expected success", task.Status)
	}
	if task.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestTaskInstance_MarkFailed(t *testing.T) {
	task := workflow.NewTaskInstance(shared.NewID(), "task1")
	task.MarkRunning()

	task.MarkFailed("execution error")

	if task.Status != workflow.TaskStatusFailed {
		t.Errorf("Status = %s, expected failed", task.Status)
	}
	if task.ErrorMessage == nil {
		t.Fatal("ErrorMessage should not be nil")
	}
	if *task.ErrorMessage != "execution error" {
		t.Errorf("ErrorMessage = %s, expected execution error", *task.ErrorMessage)
	}
	if task.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestTaskInstance_MarkSkipped(t *testing.T) {
	task := workflow.NewTaskInstance(shared.NewID(), "task1")

	task.MarkSkipped()

	if task.Status != workflow.TaskStatusSkipped {
		t.Errorf("Status = %s, expected skipped", task.Status)
	}
}

func TestTaskInstance_IncrementRetryCount(t *testing.T) {
	task := workflow.NewTaskInstance(shared.NewID(), "task1")

	task.IncrementRetryCount()
	if task.RetryCount != 1 {
		t.Errorf("RetryCount = %d, expected 1", task.RetryCount)
	}

	task.IncrementRetryCount()
	if task.RetryCount != 2 {
		t.Errorf("RetryCount = %d, expected 2", task.RetryCount)
	}
}

func TestEnums_String(t *testing.T) {
	t.Run("WfCategory", func(t *testing.T) {
		if workflow.WfCategoryMetadata.String() != "metadata" {
			t.Error("Metadata String mismatch")
		}
		if workflow.WfCategorySync.String() != "sync" {
			t.Error("Sync String mismatch")
		}
		if workflow.WfCategoryCustom.String() != "custom" {
			t.Error("Custom String mismatch")
		}
	})

	t.Run("WfDefStatus", func(t *testing.T) {
		if workflow.WfDefStatusEnabled.String() != "enabled" {
			t.Error("Enabled String mismatch")
		}
		if workflow.WfDefStatusDisabled.String() != "disabled" {
			t.Error("Disabled String mismatch")
		}
	})

	t.Run("TriggerType", func(t *testing.T) {
		if workflow.TriggerTypeManual.String() != "manual" {
			t.Error("Manual String mismatch")
		}
		if workflow.TriggerTypeCron.String() != "cron" {
			t.Error("Cron String mismatch")
		}
		if workflow.TriggerTypeEvent.String() != "event" {
			t.Error("Event String mismatch")
		}
	})

	t.Run("WfInstStatus", func(t *testing.T) {
		if workflow.WfInstStatusPending.String() != "pending" {
			t.Error("Pending String mismatch")
		}
		if workflow.WfInstStatusRunning.String() != "running" {
			t.Error("Running String mismatch")
		}
		if workflow.WfInstStatusPaused.String() != "paused" {
			t.Error("Paused String mismatch")
		}
		if workflow.WfInstStatusSuccess.String() != "success" {
			t.Error("Success String mismatch")
		}
		if workflow.WfInstStatusFailed.String() != "failed" {
			t.Error("Failed String mismatch")
		}
		if workflow.WfInstStatusCancelled.String() != "cancelled" {
			t.Error("Cancelled String mismatch")
		}
	})

	t.Run("TaskStatus", func(t *testing.T) {
		if workflow.TaskStatusPending.String() != "pending" {
			t.Error("Pending String mismatch")
		}
		if workflow.TaskStatusRunning.String() != "running" {
			t.Error("Running String mismatch")
		}
		if workflow.TaskStatusSuccess.String() != "success" {
			t.Error("Success String mismatch")
		}
		if workflow.TaskStatusFailed.String() != "failed" {
			t.Error("Failed String mismatch")
		}
		if workflow.TaskStatusSkipped.String() != "skipped" {
			t.Error("Skipped String mismatch")
		}
	})
}

func TestWorkflowStatus_Struct(t *testing.T) {
	now := shared.Now()
	errMsg := "test error"

	status := workflow.WorkflowStatus{
		InstanceID:    shared.NewID(),
		Status:        workflow.WfInstStatusFailed,
		Progress:      50.0,
		TaskCount:     10,
		CompletedTask: 5,
		FailedTask:    1,
		StartedAt:     now,
		FinishedAt:    &now,
		ErrorMessage:  &errMsg,
	}

	if status.TaskCount != 10 {
		t.Errorf("TaskCount = %d, expected 10", status.TaskCount)
	}
	if status.CompletedTask != 5 {
		t.Errorf("CompletedTask = %d, expected 5", status.CompletedTask)
	}

	// Test JSON marshaling
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var unmarshaled workflow.WorkflowStatus
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if unmarshaled.Progress != status.Progress {
		t.Errorf("Progress mismatch")
	}
}

func TestTaskInstance_Timing(t *testing.T) {
	task := workflow.NewTaskInstance(shared.NewID(), "task1")

	// Before running, times should be nil
	if task.StartedAt != nil {
		t.Error("StartedAt should be nil before running")
	}
	if task.FinishedAt != nil {
		t.Error("FinishedAt should be nil before running")
	}

	// After running, StartedAt should be set
	task.MarkRunning()
	if task.StartedAt == nil {
		t.Error("StartedAt should not be nil after running")
	}

	// Record start time
	startTime := task.StartedAt

	// Small delay
	time.Sleep(1 * time.Millisecond)

	// After success, FinishedAt should be set and after StartedAt
	task.MarkSuccess()
	if task.FinishedAt == nil {
		t.Error("FinishedAt should not be nil after success")
	}
	if !task.FinishedAt.After(*startTime) {
		t.Error("FinishedAt should be after StartedAt")
	}
}
