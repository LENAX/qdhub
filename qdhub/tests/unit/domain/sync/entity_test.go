package sync_test

import (
	"encoding/json"
	"testing"
	"time"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

func TestNewSyncJob(t *testing.T) {
	apiID := shared.NewID()
	dsID := shared.NewID()
	wfID := shared.NewID()

	job := sync.NewSyncJob("Daily Sync", "每日同步", apiID, dsID, wfID, sync.SyncModeBatch)

	if job.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if job.Name != "Daily Sync" {
		t.Errorf("Name = %s, expected Daily Sync", job.Name)
	}
	if job.Mode != sync.SyncModeBatch {
		t.Errorf("Mode = %s, expected batch", job.Mode)
	}
	if job.Status != sync.JobStatusDisabled {
		t.Errorf("Status = %s, expected disabled", job.Status)
	}
	if job.Params == nil {
		t.Error("Params should not be nil")
	}
}

func TestSyncJob_EnableDisable(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)

	// Enable
	if err := job.Enable(); err != nil {
		t.Fatalf("Enable error: %v", err)
	}
	if job.Status != sync.JobStatusEnabled {
		t.Errorf("Status after Enable = %s, expected enabled", job.Status)
	}

	// Disable
	if err := job.Disable(); err != nil {
		t.Fatalf("Disable error: %v", err)
	}
	if job.Status != sync.JobStatusDisabled {
		t.Errorf("Status after Disable = %s, expected disabled", job.Status)
	}
}

func TestSyncJob_EnableDisable_Running(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
	job.MarkRunning()

	// Cannot enable running job
	if err := job.Enable(); err == nil {
		t.Error("Enable should fail for running job")
	}

	// Cannot disable running job
	if err := job.Disable(); err == nil {
		t.Error("Disable should fail for running job")
	}
}

func TestSyncJob_MarkRunning(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
	job.Enable()

	job.MarkRunning()

	if job.Status != sync.JobStatusRunning {
		t.Errorf("Status = %s, expected running", job.Status)
	}
	if job.LastRunAt == nil {
		t.Error("LastRunAt should not be nil")
	}
}

func TestSyncJob_MarkCompleted(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
	job.MarkRunning()

	nextRun := time.Now().Add(24 * time.Hour)
	job.MarkCompleted(&nextRun)

	if job.Status != sync.JobStatusEnabled {
		t.Errorf("Status = %s, expected enabled", job.Status)
	}
	if job.NextRunAt == nil {
		t.Error("NextRunAt should not be nil")
	}
}

func TestSyncJob_SetCronExpression(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)

	job.SetCronExpression("0 0 9 * * *")

	if job.CronExpression == nil {
		t.Fatal("CronExpression should not be nil")
	}
	if *job.CronExpression != "0 0 9 * * *" {
		t.Errorf("CronExpression = %s, expected 0 0 9 * * *", *job.CronExpression)
	}
}

func TestSyncJob_SetParams(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)

	params := map[string]interface{}{
		"ts_code":    "000001.SZ",
		"start_date": "20240101",
	}
	job.SetParams(params)

	if len(job.Params) != 2 {
		t.Errorf("Params length = %d, expected 2", len(job.Params))
	}
}

func TestSyncJob_AddParamRule(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)

	job.AddParamRule(sync.ParamRule{ParamName: "ts_code", RuleType: "required"})
	job.AddParamRule(sync.ParamRule{ParamName: "trade_date", RuleType: "date_range"})

	if len(job.ParamRules) != 2 {
		t.Errorf("ParamRules length = %d, expected 2", len(job.ParamRules))
	}
}

func TestSyncJob_JSONMarshaling(t *testing.T) {
	job := sync.NewSyncJob("Test", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
	job.SetParams(map[string]interface{}{"key": "value"})
	job.AddParamRule(sync.ParamRule{ParamName: "ts_code", RuleType: "required"})

	// Test Params JSON
	paramsJSON, err := job.MarshalParamsJSON()
	if err != nil {
		t.Fatalf("MarshalParamsJSON error: %v", err)
	}

	job2 := sync.NewSyncJob("Test2", "", shared.NewID(), shared.NewID(), shared.NewID(), sync.SyncModeBatch)
	if err := job2.UnmarshalParamsJSON(paramsJSON); err != nil {
		t.Fatalf("UnmarshalParamsJSON error: %v", err)
	}
	if len(job2.Params) != 1 {
		t.Errorf("Unmarshaled Params length = %d, expected 1", len(job2.Params))
	}

	// Test ParamRules JSON
	rulesJSON, err := job.MarshalParamRulesJSON()
	if err != nil {
		t.Fatalf("MarshalParamRulesJSON error: %v", err)
	}

	if err := job2.UnmarshalParamRulesJSON(rulesJSON); err != nil {
		t.Fatalf("UnmarshalParamRulesJSON error: %v", err)
	}
	if len(job2.ParamRules) != 1 {
		t.Errorf("Unmarshaled ParamRules length = %d, expected 1", len(job2.ParamRules))
	}
}

func TestNewSyncExecution(t *testing.T) {
	jobID := shared.NewID()
	wfInstID := shared.NewID()

	exec := sync.NewSyncExecution(jobID, wfInstID)

	if exec.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if exec.SyncJobID != jobID {
		t.Error("SyncJobID mismatch")
	}
	if exec.WorkflowInstID != wfInstID {
		t.Error("WorkflowInstID mismatch")
	}
	if exec.Status != sync.ExecStatusPending {
		t.Errorf("Status = %s, expected pending", exec.Status)
	}
}

func TestSyncExecution_StatusTransitions(t *testing.T) {
	exec := sync.NewSyncExecution(shared.NewID(), shared.NewID())

	// Running
	exec.MarkRunning()
	if exec.Status != sync.ExecStatusRunning {
		t.Errorf("Status = %s, expected running", exec.Status)
	}

	// Success
	exec.MarkSuccess(1000)
	if exec.Status != sync.ExecStatusSuccess {
		t.Errorf("Status = %s, expected success", exec.Status)
	}
	if exec.RecordCount != 1000 {
		t.Errorf("RecordCount = %d, expected 1000", exec.RecordCount)
	}
	if exec.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestSyncExecution_MarkFailed(t *testing.T) {
	exec := sync.NewSyncExecution(shared.NewID(), shared.NewID())
	exec.MarkRunning()

	exec.MarkFailed("connection timeout")

	if exec.Status != sync.ExecStatusFailed {
		t.Errorf("Status = %s, expected failed", exec.Status)
	}
	if exec.ErrorMessage == nil {
		t.Fatal("ErrorMessage should not be nil")
	}
	if *exec.ErrorMessage != "connection timeout" {
		t.Errorf("ErrorMessage = %s, expected connection timeout", *exec.ErrorMessage)
	}
	if exec.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestSyncExecution_MarkCancelled(t *testing.T) {
	exec := sync.NewSyncExecution(shared.NewID(), shared.NewID())
	exec.MarkRunning()

	exec.MarkCancelled()

	if exec.Status != sync.ExecStatusCancelled {
		t.Errorf("Status = %s, expected cancelled", exec.Status)
	}
	if exec.FinishedAt == nil {
		t.Error("FinishedAt should not be nil")
	}
}

func TestEnums_String(t *testing.T) {
	t.Run("SyncMode", func(t *testing.T) {
		if sync.SyncModeBatch.String() != "batch" {
			t.Error("Batch String mismatch")
		}
		if sync.SyncModeRealtime.String() != "realtime" {
			t.Error("Realtime String mismatch")
		}
	})

	t.Run("JobStatus", func(t *testing.T) {
		if sync.JobStatusEnabled.String() != "enabled" {
			t.Error("Enabled String mismatch")
		}
		if sync.JobStatusDisabled.String() != "disabled" {
			t.Error("Disabled String mismatch")
		}
		if sync.JobStatusRunning.String() != "running" {
			t.Error("Running String mismatch")
		}
	})

	t.Run("ExecStatus", func(t *testing.T) {
		if sync.ExecStatusPending.String() != "pending" {
			t.Error("Pending String mismatch")
		}
		if sync.ExecStatusRunning.String() != "running" {
			t.Error("Running String mismatch")
		}
		if sync.ExecStatusSuccess.String() != "success" {
			t.Error("Success String mismatch")
		}
		if sync.ExecStatusFailed.String() != "failed" {
			t.Error("Failed String mismatch")
		}
		if sync.ExecStatusCancelled.String() != "cancelled" {
			t.Error("Cancelled String mismatch")
		}
	})
}

func TestParamRule_JSON(t *testing.T) {
	rule := sync.ParamRule{
		ParamName:  "trade_date",
		RuleType:   "date_range",
		RuleConfig: map[string]string{"format": "YYYYMMDD"},
	}

	data, err := json.Marshal(rule)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var unmarshaled sync.ParamRule
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if unmarshaled.ParamName != rule.ParamName {
		t.Errorf("ParamName mismatch")
	}
	if unmarshaled.RuleType != rule.RuleType {
		t.Errorf("RuleType mismatch")
	}
}
