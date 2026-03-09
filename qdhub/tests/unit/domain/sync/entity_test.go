package sync_test

import (
	"encoding/json"
	"testing"
	"time"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// ==================== SyncPlan Tests ====================

func TestNewSyncPlan(t *testing.T) {
	dataSourceID := shared.NewID()
	selectedAPIs := []string{"daily", "trade_cal", "stock_basic"}

	plan := sync.NewSyncPlan("Daily Sync Plan", "每日同步计划", dataSourceID, selectedAPIs)

	if plan.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if plan.Name != "Daily Sync Plan" {
		t.Errorf("Name = %s, expected Daily Sync Plan", plan.Name)
	}
	if plan.DataSourceID != dataSourceID {
		t.Error("DataSourceID mismatch")
	}
	if len(plan.SelectedAPIs) != 3 {
		t.Errorf("SelectedAPIs length = %d, expected 3", len(plan.SelectedAPIs))
	}
	if plan.Status != sync.PlanStatusDraft {
		t.Errorf("Status = %s, expected draft", plan.Status)
	}
}

func TestSyncPlan_SetDataStore(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})
	dataStoreID := shared.NewID()

	plan.SetDataStore(dataStoreID)

	if plan.DataStoreID != dataStoreID {
		t.Error("DataStoreID mismatch")
	}
}

func TestSyncPlan_SetExecutionGraph(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})

	graph := &sync.ExecutionGraph{
		Levels: [][]string{
			{"trade_cal", "stock_basic"},
			{"daily"},
		},
		TaskConfigs: map[string]*sync.TaskConfig{
			"daily": {
				APIName:      "daily",
				SyncMode:     sync.TaskSyncModeTemplate,
				Dependencies: []string{"FetchStockBasic"},
			},
		},
	}
	resolvedAPIs := []string{"trade_cal", "stock_basic", "daily"}

	plan.SetExecutionGraph(graph, resolvedAPIs)

	if plan.Status != sync.PlanStatusResolved {
		t.Errorf("Status = %s, expected resolved", plan.Status)
	}
	if plan.ExecutionGraph == nil {
		t.Error("ExecutionGraph should not be nil")
	}
	if len(plan.ResolvedAPIs) != 3 {
		t.Errorf("ResolvedAPIs length = %d, expected 3", len(plan.ResolvedAPIs))
	}
}

func TestSyncPlan_SetCronExpression(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})

	plan.SetCronExpression("0 0 9 * * *")

	if plan.CronExpression == nil {
		t.Fatal("CronExpression should not be nil")
	}
	if *plan.CronExpression != "0 0 9 * * *" {
		t.Errorf("CronExpression = %s, expected 0 0 9 * * *", *plan.CronExpression)
	}
}

func TestSyncPlan_SetScheduleWindow(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})

	plan.SetScheduleWindow("0 0 9 * * 1-5", "0 30 15 * * 1-5")
	if plan.ScheduleStartCron == nil || *plan.ScheduleStartCron != "0 0 9 * * 1-5" {
		t.Errorf("ScheduleStartCron = %v", plan.ScheduleStartCron)
	}
	if plan.ScheduleEndCron == nil || *plan.ScheduleEndCron != "0 30 15 * * 1-5" {
		t.Errorf("ScheduleEndCron = %v", plan.ScheduleEndCron)
	}

	plan.SetScheduleWindow("", "")
	if plan.ScheduleStartCron != nil || plan.ScheduleEndCron != nil {
		t.Error("empty strings should clear schedule window")
	}
}

func TestSyncPlan_SetPullIntervalSeconds(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})

	plan.SetPullIntervalSeconds(30)
	if plan.PullIntervalSeconds != 30 {
		t.Errorf("PullIntervalSeconds = %d, expected 30", plan.PullIntervalSeconds)
	}
	plan.SetPullIntervalSeconds(0)
	if plan.PullIntervalSeconds != 0 {
		t.Errorf("PullIntervalSeconds = %d, expected 0", plan.PullIntervalSeconds)
	}
}

func TestSyncPlan_EnableDisable(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})
	// Set to resolved first
	plan.SetExecutionGraph(&sync.ExecutionGraph{}, []string{"daily"})

	// Enable
	if err := plan.Enable(); err != nil {
		t.Fatalf("Enable error: %v", err)
	}
	if plan.Status != sync.PlanStatusEnabled {
		t.Errorf("Status after Enable = %s, expected enabled", plan.Status)
	}

	// Disable
	if err := plan.Disable(); err != nil {
		t.Fatalf("Disable error: %v", err)
	}
	if plan.Status != sync.PlanStatusDisabled {
		t.Errorf("Status after Disable = %s, expected disabled", plan.Status)
	}
}

func TestSyncPlan_EnableDraft_Error(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})

	// Cannot enable draft plan
	if err := plan.Enable(); err == nil {
		t.Error("Enable should fail for draft plan")
	}
}

func TestSyncPlan_EnableDisable_Running(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})
	plan.SetExecutionGraph(&sync.ExecutionGraph{}, []string{"daily"})
	plan.Enable()
	plan.MarkRunning()

	// Cannot enable running plan
	if err := plan.Enable(); err == nil {
		t.Error("Enable should fail for running plan")
	}

	// Cannot disable running plan
	if err := plan.Disable(); err == nil {
		t.Error("Disable should fail for running plan")
	}
}

func TestSyncPlan_MarkRunning(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})
	plan.SetExecutionGraph(&sync.ExecutionGraph{}, []string{"daily"})
	plan.Enable()

	plan.MarkRunning()

	if plan.Status != sync.PlanStatusRunning {
		t.Errorf("Status = %s, expected running", plan.Status)
	}
	if plan.LastExecutedAt == nil {
		t.Error("LastExecutedAt should not be nil")
	}
}

func TestSyncPlan_MarkCompleted(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})
	plan.SetExecutionGraph(&sync.ExecutionGraph{}, []string{"daily"})
	plan.Enable()
	plan.MarkRunning()

	nextRun := time.Now().Add(24 * time.Hour)
	plan.MarkCompleted(&nextRun)

	if plan.Status != sync.PlanStatusEnabled {
		t.Errorf("Status = %s, expected enabled", plan.Status)
	}
	if plan.NextExecuteAt == nil {
		t.Error("NextExecuteAt should not be nil")
	}
}

func TestSyncPlan_AddTask(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})
	task := sync.NewSyncTask("daily", sync.TaskSyncModeTemplate, 1)

	plan.AddTask(task)

	if len(plan.Tasks) != 1 {
		t.Errorf("Tasks length = %d, expected 1", len(plan.Tasks))
	}
	if task.SyncPlanID != plan.ID {
		t.Error("Task SyncPlanID should be set to plan ID")
	}
}

func TestSyncPlan_JSONMarshaling(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "Description", shared.NewID(), []string{"daily", "stock_basic"})
	plan.SetExecutionGraph(&sync.ExecutionGraph{
		Levels: [][]string{{"stock_basic"}, {"daily"}},
	}, []string{"stock_basic", "daily"})

	// Test SelectedAPIs JSON
	selectedJSON, err := plan.MarshalSelectedAPIsJSON()
	if err != nil {
		t.Fatalf("MarshalSelectedAPIsJSON error: %v", err)
	}

	plan2 := sync.NewSyncPlan("Test2", "", shared.NewID(), []string{})
	if err := plan2.UnmarshalSelectedAPIsJSON(selectedJSON); err != nil {
		t.Fatalf("UnmarshalSelectedAPIsJSON error: %v", err)
	}
	if len(plan2.SelectedAPIs) != 2 {
		t.Errorf("Unmarshaled SelectedAPIs length = %d, expected 2", len(plan2.SelectedAPIs))
	}

	// Test ResolvedAPIs JSON
	resolvedJSON, err := plan.MarshalResolvedAPIsJSON()
	if err != nil {
		t.Fatalf("MarshalResolvedAPIsJSON error: %v", err)
	}

	if err := plan2.UnmarshalResolvedAPIsJSON(resolvedJSON); err != nil {
		t.Fatalf("UnmarshalResolvedAPIsJSON error: %v", err)
	}
	if len(plan2.ResolvedAPIs) != 2 {
		t.Errorf("Unmarshaled ResolvedAPIs length = %d, expected 2", len(plan2.ResolvedAPIs))
	}

	// Test ExecutionGraph JSON
	graphJSON, err := plan.MarshalExecutionGraphJSON()
	if err != nil {
		t.Fatalf("MarshalExecutionGraphJSON error: %v", err)
	}

	if err := plan2.UnmarshalExecutionGraphJSON(graphJSON); err != nil {
		t.Fatalf("UnmarshalExecutionGraphJSON error: %v", err)
	}
	if plan2.ExecutionGraph == nil {
		t.Error("Unmarshaled ExecutionGraph should not be nil")
	}
	if len(plan2.ExecutionGraph.Levels) != 2 {
		t.Errorf("Unmarshaled Levels length = %d, expected 2", len(plan2.ExecutionGraph.Levels))
	}
}

// ==================== SyncTask Tests ====================

func TestNewSyncTask(t *testing.T) {
	task := sync.NewSyncTask("daily", sync.TaskSyncModeTemplate, 1)

	if task.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if task.APIName != "daily" {
		t.Errorf("APIName = %s, expected daily", task.APIName)
	}
	if task.SyncMode != sync.TaskSyncModeTemplate {
		t.Errorf("SyncMode = %s, expected template", task.SyncMode)
	}
	if task.Level != 1 {
		t.Errorf("Level = %d, expected 1", task.Level)
	}
	if task.SyncFrequency != sync.SyncFrequencyAlways {
		t.Errorf("SyncFrequency = %v, expected 0", task.SyncFrequency)
	}
}

func TestSyncTask_NeedsSync(t *testing.T) {
	t.Run("AlwaysSync", func(t *testing.T) {
		task := sync.NewSyncTask("daily", sync.TaskSyncModeDirect, 0)
		task.SyncFrequency = sync.SyncFrequencyAlways

		if !task.NeedsSync() {
			t.Error("Task with SyncFrequencyAlways should always need sync")
		}
	})

	t.Run("NeverSyncedBefore", func(t *testing.T) {
		task := sync.NewSyncTask("trade_cal", sync.TaskSyncModeDirect, 0)
		task.SyncFrequency = sync.SyncFrequencyMonthly

		if !task.NeedsSync() {
			t.Error("Task never synced before should need sync")
		}
	})

	t.Run("RecentlySynced", func(t *testing.T) {
		task := sync.NewSyncTask("stock_basic", sync.TaskSyncModeDirect, 0)
		task.SyncFrequency = sync.SyncFrequencyWeekly
		recentTime := time.Now().Add(-1 * time.Hour)
		task.LastSyncedAt = &recentTime

		if task.NeedsSync() {
			t.Error("Recently synced task should not need sync")
		}
	})

	t.Run("SyncExpired", func(t *testing.T) {
		task := sync.NewSyncTask("stock_basic", sync.TaskSyncModeDirect, 0)
		task.SyncFrequency = sync.SyncFrequencyDaily
		oldTime := time.Now().Add(-48 * time.Hour)
		task.LastSyncedAt = &oldTime

		if !task.NeedsSync() {
			t.Error("Expired sync task should need sync")
		}
	})

	t.Run("SyncOnce_AlreadySynced", func(t *testing.T) {
		task := sync.NewSyncTask("static_data", sync.TaskSyncModeDirect, 0)
		task.SyncFrequency = sync.SyncFrequencyOnce
		syncTime := time.Now().Add(-30 * 24 * time.Hour)
		task.LastSyncedAt = &syncTime

		if task.NeedsSync() {
			t.Error("Task with SyncFrequencyOnce that was synced should not need sync")
		}
	})
}

func TestSyncTask_MarkSynced(t *testing.T) {
	task := sync.NewSyncTask("daily", sync.TaskSyncModeDirect, 0)

	if task.LastSyncedAt != nil {
		t.Error("LastSyncedAt should be nil initially")
	}

	task.MarkSynced()

	if task.LastSyncedAt == nil {
		t.Error("LastSyncedAt should not be nil after MarkSynced")
	}
}

func TestSyncTask_SetMethods(t *testing.T) {
	task := sync.NewSyncTask("daily", sync.TaskSyncModeTemplate, 1)

	// SetParamMappings
	mappings := []sync.ParamMapping{
		{ParamName: "ts_code", SourceTask: "FetchStockBasic", SourceField: "ts_code", IsList: true},
	}
	task.SetParamMappings(mappings)
	if len(task.ParamMappings) != 1 {
		t.Errorf("ParamMappings length = %d, expected 1", len(task.ParamMappings))
	}

	// SetDependencies
	deps := []string{"FetchTradeCal", "FetchStockBasic"}
	task.SetDependencies(deps)
	if len(task.Dependencies) != 2 {
		t.Errorf("Dependencies length = %d, expected 2", len(task.Dependencies))
	}

	// SetSyncFrequency
	task.SetSyncFrequency(sync.SyncFrequencyWeekly)
	if task.SyncFrequency != sync.SyncFrequencyWeekly {
		t.Errorf("SyncFrequency = %v, expected %v", task.SyncFrequency, sync.SyncFrequencyWeekly)
	}
}

func TestSyncTask_JSONMarshaling(t *testing.T) {
	task := sync.NewSyncTask("daily", sync.TaskSyncModeTemplate, 1)
	task.Params = map[string]interface{}{"limit": 1000}
	task.SetParamMappings([]sync.ParamMapping{
		{ParamName: "ts_code", SourceTask: "FetchStockBasic", SourceField: "ts_code"},
	})
	task.SetDependencies([]string{"FetchStockBasic"})

	// Test Params JSON
	paramsJSON, err := task.MarshalParamsJSON()
	if err != nil {
		t.Fatalf("MarshalParamsJSON error: %v", err)
	}

	task2 := sync.NewSyncTask("test", sync.TaskSyncModeDirect, 0)
	if err := task2.UnmarshalParamsJSON(paramsJSON); err != nil {
		t.Fatalf("UnmarshalParamsJSON error: %v", err)
	}
	if len(task2.Params) != 1 {
		t.Errorf("Unmarshaled Params length = %d, expected 1", len(task2.Params))
	}

	// Test ParamMappings JSON
	mappingsJSON, err := task.MarshalParamMappingsJSON()
	if err != nil {
		t.Fatalf("MarshalParamMappingsJSON error: %v", err)
	}

	if err := task2.UnmarshalParamMappingsJSON(mappingsJSON); err != nil {
		t.Fatalf("UnmarshalParamMappingsJSON error: %v", err)
	}
	if len(task2.ParamMappings) != 1 {
		t.Errorf("Unmarshaled ParamMappings length = %d, expected 1", len(task2.ParamMappings))
	}

	// Test Dependencies JSON
	depsJSON, err := task.MarshalDependenciesJSON()
	if err != nil {
		t.Fatalf("MarshalDependenciesJSON error: %v", err)
	}

	if err := task2.UnmarshalDependenciesJSON(depsJSON); err != nil {
		t.Fatalf("UnmarshalDependenciesJSON error: %v", err)
	}
	if len(task2.Dependencies) != 1 {
		t.Errorf("Unmarshaled Dependencies length = %d, expected 1", len(task2.Dependencies))
	}
}

// ==================== SyncExecution Tests ====================

func TestNewSyncExecution(t *testing.T) {
	planID := shared.NewID()
	wfInstID := shared.NewID()

	exec := sync.NewSyncExecution(planID, wfInstID)

	if exec.ID.IsEmpty() {
		t.Error("ID should not be empty")
	}
	if exec.SyncPlanID != planID {
		t.Error("SyncPlanID mismatch")
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

func TestSyncExecution_JSONMarshaling(t *testing.T) {
	exec := sync.NewSyncExecution(shared.NewID(), shared.NewID())
	exec.ExecuteParams = &sync.ExecuteParams{
		TargetDBPath: "/data/test.db",
		StartDate:    "20240101",
		EndDate:      "20240131",
	}
	exec.SyncedAPIs = []string{"daily", "stock_basic"}
	exec.SkippedAPIs = []string{"trade_cal"}

	// Test ExecuteParams JSON
	paramsJSON, err := exec.MarshalExecuteParamsJSON()
	if err != nil {
		t.Fatalf("MarshalExecuteParamsJSON error: %v", err)
	}

	exec2 := sync.NewSyncExecution(shared.NewID(), shared.NewID())
	if err := exec2.UnmarshalExecuteParamsJSON(paramsJSON); err != nil {
		t.Fatalf("UnmarshalExecuteParamsJSON error: %v", err)
	}
	if exec2.ExecuteParams == nil {
		t.Fatal("ExecuteParams should not be nil")
	}
	if exec2.ExecuteParams.TargetDBPath != "/data/test.db" {
		t.Errorf("TargetDBPath = %s, expected /data/test.db", exec2.ExecuteParams.TargetDBPath)
	}

	// Test SyncedAPIs JSON
	syncedJSON, err := exec.MarshalSyncedAPIsJSON()
	if err != nil {
		t.Fatalf("MarshalSyncedAPIsJSON error: %v", err)
	}

	if err := exec2.UnmarshalSyncedAPIsJSON(syncedJSON); err != nil {
		t.Fatalf("UnmarshalSyncedAPIsJSON error: %v", err)
	}
	if len(exec2.SyncedAPIs) != 2 {
		t.Errorf("SyncedAPIs length = %d, expected 2", len(exec2.SyncedAPIs))
	}

	// Test SkippedAPIs JSON
	skippedJSON, err := exec.MarshalSkippedAPIsJSON()
	if err != nil {
		t.Fatalf("MarshalSkippedAPIsJSON error: %v", err)
	}

	if err := exec2.UnmarshalSkippedAPIsJSON(skippedJSON); err != nil {
		t.Fatalf("UnmarshalSkippedAPIsJSON error: %v", err)
	}
	if len(exec2.SkippedAPIs) != 1 {
		t.Errorf("SkippedAPIs length = %d, expected 1", len(exec2.SkippedAPIs))
	}
}

// ==================== Enums Tests ====================

func TestEnums_String(t *testing.T) {
	t.Run("TaskSyncMode", func(t *testing.T) {
		if sync.TaskSyncModeDirect.String() != "direct" {
			t.Error("Direct String mismatch")
		}
		if sync.TaskSyncModeTemplate.String() != "template" {
			t.Error("Template String mismatch")
		}
	})

	t.Run("PlanStatus", func(t *testing.T) {
		if sync.PlanStatusDraft.String() != "draft" {
			t.Error("Draft String mismatch")
		}
		if sync.PlanStatusResolved.String() != "resolved" {
			t.Error("Resolved String mismatch")
		}
		if sync.PlanStatusEnabled.String() != "enabled" {
			t.Error("Enabled String mismatch")
		}
		if sync.PlanStatusDisabled.String() != "disabled" {
			t.Error("Disabled String mismatch")
		}
		if sync.PlanStatusRunning.String() != "running" {
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

// ==================== Value Objects Tests ====================

func TestParamMapping_JSON(t *testing.T) {
	mapping := sync.ParamMapping{
		ParamName:   "ts_code",
		SourceTask:  "FetchStockBasic",
		SourceField: "ts_code",
		IsList:      true,
		Select:      "all",
		FilterField: "list_status",
		FilterValue: "L",
	}

	data, err := json.Marshal(mapping)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var unmarshaled sync.ParamMapping
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if unmarshaled.ParamName != mapping.ParamName {
		t.Errorf("ParamName mismatch")
	}
	if unmarshaled.SourceTask != mapping.SourceTask {
		t.Errorf("SourceTask mismatch")
	}
	if unmarshaled.IsList != mapping.IsList {
		t.Errorf("IsList mismatch")
	}
}

func TestTaskConfig_JSON(t *testing.T) {
	config := sync.TaskConfig{
		APIName:      "daily",
		SyncMode:     sync.TaskSyncModeTemplate,
		Dependencies: []string{"FetchStockBasic"},
		ParamMappings: []sync.ParamMapping{
			{ParamName: "ts_code", SourceTask: "FetchStockBasic", SourceField: "ts_code"},
		},
	}

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var unmarshaled sync.TaskConfig
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if unmarshaled.APIName != config.APIName {
		t.Errorf("APIName mismatch")
	}
	if unmarshaled.SyncMode != config.SyncMode {
		t.Errorf("SyncMode mismatch")
	}
	if len(unmarshaled.Dependencies) != 1 {
		t.Errorf("Dependencies length = %d, expected 1", len(unmarshaled.Dependencies))
	}
}

func TestExecutionGraph_JSON(t *testing.T) {
	graph := sync.ExecutionGraph{
		Levels: [][]string{
			{"trade_cal", "stock_basic"},
			{"daily"},
		},
		MissingAPIs: []string{},
		TaskConfigs: map[string]*sync.TaskConfig{
			"daily": {
				APIName:  "daily",
				SyncMode: sync.TaskSyncModeTemplate,
			},
		},
	}

	data, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var unmarshaled sync.ExecutionGraph
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if len(unmarshaled.Levels) != 2 {
		t.Errorf("Levels length = %d, expected 2", len(unmarshaled.Levels))
	}
	if len(unmarshaled.TaskConfigs) != 1 {
		t.Errorf("TaskConfigs length = %d, expected 1", len(unmarshaled.TaskConfigs))
	}
}

func TestExecuteParams_JSON(t *testing.T) {
	params := sync.ExecuteParams{
		TargetDBPath: "/data/test.db",
		StartDate:    "20240101",
		EndDate:      "20240131",
		StartTime:    "09:30:00",
		EndTime:      "15:00:00",
	}

	data, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var unmarshaled sync.ExecuteParams
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if unmarshaled.TargetDBPath != params.TargetDBPath {
		t.Errorf("TargetDBPath mismatch")
	}
	if unmarshaled.StartDate != params.StartDate {
		t.Errorf("StartDate mismatch")
	}
}

// ==================== Sync Frequency Constants Tests ====================

func TestSyncFrequencyConstants(t *testing.T) {
	if sync.SyncFrequencyDaily != 24*time.Hour {
		t.Error("SyncFrequencyDaily should be 24h")
	}
	if sync.SyncFrequencyWeekly != 7*24*time.Hour {
		t.Error("SyncFrequencyWeekly should be 168h")
	}
	if sync.SyncFrequencyMonthly != 30*24*time.Hour {
		t.Error("SyncFrequencyMonthly should be 720h")
	}
	if sync.SyncFrequencyOnce != -1*time.Hour {
		t.Error("SyncFrequencyOnce should be -1h")
	}
	if sync.SyncFrequencyAlways != 0 {
		t.Error("SyncFrequencyAlways should be 0")
	}
}
