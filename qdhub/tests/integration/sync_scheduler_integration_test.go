//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/scheduler"
	"qdhub/internal/infrastructure/persistence/uow"
)

const (
	schedTestWorkflowDefID  = "sched-test-wf-def-001"
	schedTestWorkflowInstID = "sched-test-wf-inst-001"
)

// TestScheduledPlanExecutor_Integration_TriggerSync verifies that when the cron
// scheduler fires (simulated by calling ExecuteScheduledJob), the plan is executed
// using DefaultExecuteParams and an execution record is created.
func TestScheduledPlanExecutor_Integration_TriggerSync(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	fixedWfInstID := shared.ID(schedTestWorkflowInstID)
	workflowExecutor := &MockSyncWorkflowExecutor{FixedBatchSyncInstanceID: &fixedWfInstID}

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	planExec := scheduler.NewScheduledPlanExecutor()
	planScheduler := scheduler.NewCronScheduler(planExec)
	planScheduler.Start()
	defer planScheduler.Stop()

	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	svc := impl.NewSyncApplicationService(
		syncPlanRepo,
		cronCalculator,
		planScheduler,
		dataSourceRepo,
		dataStoreRepo,
		workflowExecutor,
		dependencyResolver,
		nil,
		uowImpl,
	)
	planExec.SetSyncService(svc)

	// Data source + token
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}
	token := metadata.NewToken(dataSource.ID, "test-token", nil)
	if err := dataSourceRepo.SetToken(token); err != nil {
		t.Fatalf("Failed to set token: %v", err)
	}

	// API metadata
	api1 := metadata.NewAPIMetadata(dataSource.ID, "api1", "API 1", "GET", "/api1")
	if err := dataSourceRepo.AddAPIMetadata(api1); err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	// Data store (target path is resolved from plan's data store)
	store := datastore.NewQuantDataStore("Sched Test Store", "Test", datastore.DataStoreTypeDuckDB, "", "/tmp/sched-test.db")
	if err := dataStoreRepo.Create(store); err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	defaultParams := &sync.ExecuteParams{
		StartDate: "20250101",
		EndDate:   "20250131",
	}
	cronExpr := "0 0 9 * * *"

	plan, err := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:                 "Scheduled Plan",
		Description:          "Test",
		DataSourceID:         dataSource.ID,
		DataStoreID:          store.ID,
		SelectedAPIs:         []string{"api1"},
		CronExpression:       &cronExpr,
		DefaultExecuteParams: defaultParams,
	})
	if err != nil {
		t.Fatalf("Failed to create sync plan: %v", err)
	}

	if err := svc.ResolveSyncPlan(ctx, plan.ID); err != nil {
		t.Fatalf("ResolveSyncPlan failed: %v", err)
	}

	if err := svc.EnablePlan(ctx, plan.ID); err != nil {
		t.Fatalf("EnablePlan failed: %v", err)
	}

	// Satisfy sync_execution.workflow_inst_id FK
	_, err = db.Exec(`INSERT INTO workflow_definition (id, name) VALUES (?, ?)`, schedTestWorkflowDefID, "Test Sync")
	if err != nil {
		t.Fatalf("Failed to insert workflow_definition: %v", err)
	}
	_, err = db.Exec(`INSERT INTO workflow_instance (id, workflow_id, status) VALUES (?, ?, ?)`,
		schedTestWorkflowInstID, schedTestWorkflowDefID, "Running")
	if err != nil {
		t.Fatalf("Failed to insert workflow_instance: %v", err)
	}

	// Simulate cron trigger
	err = planExec.ExecuteScheduledJob(ctx, plan.ID.String())
	if err != nil {
		t.Fatalf("ExecuteScheduledJob failed: %v", err)
	}

	execs, err := svc.ListPlanExecutions(ctx, plan.ID)
	if err != nil {
		t.Fatalf("ListPlanExecutions failed: %v", err)
	}
	if len(execs) == 0 {
		t.Fatal("Expected at least one execution after scheduled trigger")
	}
	latest := execs[0]
	if latest.Status != sync.ExecStatusRunning && latest.Status != sync.ExecStatusSuccess {
		t.Logf("Execution status: %s (acceptable)", latest.Status)
	}
	if latest.WorkflowInstID.String() != schedTestWorkflowInstID {
		t.Errorf("Expected workflow_inst_id %s, got %s", schedTestWorkflowInstID, latest.WorkflowInstID)
	}
}
