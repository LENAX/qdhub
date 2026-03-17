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
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/scheduler"
)

// TestSyncService_Integration_ResolveSyncPlan_Transactional tests ResolveSyncPlan transaction behavior
func TestSyncService_Integration_ResolveSyncPlan_Transactional(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Setup
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	svc := impl.NewSyncApplicationService(
		syncPlanRepo,
		cronCalculator,
		nil,
		dataSourceRepo,
		dataStoreRepo,
		workflowExecutor,
		dependencyResolver,
		nil,
		uowImpl,
		metadataRepo,
		nil,
		"",
		nil,
	)

	// Create a data source
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create API metadata
	api1 := metadata.NewAPIMetadata(dataSource.ID, "api1", "API 1", "GET", "/api1")
	if err := dataSourceRepo.AddAPIMetadata(api1); err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	api2 := metadata.NewAPIMetadata(dataSource.ID, "api2", "API 2", "GET", "/api2")
	if err := dataSourceRepo.AddAPIMetadata(api2); err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	// Create sync plan
	plan, err := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Test Plan",
		Description:  "Test",
		DataSourceID: dataSource.ID,
		SelectedAPIs:  []string{"api1", "api2"},
	})
	if err != nil {
		t.Fatalf("Failed to create sync plan: %v", err)
	}

	// Resolve sync plan (should create tasks in transaction)
	err = svc.ResolveSyncPlan(ctx, plan.ID)
	if err != nil {
		t.Fatalf("ResolveSyncPlan failed: %v", err)
	}

	// Verify tasks were created
	retrieved, err := syncPlanRepo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if len(retrieved.Tasks) == 0 {
		t.Error("Expected tasks to be created")
	}
}

const (
	testWorkflowDefID  = "test-wf-def-uow-001"
	testWorkflowInstID = "test-wf-inst-uow-001"
)

// TestSyncService_Integration_ExecuteSyncPlan_Transactional tests ExecuteSyncPlan transaction behavior
func TestSyncService_Integration_ExecuteSyncPlan_Transactional(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Fixed workflow instance ID so we can insert workflow_def+instance to satisfy sync_execution.workflow_inst_id FK
	fixedWfInstID := shared.ID(testWorkflowInstID)
	workflowExecutor := &MockSyncWorkflowExecutor{FixedBatchSyncInstanceID: &fixedWfInstID}

	// Setup
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	metadataRepo := repository.NewMetadataRepository(db)
	svc := impl.NewSyncApplicationService(
		syncPlanRepo,
		cronCalculator,
		nil,
		dataSourceRepo,
		dataStoreRepo,
		workflowExecutor,
		dependencyResolver,
		nil,
		uowImpl,
		metadataRepo,
		nil,
		"",
		nil,
	)

	// Create a data source with token
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	token := metadata.NewToken(dataSource.ID, "test-token", nil)
	if err := dataSourceRepo.SetToken(token); err != nil {
		t.Fatalf("Failed to set token: %v", err)
	}

	// Create API metadata (required for ResolveSyncPlan)
	api1 := metadata.NewAPIMetadata(dataSource.ID, "api1", "API 1", "GET", "/api1")
	if err := dataSourceRepo.AddAPIMetadata(api1); err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	// Data store (target path resolved from plan's data store)
	store := datastore.NewQuantDataStore("Test Store", "Test", datastore.DataStoreTypeDuckDB, "", "/tmp/test.db")
	if err := dataStoreRepo.Create(store); err != nil {
		t.Fatalf("Failed to create data store: %v", err)
	}

	// Create and resolve sync plan
	plan, err := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Test Plan",
		Description:  "Test",
		DataSourceID: dataSource.ID,
		DataStoreID:  store.ID,
		SelectedAPIs: []string{"api1"},
	})
	if err != nil {
		t.Fatalf("Failed to create sync plan: %v", err)
	}

	err = svc.ResolveSyncPlan(ctx, plan.ID)
	if err != nil {
		t.Fatalf("ResolveSyncPlan failed: %v", err)
	}

	// Verify plan exists before executing
	retrievedPlan, err := syncPlanRepo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan before execution: %v", err)
	}
	if retrievedPlan == nil {
		t.Fatal("Plan should exist before execution")
	}

	// Enable plan
	if err := svc.EnablePlan(ctx, plan.ID); err != nil {
		t.Fatalf("EnablePlan failed: %v", err)
	}

	// Verify plan is enabled
	retrievedPlan, err = syncPlanRepo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan after enable: %v", err)
	}
	if retrievedPlan.Status != sync.PlanStatusEnabled {
		t.Fatalf("Plan should be enabled, got %s", retrievedPlan.Status)
	}

	// Insert workflow_definition and workflow_instance so sync_execution.workflow_inst_id FK is satisfied
	_, err = db.Exec(`INSERT INTO workflow_definition (id, name) VALUES (?, ?)`, testWorkflowDefID, "Test Sync")
	if err != nil {
		t.Fatalf("Failed to insert workflow_definition: %v", err)
	}
	_, err = db.Exec(`INSERT INTO workflow_instance (id, workflow_id, status) VALUES (?, ?, ?)`, testWorkflowInstID, testWorkflowDefID, "Running")
	if err != nil {
		t.Fatalf("Failed to insert workflow_instance: %v", err)
	}

	// Execute sync plan (should create execution and update plan in transaction)
	executionID, err := svc.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: "20250101",
		EndDate:   "20250131",
	})
	if err != nil {
		t.Fatalf("ExecuteSyncPlan failed: %v", err)
	}

	// Verify execution was created
	exec, err := syncPlanRepo.GetPlanExecution(executionID)
	if err != nil {
		t.Fatalf("Failed to get execution: %v", err)
	}
	if exec == nil {
		t.Error("Execution should exist")
	}

	// Verify plan status was updated
	retrieved, err := syncPlanRepo.Get(plan.ID)
	if err != nil {
		t.Fatalf("Failed to get plan: %v", err)
	}
	if retrieved.Status != sync.PlanStatusRunning {
		t.Errorf("Expected plan status Running, got %s", retrieved.Status)
	}
}
