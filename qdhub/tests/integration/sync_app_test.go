//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/persistence/uow"
	"qdhub/internal/infrastructure/scheduler"
)

// MockSyncWorkflowExecutor is a mock workflow executor for sync integration testing.
type MockSyncWorkflowExecutor struct {
	// FixedBatchSyncInstanceID when set, ExecuteBatchDataSync returns this ID instead of NewID().
	// Use in tests that insert workflow_instance (e.g. UoW ExecuteSyncPlan) to satisfy FK.
	FixedBatchSyncInstanceID *shared.ID
}

func (m *MockSyncWorkflowExecutor) ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockSyncWorkflowExecutor) ExecuteMetadataCrawl(ctx context.Context, req workflow.MetadataCrawlRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockSyncWorkflowExecutor) ExecuteCreateTables(ctx context.Context, req workflow.CreateTablesRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockSyncWorkflowExecutor) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	if m.FixedBatchSyncInstanceID != nil {
		return *m.FixedBatchSyncInstanceID, nil
	}
	return shared.NewID(), nil
}

func (m *MockSyncWorkflowExecutor) ExecuteRealtimeDataSync(ctx context.Context, req workflow.RealtimeDataSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockSyncWorkflowExecutor) ExecuteFromExecutionGraph(ctx context.Context, req workflow.ExecutionGraphSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

// MockSyncPlanScheduler is a mock plan scheduler for testing.
type MockSyncPlanScheduler struct {
	scheduledPlans map[string]string
}

func NewMockSyncPlanScheduler() *MockSyncPlanScheduler {
	return &MockSyncPlanScheduler{
		scheduledPlans: make(map[string]string),
	}
}

func (m *MockSyncPlanScheduler) SchedulePlan(planID string, cronExpr string) error {
	m.scheduledPlans[planID] = cronExpr
	return nil
}

func (m *MockSyncPlanScheduler) UnschedulePlan(planID string) {
	delete(m.scheduledPlans, planID)
}

func (m *MockSyncPlanScheduler) GetNextRunTime(planID string) *interface{} {
	return nil
}

// MockSyncDependencyResolver is a mock dependency resolver for testing.
type MockSyncDependencyResolver struct{}

func (m *MockSyncDependencyResolver) Resolve(selectedAPIs []string, allAPIDependencies map[string][]sync.ParamDependency) (*sync.ExecutionGraph, []string, error) {
	graph := &sync.ExecutionGraph{
		Levels:      [][]string{selectedAPIs},
		TaskConfigs: make(map[string]*sync.TaskConfig),
	}
	for _, api := range selectedAPIs {
		graph.TaskConfigs[api] = &sync.TaskConfig{
			APIName:  api,
			SyncMode: sync.TaskSyncModeDirect,
		}
	}
	return graph, selectedAPIs, nil
}

// ==================== Integration Tests ====================

func TestSyncApplicationService_Integration_CreateAndGetSyncPlan(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create repositories
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, workflowExecutor, dependencyResolver, nil, uowImpl)

	// Create a data source first
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create sync plan
	req := contracts.CreateSyncPlanRequest{
		Name:         "Integration Test Plan",
		Description:  "A sync plan for integration testing",
		DataSourceID: dataSource.ID,
		SelectedAPIs: []string{"daily", "stock_basic"},
	}

	plan, err := svc.CreateSyncPlan(ctx, req)
	if err != nil {
		t.Fatalf("CreateSyncPlan failed: %v", err)
	}
	if plan == nil {
		t.Fatal("Expected sync plan to be non-nil")
	}

	// Get sync plan
	retrieved, err := svc.GetSyncPlan(ctx, plan.ID)
	if err != nil {
		t.Fatalf("GetSyncPlan failed: %v", err)
	}
	if retrieved.Name != req.Name {
		t.Errorf("Expected name %s, got %s", req.Name, retrieved.Name)
	}
	if len(retrieved.SelectedAPIs) != len(req.SelectedAPIs) {
		t.Errorf("Expected %d selected APIs, got %d", len(req.SelectedAPIs), len(retrieved.SelectedAPIs))
	}
}

func TestSyncApplicationService_Integration_ListSyncPlans(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, workflowExecutor, dependencyResolver, nil, uowImpl)

	// Create a data source first
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create multiple sync plans
	for i := 0; i < 3; i++ {
		svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
			Name:         "Test Plan",
			Description:  "Test",
			DataSourceID: dataSource.ID,
			SelectedAPIs: []string{"daily"},
		})
	}

	// List sync plans
	plans, err := svc.ListSyncPlans(ctx)
	if err != nil {
		t.Fatalf("ListSyncPlans failed: %v", err)
	}
	if len(plans) != 3 {
		t.Errorf("Expected 3 plans, got %d", len(plans))
	}
}

func TestSyncApplicationService_Integration_UpdateSyncPlan(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, workflowExecutor, dependencyResolver, nil, uowImpl)

	// Create a data source
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create sync plan
	plan, _ := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Test Plan",
		Description:  "Test",
		DataSourceID: dataSource.ID,
		SelectedAPIs: []string{"daily"},
	})

	// Update sync plan
	newName := "Updated Plan"
	newDesc := "Updated description"
	err := svc.UpdateSyncPlan(ctx, plan.ID, contracts.UpdateSyncPlanRequest{
		Name:        &newName,
		Description: &newDesc,
	})
	if err != nil {
		t.Fatalf("UpdateSyncPlan failed: %v", err)
	}

	// Verify update
	updated, _ := svc.GetSyncPlan(ctx, plan.ID)
	if updated.Name != newName {
		t.Errorf("Expected name %s, got %s", newName, updated.Name)
	}
	if updated.Description != newDesc {
		t.Errorf("Expected description %s, got %s", newDesc, updated.Description)
	}
}

func TestSyncApplicationService_Integration_DeleteSyncPlan(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, workflowExecutor, dependencyResolver, nil, uowImpl)

	// Create a data source
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create sync plan
	plan, _ := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Test Plan",
		Description:  "Test",
		DataSourceID: dataSource.ID,
		SelectedAPIs: []string{"daily"},
	})

	// Delete sync plan
	err := svc.DeleteSyncPlan(ctx, plan.ID)
	if err != nil {
		t.Fatalf("DeleteSyncPlan failed: %v", err)
	}

	// Verify deletion
	_, err = svc.GetSyncPlan(ctx, plan.ID)
	if err == nil {
		t.Fatal("Expected error for deleted sync plan")
	}
}

func TestSyncApplicationService_Integration_EnableDisablePlan(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, workflowExecutor, dependencyResolver, nil, uowImpl)

	// Create a data source
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create sync plan
	plan, _ := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Test Plan",
		Description:  "Test",
		DataSourceID: dataSource.ID,
		SelectedAPIs: []string{"daily"},
	})

	// Resolve dependencies first (required before enabling)
	if err := svc.ResolveSyncPlan(ctx, plan.ID); err != nil {
		t.Fatalf("ResolveSyncPlan failed: %v", err)
	}

	// Enable plan
	if err := svc.EnablePlan(ctx, plan.ID); err != nil {
		t.Fatalf("EnablePlan failed: %v", err)
	}

	enabled, _ := svc.GetSyncPlan(ctx, plan.ID)
	if enabled.Status != sync.PlanStatusEnabled {
		t.Errorf("Expected plan status enabled, got %s", enabled.Status)
	}

	// Disable plan
	if err := svc.DisablePlan(ctx, plan.ID); err != nil {
		t.Fatalf("DisablePlan failed: %v", err)
	}

	disabled, _ := svc.GetSyncPlan(ctx, plan.ID)
	if disabled.Status != sync.PlanStatusDisabled {
		t.Errorf("Expected plan status disabled, got %s", disabled.Status)
	}
}

func TestSyncApplicationService_Integration_CreateSyncPlan_DataSourceNotFound(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, workflowExecutor, dependencyResolver, nil, uowImpl)

	// Try to create sync plan with non-existent data source
	_, err := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Test Plan",
		Description:  "Test",
		DataSourceID: shared.NewID(), // Non-existent
		SelectedAPIs: []string{"daily"},
	})
	if err == nil {
		t.Error("Expected error for non-existent data source")
	}
}
