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
	// LastBatchDataSyncRequest captures the last BatchDataSyncRequest passed to ExecuteBatchDataSync (for regression tests).
	LastBatchDataSyncRequest *workflow.BatchDataSyncRequest
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
	// Capture request for regression tests (shallow copy of request, configs slice is shared)
	m.LastBatchDataSyncRequest = &req
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

func (m *MockSyncPlanScheduler) GetScheduledPlanIDs() []string {
	ids := make([]string, 0, len(m.scheduledPlans))
	for id := range m.scheduledPlans {
		ids = append(ids, id)
	}
	return ids
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

// ResolverWithParamMappings returns a graph where the first API has template mode and ParamMappings (for regression tests).
type ResolverWithParamMappings struct{}

func (m *ResolverWithParamMappings) Resolve(selectedAPIs []string, allAPIDependencies map[string][]sync.ParamDependency) (*sync.ExecutionGraph, []string, error) {
	// Level 0: base deps; Level 1: selected APIs
	levels := [][]string{{"trade_cal", "stock_basic"}, selectedAPIs}
	graph := &sync.ExecutionGraph{
		Levels:      levels,
		TaskConfigs: make(map[string]*sync.TaskConfig),
	}
	graph.TaskConfigs["trade_cal"] = &sync.TaskConfig{APIName: "trade_cal", SyncMode: sync.TaskSyncModeDirect}
	graph.TaskConfigs["stock_basic"] = &sync.TaskConfig{APIName: "stock_basic", SyncMode: sync.TaskSyncModeDirect}
	for _, api := range selectedAPIs {
		graph.TaskConfigs[api] = &sync.TaskConfig{
			APIName:      api,
			SyncMode:     sync.TaskSyncModeTemplate,
			Dependencies: []string{"FetchTradeCal"},
			ParamMappings: []sync.ParamMapping{
				{ParamName: "trade_date", SourceTask: "FetchTradeCal", SourceField: "cal_date", Select: "last"},
			},
		}
	}
	return graph, append([]string{"trade_cal", "stock_basic"}, selectedAPIs...), nil
}

// ==================== Integration Tests ====================

func TestSyncApplicationService_Integration_CreateAndGetSyncPlan(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create repositories
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)
	metadataRepo := repository.NewMetadataRepository(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, workflowExecutor, dependencyResolver, nil, uowImpl, metadataRepo, nil)

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
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	metadataRepo := repository.NewMetadataRepository(db)
	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, workflowExecutor, dependencyResolver, nil, uowImpl, metadataRepo, nil)

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
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	metadataRepo := repository.NewMetadataRepository(db)
	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, workflowExecutor, dependencyResolver, nil, uowImpl, metadataRepo, nil)

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
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	metadataRepo := repository.NewMetadataRepository(db)
	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, workflowExecutor, dependencyResolver, nil, uowImpl, metadataRepo, nil)

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
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)

	metadataRepo := repository.NewMetadataRepository(db)
	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, workflowExecutor, dependencyResolver, nil, uowImpl, metadataRepo, nil)

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
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	workflowExecutor := &MockSyncWorkflowExecutor{}
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)
	metadataRepo := repository.NewMetadataRepository(db)

	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, workflowExecutor, dependencyResolver, nil, uowImpl, metadataRepo, nil)

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

// TestSyncApplicationService_Integration_ExecuteSyncPlan_PassesAPIConfigs 回归测试：执行同步计划时必须将 APIConfigs 传给工作流，而非仅 APINames
func TestSyncApplicationService_Integration_ExecuteSyncPlan_PassesAPIConfigs(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()
	fixedWfInstID := shared.ID("test-wf-inst-apiconfigs")
	capturingExecutor := &MockSyncWorkflowExecutor{FixedBatchSyncInstanceID: &fixedWfInstID}

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	dependencyResolver := &MockSyncDependencyResolver{}
	uowImpl := uow.NewUnitOfWork(db)
	metadataRepo := repository.NewMetadataRepository(db)

	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, capturingExecutor, dependencyResolver, nil, uowImpl, metadataRepo, nil)

	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("create data source: %v", err)
	}
	token := metadata.NewToken(dataSource.ID, "test-token", nil)
	if err := dataSourceRepo.SetToken(token); err != nil {
		t.Fatalf("set token: %v", err)
	}
	apiMeta := metadata.NewAPIMetadata(dataSource.ID, "api1", "API 1", "GET", "/api1")
	if err := dataSourceRepo.AddAPIMetadata(apiMeta); err != nil {
		t.Fatalf("add api metadata: %v", err)
	}
	store := datastore.NewQuantDataStore("Test Store", "Test", datastore.DataStoreTypeDuckDB, "", "/tmp/test.db")
	if err := dataStoreRepo.Create(store); err != nil {
		t.Fatalf("create data store: %v", err)
	}

	plan, err := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Plan",
		DataSourceID: dataSource.ID,
		DataStoreID:  store.ID,
		SelectedAPIs: []string{"api1"},
	})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if err := svc.ResolveSyncPlan(ctx, plan.ID); err != nil {
		t.Fatalf("resolve plan: %v", err)
	}
	if err := svc.EnablePlan(ctx, plan.ID); err != nil {
		t.Fatalf("enable plan: %v", err)
	}

	_, _ = db.Exec(`INSERT INTO workflow_definition (id, name) VALUES (?, ?)`, "wf-def-apiconfigs", "Test")
	_, _ = db.Exec(`INSERT INTO workflow_instance (id, workflow_id, status) VALUES (?, ?, ?)`, string(fixedWfInstID), "wf-def-apiconfigs", "Running")

	_, err = svc.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: "20250101",
		EndDate:   "20250131",
	})
	if err != nil {
		t.Fatalf("ExecuteSyncPlan: %v", err)
	}

	req := capturingExecutor.LastBatchDataSyncRequest
	if req == nil {
		t.Fatal("ExecuteBatchDataSync was not called or request was not captured")
	}
	if len(req.APIConfigs) == 0 {
		t.Errorf("expected APIConfigs to be set (SyncPlan path); got len(APIConfigs)=0")
	}
	if len(req.APINames) != 0 {
		t.Errorf("expected APINames to be empty when using APIConfigs; got len(APINames)=%d", len(req.APINames))
	}
}

// TestSyncApplicationService_Integration_ExecuteSyncPlan_APIConfigsHaveParamKeyAndUpstreamTask 回归测试：当计划任务含 ParamMappings 时，APIConfigs 中应带上 ParamKey 与 UpstreamTask
func TestSyncApplicationService_Integration_ExecuteSyncPlan_APIConfigsHaveParamKeyAndUpstreamTask(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()
	fixedWfInstID := shared.ID("test-wf-inst-paramkey")
	capturingExecutor := &MockSyncWorkflowExecutor{FixedBatchSyncInstanceID: &fixedWfInstID}
	resolverWithMappings := &ResolverWithParamMappings{}

	syncPlanRepo := repository.NewSyncPlanRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()
	uowImpl := uow.NewUnitOfWork(db)
	metadataRepo := repository.NewMetadataRepository(db)

	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	svc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, dataStoreRepo, capturingExecutor, resolverWithMappings, nil, uowImpl, metadataRepo, nil)

	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("create data source: %v", err)
	}
	token := metadata.NewToken(dataSource.ID, "test-token", nil)
	if err := dataSourceRepo.SetToken(token); err != nil {
		t.Fatalf("set token: %v", err)
	}
	for _, name := range []string{"trade_cal", "stock_basic", "daily"} {
		apiMeta := metadata.NewAPIMetadata(dataSource.ID, name, name, "GET", "/"+name)
		if err := dataSourceRepo.AddAPIMetadata(apiMeta); err != nil {
			t.Fatalf("add api metadata %s: %v", name, err)
		}
	}
	store := datastore.NewQuantDataStore("Test Store", "Test", datastore.DataStoreTypeDuckDB, "", "/tmp/test.db")
	if err := dataStoreRepo.Create(store); err != nil {
		t.Fatalf("create data store: %v", err)
	}

	plan, err := svc.CreateSyncPlan(ctx, contracts.CreateSyncPlanRequest{
		Name:         "Plan",
		DataSourceID: dataSource.ID,
		DataStoreID:  store.ID,
		SelectedAPIs: []string{"daily"},
	})
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	if err := svc.ResolveSyncPlan(ctx, plan.ID); err != nil {
		t.Fatalf("resolve plan: %v", err)
	}
	if err := svc.EnablePlan(ctx, plan.ID); err != nil {
		t.Fatalf("enable plan: %v", err)
	}

	_, _ = db.Exec(`INSERT INTO workflow_definition (id, name) VALUES (?, ?)`, "wf-def-paramkey", "Test")
	_, _ = db.Exec(`INSERT INTO workflow_instance (id, workflow_id, status) VALUES (?, ?, ?)`, string(fixedWfInstID), "wf-def-paramkey", "Running")

	_, err = svc.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: "20250101",
		EndDate:   "20250131",
	})
	if err != nil {
		t.Fatalf("ExecuteSyncPlan: %v", err)
	}

	req := capturingExecutor.LastBatchDataSyncRequest
	if req == nil || len(req.APIConfigs) == 0 {
		t.Fatal("ExecuteBatchDataSync was not called or APIConfigs empty")
	}

	var foundWithParamKey bool
	for _, c := range req.APIConfigs {
		if c.APIName == "daily" && c.ParamKey == "trade_date" && c.UpstreamTask == "FetchTradeCal" {
			foundWithParamKey = true
			break
		}
	}
	if !foundWithParamKey {
		t.Errorf("expected at least one APIConfig (daily) to have ParamKey=trade_date and UpstreamTask=FetchTradeCal; got configs: %+v", req.APIConfigs)
	}
}
