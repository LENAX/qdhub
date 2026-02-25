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
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence/repository"
)

// MockIntegrationQuantDBAdapter is a mock adapter for integration testing.
type MockIntegrationQuantDBAdapter struct{}

func NewMockIntegrationQuantDBAdapter() *MockIntegrationQuantDBAdapter {
	return &MockIntegrationQuantDBAdapter{}
}

func (m *MockIntegrationQuantDBAdapter) TestConnection(ctx context.Context, ds *datastore.QuantDataStore) error {
	return nil
}

func (m *MockIntegrationQuantDBAdapter) ExecuteDDL(ctx context.Context, ds *datastore.QuantDataStore, ddl string) error {
	return nil
}

func (m *MockIntegrationQuantDBAdapter) TableExists(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (bool, error) {
	return false, nil
}

// MockIntegrationWorkflowExecutor is a mock workflow executor for integration testing.
type MockIntegrationWorkflowExecutor struct{}

func (m *MockIntegrationWorkflowExecutor) ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockIntegrationWorkflowExecutor) ExecuteMetadataCrawl(ctx context.Context, req workflow.MetadataCrawlRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockIntegrationWorkflowExecutor) ExecuteCreateTables(ctx context.Context, req workflow.CreateTablesRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockIntegrationWorkflowExecutor) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockIntegrationWorkflowExecutor) ExecuteRealtimeDataSync(ctx context.Context, req workflow.RealtimeDataSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockIntegrationWorkflowExecutor) ExecuteFromExecutionGraph(ctx context.Context, req workflow.ExecutionGraphSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

// ==================== Integration Tests ====================

func TestDataStoreApplicationService_Integration_CreateAndGetDataStore(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create repositories
	dsRepo := repository.NewQuantDataStoreRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowExecutor := &MockIntegrationWorkflowExecutor{}

	svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)

	// Create data store
	req := contracts.CreateDataStoreRequest{
		Name:        "Integration Test Store",
		Description: "A data store for integration testing",
		Type:        datastore.DataStoreTypeDuckDB,
		DSN:         "",
		StoragePath: "/tmp/test.duckdb",
	}

	ds, err := svc.CreateDataStore(ctx, req)
	if err != nil {
		t.Fatalf("CreateDataStore failed: %v", err)
	}
	if ds == nil {
		t.Fatal("Expected data store to be non-nil")
	}

	// Get data store
	retrieved, err := svc.GetDataStore(ctx, ds.ID)
	if err != nil {
		t.Fatalf("GetDataStore failed: %v", err)
	}
	if retrieved.Name != req.Name {
		t.Errorf("Expected name %s, got %s", req.Name, retrieved.Name)
	}
	if retrieved.Type != req.Type {
		t.Errorf("Expected type %s, got %s", req.Type, retrieved.Type)
	}
}

func TestDataStoreApplicationService_Integration_ListDataStores(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowExecutor := &MockIntegrationWorkflowExecutor{}

	svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)

	// Create multiple data stores
	for i := 0; i < 3; i++ {
		svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name:        "Test Store",
			Description: "Test",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: "/tmp/test.duckdb",
		})
	}

	// List data stores
	stores, err := svc.ListDataStores(ctx)
	if err != nil {
		t.Fatalf("ListDataStores failed: %v", err)
	}
	if len(stores) != 3 {
		t.Errorf("Expected 3 stores, got %d", len(stores))
	}
}

func TestDataStoreApplicationService_Integration_CreateTablesForDatasource(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowExecutor := &MockIntegrationWorkflowExecutor{}

	// Create a data source first
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)

	// Create data store
	ds, _ := svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "Test Store",
		Description: "Test",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})

	// Create tables for datasource
	instanceID, err := svc.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: dataSource.ID,
		DataStoreID:  ds.ID,
	})
	if err != nil {
		t.Fatalf("CreateTablesForDatasource failed: %v", err)
	}
	if instanceID.IsEmpty() {
		t.Error("Expected non-empty instance ID")
	}
}

func TestDataStoreApplicationService_Integration_CreateTablesForDatasource_DataSourceNotFound(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowExecutor := &MockIntegrationWorkflowExecutor{}

	svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)

	// Create data store
	ds, _ := svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "Test Store",
		Description: "Test",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})

	// Try to create tables with non-existent data source
	_, err := svc.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: shared.NewID(), // Non-existent
		DataStoreID:  ds.ID,
	})
	if err == nil {
		t.Error("Expected error for non-existent data source")
	}
}

func TestDataStoreApplicationService_Integration_CreateTablesForDatasource_DataStoreNotFound(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	workflowExecutor := &MockIntegrationWorkflowExecutor{}

	// Create a data source
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)

	// Try to create tables with non-existent data store
	_, err := svc.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: dataSource.ID,
		DataStoreID:  shared.NewID(), // Non-existent
	})
	if err == nil {
		t.Error("Expected error for non-existent data store")
	}
}
