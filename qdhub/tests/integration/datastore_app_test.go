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

// ==================== Integration Tests ====================

func TestDataStoreApplicationService_Integration_CreateAndGetDataStore(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create repositories
	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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

func TestDataStoreApplicationService_Integration_UpdateDataStore(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

	// Create data store
	ds, _ := svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "Test Store",
		Description: "Test",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})

	// Update data store
	newName := "Updated Store"
	newDesc := "Updated description"
	err := svc.UpdateDataStore(ctx, ds.ID, contracts.UpdateDataStoreRequest{
		Name:        &newName,
		Description: &newDesc,
	})
	if err != nil {
		t.Fatalf("UpdateDataStore failed: %v", err)
	}

	// Verify update
	updated, _ := svc.GetDataStore(ctx, ds.ID)
	if updated.Name != newName {
		t.Errorf("Expected name %s, got %s", newName, updated.Name)
	}
	if updated.Description != newDesc {
		t.Errorf("Expected description %s, got %s", newDesc, updated.Description)
	}
}

func TestDataStoreApplicationService_Integration_DeleteDataStore(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

	// Create data store
	ds, _ := svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "Test Store",
		Description: "Test",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})

	// Delete data store
	err := svc.DeleteDataStore(ctx, ds.ID)
	if err != nil {
		t.Fatalf("DeleteDataStore failed: %v", err)
	}

	// Verify deletion
	_, err = svc.GetDataStore(ctx, ds.ID)
	if err == nil {
		t.Fatal("Expected error for deleted data store")
	}
}

func TestDataStoreApplicationService_Integration_ListDataStores(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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

func TestDataStoreApplicationService_Integration_GenerateTableSchema(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	// Create a data source first (required for API metadata FK)
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

	// Create data store
	ds, _ := svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "Test Store",
		Description: "Test",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})

	// Create API metadata with response fields using MetadataRepository
	metadataRepo := repository.NewMetadataRepository(db)
	api := metadata.NewAPIMetadata(dataSource.ID, "daily", "日线行情", "日线数据", "/api/daily")
	api.SetResponseFields([]metadata.FieldMeta{
		{Name: "ts_code", Type: "str", Description: "股票代码", IsPrimary: true},
		{Name: "trade_date", Type: "str", Description: "交易日期", IsPrimary: true},
		{Name: "open", Type: "float", Description: "开盘价"},
		{Name: "close", Type: "float", Description: "收盘价"},
	})
	if err := metadataRepo.SaveAPIMetadata(ctx, api); err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	// Initialize default mapping rules
	if err := ruleRepo.InitDefaultRules(); err != nil {
		t.Fatalf("Failed to init default rules: %v", err)
	}

	// Generate table schema
	schema, err := svc.GenerateTableSchema(ctx, contracts.GenerateSchemaRequest{
		APIMetadataID: api.ID,
		DataStoreID:   ds.ID,
		TableName:     "test_daily",
		AutoCreate:    false,
	})
	if err != nil {
		t.Fatalf("GenerateTableSchema failed: %v", err)
	}
	if schema == nil {
		t.Fatal("Expected schema to be non-nil")
	}
	if len(schema.Columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(schema.Columns))
	}
	if len(schema.PrimaryKeys) != 2 {
		t.Errorf("Expected 2 primary keys, got %d", len(schema.PrimaryKeys))
	}
}

func TestDataStoreApplicationService_Integration_TableSchemaLifecycle(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	// Create a data source first (required for API metadata FK)
	dataSource := metadata.NewDataSource("Tushare", "Test", "https://api.tushare.pro", "https://doc.tushare.pro")
	if err := dataSourceRepo.Create(dataSource); err != nil {
		t.Fatalf("Failed to create data source: %v", err)
	}

	// Create API metadata (required for TableSchema FK)
	metadataRepo := repository.NewMetadataRepository(db)
	api := metadata.NewAPIMetadata(dataSource.ID, "test_api", "Test API", "Test API description", "/api/test")
	if err := metadataRepo.SaveAPIMetadata(ctx, api); err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

	// Create data store
	ds, _ := svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "Test Store",
		Description: "Test",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})

	// Create schema directly using repository (with valid API metadata ID)
	schema := datastore.NewTableSchema(ds.ID, api.ID, "test_table")
	schema.SetColumns([]datastore.ColumnDef{
		{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
		{Name: "name", SourceType: "str", TargetType: "VARCHAR", Nullable: true},
	})
	if err := dsRepo.AddSchema(schema); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Get schema
	retrieved, err := svc.GetTableSchema(ctx, schema.ID)
	if err != nil {
		t.Fatalf("GetTableSchema failed: %v", err)
	}
	if retrieved.TableName != "test_table" {
		t.Errorf("Expected table name test_table, got %s", retrieved.TableName)
	}

	// List schemas
	schemas, err := svc.ListTableSchemas(ctx, ds.ID)
	if err != nil {
		t.Fatalf("ListTableSchemas failed: %v", err)
	}
	if len(schemas) != 1 {
		t.Errorf("Expected 1 schema, got %d", len(schemas))
	}

	// Create table
	err = svc.CreateTable(ctx, schema.ID)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify status
	updated, _ := svc.GetTableSchema(ctx, schema.ID)
	if updated.Status != datastore.SchemaStatusCreated {
		t.Errorf("Expected schema status created, got %s", updated.Status)
	}
}

func TestDataStoreApplicationService_Integration_MappingRules(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

	// Create mapping rule
	rule, err := svc.CreateMappingRule(ctx, contracts.CreateMappingRuleRequest{
		DataSourceType: "tushare",
		SourceType:     "str",
		TargetDBType:   "duckdb",
		TargetType:     "VARCHAR",
		Priority:       100,
	})
	if err != nil {
		t.Fatalf("CreateMappingRule failed: %v", err)
	}
	if rule == nil {
		t.Fatal("Expected rule to be non-nil")
	}

	// Get mapping rules
	rules, err := svc.GetMappingRules(ctx, "tushare", "duckdb")
	if err != nil {
		t.Fatalf("GetMappingRules failed: %v", err)
	}
	if len(rules) != 1 {
		t.Errorf("Expected 1 rule, got %d", len(rules))
	}
}

func TestDataStoreApplicationService_Integration_TestConnection(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewQuantDataStoreRepository(db)
	ruleRepo := repository.NewDataTypeMappingRuleRepository(db)
	dataSourceRepo := repository.NewDataSourceRepository(db)
	adapter := NewMockIntegrationQuantDBAdapter()

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

	// Create data store
	ds, _ := svc.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
		Name:        "Test Store",
		Description: "Test",
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: "/tmp/test.duckdb",
	})

	// Test connection
	err := svc.TestConnection(ctx, ds.ID)
	if err != nil {
		t.Fatalf("TestConnection failed: %v", err)
	}
}
