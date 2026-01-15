package application_test

import (
	"context"
	"errors"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// ==================== Mock Implementations ====================

// MockQuantDataStoreRepository is a mock implementation of datastore.QuantDataStoreRepository.
type MockQuantDataStoreRepository struct {
	stores    map[shared.ID]*datastore.QuantDataStore
	createErr error
	updateErr error
	deleteErr error
}

func NewMockQuantDataStoreRepository() *MockQuantDataStoreRepository {
	return &MockQuantDataStoreRepository{
		stores: make(map[shared.ID]*datastore.QuantDataStore),
	}
}

func (m *MockQuantDataStoreRepository) Create(ds *datastore.QuantDataStore) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.stores[ds.ID] = ds
	return nil
}

func (m *MockQuantDataStoreRepository) Get(id shared.ID) (*datastore.QuantDataStore, error) {
	ds, exists := m.stores[id]
	if !exists {
		return nil, nil
	}
	return ds, nil
}

func (m *MockQuantDataStoreRepository) Update(ds *datastore.QuantDataStore) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.stores[ds.ID] = ds
	return nil
}

func (m *MockQuantDataStoreRepository) Delete(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.stores, id)
	return nil
}

func (m *MockQuantDataStoreRepository) List() ([]*datastore.QuantDataStore, error) {
	result := make([]*datastore.QuantDataStore, 0, len(m.stores))
	for _, ds := range m.stores {
		result = append(result, ds)
	}
	return result, nil
}

// MockTableSchemaRepository is a mock implementation of datastore.TableSchemaRepository.
type MockTableSchemaRepository struct {
	schemas   map[shared.ID]*datastore.TableSchema
	createErr error
	updateErr error
	deleteErr error
}

func NewMockTableSchemaRepository() *MockTableSchemaRepository {
	return &MockTableSchemaRepository{
		schemas: make(map[shared.ID]*datastore.TableSchema),
	}
}

func (m *MockTableSchemaRepository) Create(schema *datastore.TableSchema) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.schemas[schema.ID] = schema
	return nil
}

func (m *MockTableSchemaRepository) Get(id shared.ID) (*datastore.TableSchema, error) {
	schema, exists := m.schemas[id]
	if !exists {
		return nil, nil
	}
	return schema, nil
}

func (m *MockTableSchemaRepository) GetByDataStore(dataStoreID shared.ID) ([]*datastore.TableSchema, error) {
	result := make([]*datastore.TableSchema, 0)
	for _, schema := range m.schemas {
		if schema.DataStoreID == dataStoreID {
			result = append(result, schema)
		}
	}
	return result, nil
}

func (m *MockTableSchemaRepository) GetByAPIMetadata(apiMetadataID shared.ID) (*datastore.TableSchema, error) {
	for _, schema := range m.schemas {
		if schema.APIMetadataID == apiMetadataID {
			return schema, nil
		}
	}
	return nil, nil
}

func (m *MockTableSchemaRepository) Update(schema *datastore.TableSchema) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.schemas[schema.ID] = schema
	return nil
}

func (m *MockTableSchemaRepository) Delete(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.schemas, id)
	return nil
}

// MockMappingRuleRepository is a mock implementation of datastore.DataTypeMappingRuleRepository.
type MockMappingRuleRepository struct {
	rules     map[shared.ID]*datastore.DataTypeMappingRule
	createErr error
}

func NewMockMappingRuleRepository() *MockMappingRuleRepository {
	return &MockMappingRuleRepository{
		rules: make(map[shared.ID]*datastore.DataTypeMappingRule),
	}
}

func (m *MockMappingRuleRepository) Create(rule *datastore.DataTypeMappingRule) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.rules[rule.ID] = rule
	return nil
}

func (m *MockMappingRuleRepository) Get(id shared.ID) (*datastore.DataTypeMappingRule, error) {
	rule, exists := m.rules[id]
	if !exists {
		return nil, nil
	}
	return rule, nil
}

func (m *MockMappingRuleRepository) GetBySourceAndTarget(dataSourceType, targetDBType string) ([]*datastore.DataTypeMappingRule, error) {
	result := make([]*datastore.DataTypeMappingRule, 0)
	for _, rule := range m.rules {
		if rule.DataSourceType == dataSourceType && rule.TargetDBType == targetDBType {
			result = append(result, rule)
		}
	}
	return result, nil
}

func (m *MockMappingRuleRepository) SaveBatch(rules []*datastore.DataTypeMappingRule) error {
	for _, rule := range rules {
		m.rules[rule.ID] = rule
	}
	return nil
}

func (m *MockMappingRuleRepository) InitDefaultRules() error {
	return nil
}

func (m *MockMappingRuleRepository) List() ([]*datastore.DataTypeMappingRule, error) {
	result := make([]*datastore.DataTypeMappingRule, 0, len(m.rules))
	for _, rule := range m.rules {
		result = append(result, rule)
	}
	return result, nil
}

func (m *MockMappingRuleRepository) Update(rule *datastore.DataTypeMappingRule) error {
	m.rules[rule.ID] = rule
	return nil
}

func (m *MockMappingRuleRepository) Delete(id shared.ID) error {
	delete(m.rules, id)
	return nil
}

// MockAPIMetadataRepository is a mock for metadata.APIMetadataRepository.
type MockAPIMetadataRepository struct {
	apis map[shared.ID]*metadata.APIMetadata
}

func NewMockAPIMetadataRepository() *MockAPIMetadataRepository {
	return &MockAPIMetadataRepository{
		apis: make(map[shared.ID]*metadata.APIMetadata),
	}
}

func (m *MockAPIMetadataRepository) Create(meta *metadata.APIMetadata) error {
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockAPIMetadataRepository) Get(id shared.ID) (*metadata.APIMetadata, error) {
	api, exists := m.apis[id]
	if !exists {
		return nil, nil
	}
	return api, nil
}

func (m *MockAPIMetadataRepository) Update(meta *metadata.APIMetadata) error {
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockAPIMetadataRepository) Delete(id shared.ID) error {
	delete(m.apis, id)
	return nil
}

func (m *MockAPIMetadataRepository) ListByDataSource(dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.DataSourceID == dataSourceID {
			result = append(result, api)
		}
	}
	return result, nil
}

func (m *MockAPIMetadataRepository) ListByCategory(categoryID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.CategoryID != nil && *api.CategoryID == categoryID {
			result = append(result, api)
		}
	}
	return result, nil
}

// MockQuantDBAdapter is a mock implementation of QuantDBAdapter.
type MockQuantDBAdapter struct {
	testConnErr   error
	executeDDLErr error
	tableExists   bool
}

func NewMockQuantDBAdapter() *MockQuantDBAdapter {
	return &MockQuantDBAdapter{}
}

func (m *MockQuantDBAdapter) TestConnection(ctx context.Context, ds *datastore.QuantDataStore) error {
	return m.testConnErr
}

func (m *MockQuantDBAdapter) ExecuteDDL(ctx context.Context, ds *datastore.QuantDataStore, ddl string) error {
	return m.executeDDLErr
}

func (m *MockQuantDBAdapter) TableExists(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (bool, error) {
	return m.tableExists, nil
}

// ==================== Test Cases ====================

func TestDataStoreApplicationService_CreateDataStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		req := contracts.CreateDataStoreRequest{
			Name:        "Test Store",
			Description: "A test data store",
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
		if ds.Name != req.Name {
			t.Errorf("Expected name %s, got %s", req.Name, ds.Name)
		}
		if ds.Type != req.Type {
			t.Errorf("Expected type %s, got %s", req.Type, ds.Type)
		}
	})

	t.Run("Repository error", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		dsRepo.createErr = errors.New("create error")
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		req := contracts.CreateDataStoreRequest{
			Name:        "Test Store",
			Description: "A test data store",
			Type:        datastore.DataStoreTypeDuckDB,
			StoragePath: "/tmp/test.duckdb",
		}

		_, err := svc.CreateDataStore(ctx, req)
		if err == nil {
			t.Fatal("Expected error for repository failure")
		}
	})
}

func TestDataStoreApplicationService_GetDataStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		result, err := svc.GetDataStore(ctx, ds.ID)
		if err != nil {
			t.Fatalf("GetDataStore failed: %v", err)
		}
		if result.ID != ds.ID {
			t.Errorf("Expected ID %s, got %s", ds.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		_, err := svc.GetDataStore(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})
}

func TestDataStoreApplicationService_UpdateDataStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		newName := "Updated Name"
		newDesc := "Updated Description"
		err := svc.UpdateDataStore(ctx, ds.ID, contracts.UpdateDataStoreRequest{
			Name:        &newName,
			Description: &newDesc,
		})
		if err != nil {
			t.Fatalf("UpdateDataStore failed: %v", err)
		}

		updated, _ := dsRepo.Get(ds.ID)
		if updated.Name != newName {
			t.Errorf("Expected name %s, got %s", newName, updated.Name)
		}
		if updated.Description != newDesc {
			t.Errorf("Expected description %s, got %s", newDesc, updated.Description)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		newName := "Updated Name"
		err := svc.UpdateDataStore(ctx, shared.NewID(), contracts.UpdateDataStoreRequest{
			Name: &newName,
		})
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})
}

func TestDataStoreApplicationService_DeleteDataStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.DeleteDataStore(ctx, ds.ID)
		if err != nil {
			t.Fatalf("DeleteDataStore failed: %v", err)
		}

		deleted, _ := dsRepo.Get(ds.ID)
		if deleted != nil {
			t.Error("Data store should be deleted")
		}
	})

	t.Run("Cannot delete with existing schemas", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		// Create a schema for this data store
		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.DeleteDataStore(ctx, ds.ID)
		if err == nil {
			t.Fatal("Expected error when deleting data store with existing schemas")
		}
	})
}

func TestDataStoreApplicationService_ListDataStores(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockQuantDataStoreRepository()
	schemaRepo := NewMockTableSchemaRepository()
	ruleRepo := NewMockMappingRuleRepository()
	apiMetaRepo := NewMockAPIMetadataRepository()
	adapter := NewMockQuantDBAdapter()

	// Create multiple data stores
	for i := 0; i < 3; i++ {
		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

	stores, err := svc.ListDataStores(ctx)
	if err != nil {
		t.Fatalf("ListDataStores failed: %v", err)
	}
	if len(stores) != 3 {
		t.Errorf("Expected 3 stores, got %d", len(stores))
	}
}

func TestDataStoreApplicationService_TestConnection(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.TestConnection(ctx, ds.ID)
		if err != nil {
			t.Fatalf("TestConnection failed: %v", err)
		}
	})

	t.Run("Connection failed", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()
		adapter.testConnErr = errors.New("connection failed")

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.TestConnection(ctx, ds.ID)
		if err == nil {
			t.Fatal("Expected error when connection fails")
		}
	})
}

func TestDataStoreApplicationService_GenerateTableSchema(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		// Create data store
		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		// Create API metadata with response fields
		api := metadata.NewAPIMetadata(shared.NewID(), "daily", "日线行情", "日线数据", "/api/daily")
		api.SetResponseFields([]metadata.FieldMeta{
			{Name: "ts_code", Type: "str", Description: "股票代码", IsPrimary: true},
			{Name: "trade_date", Type: "str", Description: "交易日期", IsPrimary: true},
			{Name: "open", Type: "float", Description: "开盘价"},
			{Name: "close", Type: "float", Description: "收盘价"},
		})
		apiMetaRepo.Create(api)

		// Create mapping rules
		rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
		ruleRepo.Create(rule1)
		rule2 := datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true)
		ruleRepo.Create(rule2)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

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
	})

	t.Run("DataStore not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		_, err := svc.GenerateTableSchema(ctx, contracts.GenerateSchemaRequest{
			APIMetadataID: shared.NewID(),
			DataStoreID:   shared.NewID(),
			TableName:     "test_table",
		})
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})
}

func TestDataStoreApplicationService_CreateTable(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		// Create data store
		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		// Create schema
		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		schema.SetColumns([]datastore.ColumnDef{
			{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
			{Name: "name", SourceType: "str", TargetType: "VARCHAR", Nullable: true},
		})
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.CreateTable(ctx, schema.ID)
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}

		updated, _ := schemaRepo.Get(schema.ID)
		if updated.Status != datastore.SchemaStatusCreated {
			t.Errorf("Expected schema status created, got %s", updated.Status)
		}
	})

	t.Run("DDL execution failed", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()
		adapter.executeDDLErr = errors.New("DDL error")

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		schema.SetColumns([]datastore.ColumnDef{
			{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
		})
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.CreateTable(ctx, schema.ID)
		if err == nil {
			t.Fatal("Expected error when DDL execution fails")
		}

		updated, _ := schemaRepo.Get(schema.ID)
		if updated.Status != datastore.SchemaStatusFailed {
			t.Errorf("Expected schema status failed, got %s", updated.Status)
		}
	})
}

func TestDataStoreApplicationService_DropTable(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		schema.MarkCreated()
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.DropTable(ctx, schema.ID)
		if err != nil {
			t.Fatalf("DropTable failed: %v", err)
		}

		deleted, _ := schemaRepo.Get(schema.ID)
		if deleted != nil {
			t.Error("Schema should be deleted")
		}
	})
}

func TestDataStoreApplicationService_GetTableSchema(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test_table")
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		result, err := svc.GetTableSchema(ctx, schema.ID)
		if err != nil {
			t.Fatalf("GetTableSchema failed: %v", err)
		}
		if result.ID != schema.ID {
			t.Errorf("Expected ID %s, got %s", schema.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		_, err := svc.GetTableSchema(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent schema")
		}
	})
}

func TestDataStoreApplicationService_GetTableSchemaByAPI(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockQuantDataStoreRepository()
	schemaRepo := NewMockTableSchemaRepository()
	ruleRepo := NewMockMappingRuleRepository()
	apiMetaRepo := NewMockAPIMetadataRepository()
	adapter := NewMockQuantDBAdapter()

	apiID := shared.NewID()
	schema := datastore.NewTableSchema(shared.NewID(), apiID, "test_table")
	schemaRepo.Create(schema)

	svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

	result, err := svc.GetTableSchemaByAPI(ctx, apiID)
	if err != nil {
		t.Fatalf("GetTableSchemaByAPI failed: %v", err)
	}
	if result.APIMetadataID != apiID {
		t.Errorf("Expected API ID %s, got %s", apiID, result.APIMetadataID)
	}
}

func TestDataStoreApplicationService_ListTableSchemas(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockQuantDataStoreRepository()
	schemaRepo := NewMockTableSchemaRepository()
	ruleRepo := NewMockMappingRuleRepository()
	apiMetaRepo := NewMockAPIMetadataRepository()
	adapter := NewMockQuantDBAdapter()

	dsID := shared.NewID()
	for i := 0; i < 3; i++ {
		schema := datastore.NewTableSchema(dsID, shared.NewID(), "test_table")
		schemaRepo.Create(schema)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

	schemas, err := svc.ListTableSchemas(ctx, dsID)
	if err != nil {
		t.Fatalf("ListTableSchemas failed: %v", err)
	}
	if len(schemas) != 3 {
		t.Errorf("Expected 3 schemas, got %d", len(schemas))
	}
}

func TestDataStoreApplicationService_UpdateTableSchema(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test_table")
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		newCols := []datastore.ColumnDef{
			{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
		}
		err := svc.UpdateTableSchema(ctx, schema.ID, contracts.UpdateSchemaRequest{
			Columns: &newCols,
		})
		if err != nil {
			t.Fatalf("UpdateTableSchema failed: %v", err)
		}

		updated, _ := schemaRepo.Get(schema.ID)
		if len(updated.Columns) != 1 {
			t.Errorf("Expected 1 column, got %d", len(updated.Columns))
		}
	})

	t.Run("Cannot update created schema", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test_table")
		schema.MarkCreated()
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		newCols := []datastore.ColumnDef{
			{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
		}
		err := svc.UpdateTableSchema(ctx, schema.ID, contracts.UpdateSchemaRequest{
			Columns: &newCols,
		})
		if err == nil {
			t.Fatal("Expected error when updating created schema")
		}
	})
}

func TestDataStoreApplicationService_CreateMappingRule(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		req := contracts.CreateMappingRuleRequest{
			DataSourceType: "tushare",
			SourceType:     "str",
			TargetDBType:   "duckdb",
			TargetType:     "VARCHAR",
			Priority:       100,
		}

		rule, err := svc.CreateMappingRule(ctx, req)
		if err != nil {
			t.Fatalf("CreateMappingRule failed: %v", err)
		}
		if rule == nil {
			t.Fatal("Expected rule to be non-nil")
		}
		if rule.SourceType != req.SourceType {
			t.Errorf("Expected source type %s, got %s", req.SourceType, rule.SourceType)
		}
	})
}

func TestDataStoreApplicationService_GetMappingRules(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockQuantDataStoreRepository()
	schemaRepo := NewMockTableSchemaRepository()
	ruleRepo := NewMockMappingRuleRepository()
	apiMetaRepo := NewMockAPIMetadataRepository()
	adapter := NewMockQuantDBAdapter()

	// Create rules
	rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	ruleRepo.Create(rule1)
	rule2 := datastore.NewDataTypeMappingRule("tushare", "int", "duckdb", "INTEGER", 100, true)
	ruleRepo.Create(rule2)

	svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

	rules, err := svc.GetMappingRules(ctx, "tushare", "duckdb")
	if err != nil {
		t.Fatalf("GetMappingRules failed: %v", err)
	}
	if len(rules) != 2 {
		t.Errorf("Expected 2 rules, got %d", len(rules))
	}
}

func TestDataStoreApplicationService_SyncSchemaStatus(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		schemaRepo := NewMockTableSchemaRepository()
		ruleRepo := NewMockMappingRuleRepository()
		apiMetaRepo := NewMockAPIMetadataRepository()
		adapter := NewMockQuantDBAdapter()
		adapter.tableExists = true

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		schemaRepo.Create(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, schemaRepo, ruleRepo, apiMetaRepo, adapter)

		err := svc.SyncSchemaStatus(ctx, ds.ID)
		if err != nil {
			t.Fatalf("SyncSchemaStatus failed: %v", err)
		}

		updated, _ := schemaRepo.Get(schema.ID)
		if updated.Status != datastore.SchemaStatusCreated {
			t.Errorf("Expected schema status created, got %s", updated.Status)
		}
	})
}
