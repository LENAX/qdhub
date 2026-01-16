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
// Following DDD, this repository handles both QuantDataStore (aggregate root) and TableSchema (child entity).
type MockQuantDataStoreRepository struct {
	stores    map[shared.ID]*datastore.QuantDataStore
	schemas   map[shared.ID]*datastore.TableSchema
	createErr error
	updateErr error
	deleteErr error
}

func NewMockQuantDataStoreRepository() *MockQuantDataStoreRepository {
	return &MockQuantDataStoreRepository{
		stores:  make(map[shared.ID]*datastore.QuantDataStore),
		schemas: make(map[shared.ID]*datastore.TableSchema),
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

// ==================== Child Entity Operations (TableSchema) ====================

func (m *MockQuantDataStoreRepository) AddSchema(schema *datastore.TableSchema) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.schemas[schema.ID] = schema
	return nil
}

func (m *MockQuantDataStoreRepository) GetSchema(id shared.ID) (*datastore.TableSchema, error) {
	schema, exists := m.schemas[id]
	if !exists {
		return nil, nil
	}
	return schema, nil
}

func (m *MockQuantDataStoreRepository) GetSchemaByAPIMetadata(apiMetadataID shared.ID) (*datastore.TableSchema, error) {
	for _, schema := range m.schemas {
		if schema.APIMetadataID == apiMetadataID {
			return schema, nil
		}
	}
	return nil, nil
}

func (m *MockQuantDataStoreRepository) GetSchemasByDataStore(dataStoreID shared.ID) ([]*datastore.TableSchema, error) {
	result := make([]*datastore.TableSchema, 0)
	for _, schema := range m.schemas {
		if schema.DataStoreID == dataStoreID {
			result = append(result, schema)
		}
	}
	return result, nil
}

func (m *MockQuantDataStoreRepository) UpdateSchema(schema *datastore.TableSchema) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.schemas[schema.ID] = schema
	return nil
}

func (m *MockQuantDataStoreRepository) DeleteSchema(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.schemas, id)
	return nil
}

// ==================== Extended Query Operations ====================

func (m *MockQuantDataStoreRepository) FindBy(conditions ...shared.QueryCondition) ([]*datastore.QuantDataStore, error) {
	return m.List()
}

func (m *MockQuantDataStoreRepository) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*datastore.QuantDataStore, error) {
	return m.List()
}

func (m *MockQuantDataStoreRepository) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[datastore.QuantDataStore], error) {
	stores, _ := m.List()
	return shared.NewPageResult(stores, int64(len(stores)), pagination), nil
}

func (m *MockQuantDataStoreRepository) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[datastore.QuantDataStore], error) {
	return m.ListWithPagination(pagination)
}

func (m *MockQuantDataStoreRepository) Count(conditions ...shared.QueryCondition) (int64, error) {
	return int64(len(m.stores)), nil
}

func (m *MockQuantDataStoreRepository) Exists(conditions ...shared.QueryCondition) (bool, error) {
	return len(m.stores) > 0, nil
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

// ==================== Extended Query Operations ====================

func (m *MockMappingRuleRepository) FindBy(conditions ...shared.QueryCondition) ([]*datastore.DataTypeMappingRule, error) {
	return m.List()
}

func (m *MockMappingRuleRepository) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*datastore.DataTypeMappingRule, error) {
	return m.List()
}

func (m *MockMappingRuleRepository) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[datastore.DataTypeMappingRule], error) {
	rules, _ := m.List()
	return shared.NewPageResult(rules, int64(len(rules)), pagination), nil
}

func (m *MockMappingRuleRepository) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[datastore.DataTypeMappingRule], error) {
	return m.ListWithPagination(pagination)
}

func (m *MockMappingRuleRepository) Count(conditions ...shared.QueryCondition) (int64, error) {
	return int64(len(m.rules)), nil
}

func (m *MockMappingRuleRepository) Exists(conditions ...shared.QueryCondition) (bool, error) {
	return len(m.rules) > 0, nil
}

// MockDataSourceRepository is a mock implementation of metadata.DataSourceRepository.
type MockDataSourceRepository struct {
	sources    map[shared.ID]*metadata.DataSource
	categories map[shared.ID]*metadata.APICategory
	apis       map[shared.ID]*metadata.APIMetadata
	tokens     map[shared.ID]*metadata.Token
}

func NewMockDataSourceRepository() *MockDataSourceRepository {
	return &MockDataSourceRepository{
		sources:    make(map[shared.ID]*metadata.DataSource),
		categories: make(map[shared.ID]*metadata.APICategory),
		apis:       make(map[shared.ID]*metadata.APIMetadata),
		tokens:     make(map[shared.ID]*metadata.Token),
	}
}

func (m *MockDataSourceRepository) Create(ds *metadata.DataSource) error {
	m.sources[ds.ID] = ds
	return nil
}

func (m *MockDataSourceRepository) Get(id shared.ID) (*metadata.DataSource, error) {
	ds, exists := m.sources[id]
	if !exists {
		return nil, nil
	}
	return ds, nil
}

func (m *MockDataSourceRepository) Update(ds *metadata.DataSource) error {
	m.sources[ds.ID] = ds
	return nil
}

func (m *MockDataSourceRepository) Delete(id shared.ID) error {
	delete(m.sources, id)
	return nil
}

func (m *MockDataSourceRepository) List() ([]*metadata.DataSource, error) {
	result := make([]*metadata.DataSource, 0, len(m.sources))
	for _, ds := range m.sources {
		result = append(result, ds)
	}
	return result, nil
}

// Child Entity Operations (APICategory)
func (m *MockDataSourceRepository) AddCategory(cat *metadata.APICategory) error {
	m.categories[cat.ID] = cat
	return nil
}

func (m *MockDataSourceRepository) GetCategory(id shared.ID) (*metadata.APICategory, error) {
	cat, exists := m.categories[id]
	if !exists {
		return nil, nil
	}
	return cat, nil
}

func (m *MockDataSourceRepository) ListCategoriesByDataSource(dataSourceID shared.ID) ([]*metadata.APICategory, error) {
	result := make([]*metadata.APICategory, 0)
	for _, cat := range m.categories {
		if cat.DataSourceID == dataSourceID {
			result = append(result, cat)
		}
	}
	return result, nil
}

func (m *MockDataSourceRepository) UpdateCategory(cat *metadata.APICategory) error {
	m.categories[cat.ID] = cat
	return nil
}

func (m *MockDataSourceRepository) DeleteCategory(id shared.ID) error {
	delete(m.categories, id)
	return nil
}

// Child Entity Operations (APIMetadata)
func (m *MockDataSourceRepository) AddAPIMetadata(meta *metadata.APIMetadata) error {
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockDataSourceRepository) GetAPIMetadata(id shared.ID) (*metadata.APIMetadata, error) {
	api, exists := m.apis[id]
	if !exists {
		return nil, nil
	}
	return api, nil
}

func (m *MockDataSourceRepository) ListAPIMetadataByDataSource(dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.DataSourceID == dataSourceID {
			result = append(result, api)
		}
	}
	return result, nil
}

func (m *MockDataSourceRepository) ListAPIMetadataByCategory(categoryID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.CategoryID != nil && *api.CategoryID == categoryID {
			result = append(result, api)
		}
	}
	return result, nil
}

func (m *MockDataSourceRepository) UpdateAPIMetadata(meta *metadata.APIMetadata) error {
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockDataSourceRepository) DeleteAPIMetadata(id shared.ID) error {
	delete(m.apis, id)
	return nil
}

// Child Entity Operations (Token)
func (m *MockDataSourceRepository) SetToken(token *metadata.Token) error {
	m.tokens[token.ID] = token
	return nil
}

func (m *MockDataSourceRepository) GetToken(id shared.ID) (*metadata.Token, error) {
	token, exists := m.tokens[id]
	if !exists {
		return nil, nil
	}
	return token, nil
}

func (m *MockDataSourceRepository) GetTokenByDataSource(dataSourceID shared.ID) (*metadata.Token, error) {
	for _, token := range m.tokens {
		if token.DataSourceID == dataSourceID {
			return token, nil
		}
	}
	return nil, nil
}

func (m *MockDataSourceRepository) DeleteToken(id shared.ID) error {
	delete(m.tokens, id)
	return nil
}

// Extended Query Operations
func (m *MockDataSourceRepository) FindBy(conditions ...shared.QueryCondition) ([]*metadata.DataSource, error) {
	return m.List()
}

func (m *MockDataSourceRepository) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*metadata.DataSource, error) {
	return m.List()
}

func (m *MockDataSourceRepository) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[metadata.DataSource], error) {
	sources, _ := m.List()
	return shared.NewPageResult(sources, int64(len(sources)), pagination), nil
}

func (m *MockDataSourceRepository) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[metadata.DataSource], error) {
	return m.ListWithPagination(pagination)
}

func (m *MockDataSourceRepository) Count(conditions ...shared.QueryCondition) (int64, error) {
	return int64(len(m.sources)), nil
}

func (m *MockDataSourceRepository) Exists(conditions ...shared.QueryCondition) (bool, error) {
	return len(m.sources) > 0, nil
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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		// Create a schema for this data store
		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.DeleteDataStore(ctx, ds.ID)
		if err == nil {
			t.Fatal("Expected error when deleting data store with existing schemas")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.DeleteDataStore(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})
}

func TestDataStoreApplicationService_ListDataStores(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockQuantDataStoreRepository()
	ruleRepo := NewMockMappingRuleRepository()
	dataSourceRepo := NewMockDataSourceRepository()
	adapter := NewMockQuantDBAdapter()

	// Create multiple data stores
	for i := 0; i < 3; i++ {
		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.TestConnection(ctx, ds.ID)
		if err != nil {
			t.Fatalf("TestConnection failed: %v", err)
		}
	})

	t.Run("Connection failed", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()
		adapter.testConnErr = errors.New("connection failed")

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.TestConnection(ctx, ds.ID)
		if err == nil {
			t.Fatal("Expected error when connection fails")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.TestConnection(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})
}

func TestDataStoreApplicationService_GenerateTableSchema(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
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
		dataSourceRepo.AddAPIMetadata(api)

		// Create mapping rules
		rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
		ruleRepo.Create(rule1)
		rule2 := datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true)
		ruleRepo.Create(rule2)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		_, err := svc.GenerateTableSchema(ctx, contracts.GenerateSchemaRequest{
			APIMetadataID: shared.NewID(),
			DataStoreID:   shared.NewID(),
			TableName:     "test_table",
		})
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})

	t.Run("API metadata not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		_, err := svc.GenerateTableSchema(ctx, contracts.GenerateSchemaRequest{
			APIMetadataID: shared.NewID(),
			DataStoreID:   ds.ID,
			TableName:     "test_table",
		})
		if err == nil {
			t.Fatal("Expected error for non-existent API metadata")
		}
	})
}

func TestDataStoreApplicationService_CreateTable(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
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
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.CreateTable(ctx, schema.ID)
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}

		updated, _ := dsRepo.GetSchema(schema.ID)
		if updated.Status != datastore.SchemaStatusCreated {
			t.Errorf("Expected schema status created, got %s", updated.Status)
		}
	})

	t.Run("DDL execution failed", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()
		adapter.executeDDLErr = errors.New("DDL error")

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		schema.SetColumns([]datastore.ColumnDef{
			{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
		})
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.CreateTable(ctx, schema.ID)
		if err == nil {
			t.Fatal("Expected error when DDL execution fails")
		}

		updated, _ := dsRepo.GetSchema(schema.ID)
		if updated.Status != datastore.SchemaStatusFailed {
			t.Errorf("Expected schema status failed, got %s", updated.Status)
		}
	})

	t.Run("Schema not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.CreateTable(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent schema")
		}
	})
}

func TestDataStoreApplicationService_DropTable(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		schema.MarkCreated()
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.DropTable(ctx, schema.ID)
		if err != nil {
			t.Fatalf("DropTable failed: %v", err)
		}

		deleted, _ := dsRepo.GetSchema(schema.ID)
		if deleted != nil {
			t.Error("Schema should be deleted")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.DropTable(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent schema")
		}
	})
}

func TestDataStoreApplicationService_GetTableSchema(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test_table")
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		_, err := svc.GetTableSchema(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent schema")
		}
	})
}

func TestDataStoreApplicationService_GetTableSchemaByAPI(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		apiID := shared.NewID()
		schema := datastore.NewTableSchema(shared.NewID(), apiID, "test_table")
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		result, err := svc.GetTableSchemaByAPI(ctx, apiID)
		if err != nil {
			t.Fatalf("GetTableSchemaByAPI failed: %v", err)
		}
		if result.APIMetadataID != apiID {
			t.Errorf("Expected API ID %s, got %s", apiID, result.APIMetadataID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		_, err := svc.GetTableSchemaByAPI(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent schema")
		}
	})
}

func TestDataStoreApplicationService_ListTableSchemas(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockQuantDataStoreRepository()
	ruleRepo := NewMockMappingRuleRepository()
	dataSourceRepo := NewMockDataSourceRepository()
	adapter := NewMockQuantDBAdapter()

	dsID := shared.NewID()
	for i := 0; i < 3; i++ {
		schema := datastore.NewTableSchema(dsID, shared.NewID(), "test_table")
		dsRepo.AddSchema(schema)
	}

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test_table")
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		newCols := []datastore.ColumnDef{
			{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
		}
		err := svc.UpdateTableSchema(ctx, schema.ID, contracts.UpdateSchemaRequest{
			Columns: &newCols,
		})
		if err != nil {
			t.Fatalf("UpdateTableSchema failed: %v", err)
		}

		updated, _ := dsRepo.GetSchema(schema.ID)
		if len(updated.Columns) != 1 {
			t.Errorf("Expected 1 column, got %d", len(updated.Columns))
		}
	})

	t.Run("Cannot update created schema", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		schema := datastore.NewTableSchema(shared.NewID(), shared.NewID(), "test_table")
		schema.MarkCreated()
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		newCols := []datastore.ColumnDef{
			{Name: "id", SourceType: "int", TargetType: "INTEGER", Nullable: false},
		}
		err := svc.UpdateTableSchema(ctx, shared.NewID(), contracts.UpdateSchemaRequest{
			Columns: &newCols,
		})
		if err == nil {
			t.Fatal("Expected error for non-existent schema")
		}
	})
}

func TestDataStoreApplicationService_CreateMappingRule(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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
	ruleRepo := NewMockMappingRuleRepository()
	dataSourceRepo := NewMockDataSourceRepository()
	adapter := NewMockQuantDBAdapter()

	// Create rules
	rule1 := datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true)
	ruleRepo.Create(rule1)
	rule2 := datastore.NewDataTypeMappingRule("tushare", "int", "duckdb", "INTEGER", 100, true)
	ruleRepo.Create(rule2)

	svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

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

	t.Run("Success - table exists", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()
		adapter.tableExists = true

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		schema := datastore.NewTableSchema(ds.ID, shared.NewID(), "test_table")
		dsRepo.AddSchema(schema)

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.SyncSchemaStatus(ctx, ds.ID)
		if err != nil {
			t.Fatalf("SyncSchemaStatus failed: %v", err)
		}

		updated, _ := dsRepo.GetSchema(schema.ID)
		if updated.Status != datastore.SchemaStatusCreated {
			t.Errorf("Expected schema status created, got %s", updated.Status)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		ruleRepo := NewMockMappingRuleRepository()
		dataSourceRepo := NewMockDataSourceRepository()
		adapter := NewMockQuantDBAdapter()

		svc := impl.NewDataStoreApplicationService(dsRepo, ruleRepo, dataSourceRepo, adapter)

		err := svc.SyncSchemaStatus(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})
}
