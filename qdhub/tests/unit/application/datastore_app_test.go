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
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
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

// MockSyncPlanRepository is a minimal mock for sync.SyncPlanRepository (used by DataStoreApplicationService.DeleteDataStore).
type MockSyncPlanRepository struct {
	existsByDataStoreID bool // Exists(shared.Eq("data_store_id", id)) returns this
}

func (m *MockSyncPlanRepository) Create(plan *sync.SyncPlan) error                    { return nil }
func (m *MockSyncPlanRepository) Get(id shared.ID) (*sync.SyncPlan, error)            { return nil, nil }
func (m *MockSyncPlanRepository) Update(plan *sync.SyncPlan) error                      { return nil }
func (m *MockSyncPlanRepository) Delete(id shared.ID) error                           { return nil }
func (m *MockSyncPlanRepository) List() ([]*sync.SyncPlan, error)                      { return nil, nil }
func (m *MockSyncPlanRepository) FindBy(conditions ...shared.QueryCondition) ([]*sync.SyncPlan, error) {
	return nil, nil
}
func (m *MockSyncPlanRepository) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*sync.SyncPlan, error) {
	return nil, nil
}
func (m *MockSyncPlanRepository) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[sync.SyncPlan], error) {
	return shared.NewPageResult([]*sync.SyncPlan{}, 0, pagination), nil
}
func (m *MockSyncPlanRepository) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[sync.SyncPlan], error) {
	return shared.NewPageResult([]*sync.SyncPlan{}, 0, pagination), nil
}
func (m *MockSyncPlanRepository) Count(conditions ...shared.QueryCondition) (int64, error) {
	if m.existsByDataStoreID {
		return 1, nil
	}
	return 0, nil
}
func (m *MockSyncPlanRepository) Exists(conditions ...shared.QueryCondition) (bool, error) {
	return m.existsByDataStoreID, nil
}
func (m *MockSyncPlanRepository) AddTask(task *sync.SyncTask) error                          { return nil }
func (m *MockSyncPlanRepository) GetTask(id shared.ID) (*sync.SyncTask, error)              { return nil, nil }
func (m *MockSyncPlanRepository) GetTasksByPlan(planID shared.ID) ([]*sync.SyncTask, error) { return nil, nil }
func (m *MockSyncPlanRepository) UpdateTask(task *sync.SyncTask) error                      { return nil }
func (m *MockSyncPlanRepository) DeleteTasksByPlan(planID shared.ID) error                  { return nil }
func (m *MockSyncPlanRepository) AddPlanExecution(exec *sync.SyncExecution) error           { return nil }
func (m *MockSyncPlanRepository) GetPlanExecution(id shared.ID) (*sync.SyncExecution, error) { return nil, nil }
func (m *MockSyncPlanRepository) GetExecutionsByPlan(planID shared.ID) ([]*sync.SyncExecution, error) {
	return nil, nil
}
func (m *MockSyncPlanRepository) GetExecutionsByPlanPaged(planID shared.ID, limit, offset int) ([]*sync.SyncExecution, int, error) {
	return nil, 0, nil
}
func (m *MockSyncPlanRepository) GetExecutionByWorkflowInstID(workflowInstID string) (*sync.SyncExecution, error) {
	return nil, nil
}
func (m *MockSyncPlanRepository) UpdatePlanExecution(exec *sync.SyncExecution) error { return nil }
func (m *MockSyncPlanRepository) AddExecutionDetail(detail *sync.SyncExecutionDetail) error {
	return nil
}
func (m *MockSyncPlanRepository) GetExecutionDetailsByExecutionID(executionID shared.ID) ([]*sync.SyncExecutionDetail, error) {
	return nil, nil
}
func (m *MockSyncPlanRepository) GetByDataSource(dataSourceID shared.ID) ([]*sync.SyncPlan, error) {
	return nil, nil
}
func (m *MockSyncPlanRepository) GetEnabledPlans() ([]*sync.SyncPlan, error) { return nil, nil }
func (m *MockSyncPlanRepository) GetByStatus(status sync.PlanStatus) ([]*sync.SyncPlan, error) {
	return nil, nil
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

func (m *MockQuantDBAdapter) ListTables(ctx context.Context, ds *datastore.QuantDataStore) ([]string, error) {
	return nil, nil
}

func (m *MockQuantDBAdapter) Query(ctx context.Context, ds *datastore.QuantDataStore, sql string, args ...any) ([]map[string]any, error) {
	return nil, nil
}

// MockWorkflowExecutor is a simple mock implementation of workflow.WorkflowExecutor.
// Only used for testing DataStoreApplicationService, so most methods return mock values.
type MockWorkflowExecutor struct{}

func NewMockWorkflowExecutor() *MockWorkflowExecutor {
	return &MockWorkflowExecutor{}
}

func (m *MockWorkflowExecutor) ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockWorkflowExecutor) ExecuteMetadataCrawl(ctx context.Context, req workflow.MetadataCrawlRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockWorkflowExecutor) ExecuteCreateTables(ctx context.Context, req workflow.CreateTablesRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockWorkflowExecutor) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockWorkflowExecutor) ExecuteRealtimeDataSync(ctx context.Context, req workflow.RealtimeDataSyncRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockWorkflowExecutor) ExecuteFromExecutionGraph(ctx context.Context, req workflow.ExecutionGraphSyncRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

// ==================== Test Cases ====================

func TestDataStoreApplicationService_CreateDataStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockQuantDataStoreRepository()
		dataSourceRepo := NewMockDataSourceRepository()

		workflowSvc := NewMockWorkflowExecutor()
		syncPlanRepo := &MockSyncPlanRepository{}
		svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowSvc, nil)

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
		dataSourceRepo := NewMockDataSourceRepository()

		workflowSvc := NewMockWorkflowExecutor()
		syncPlanRepo := &MockSyncPlanRepository{}
		svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowSvc, nil)

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
		dataSourceRepo := NewMockDataSourceRepository()

		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)

		workflowSvc := NewMockWorkflowExecutor()
		syncPlanRepo := &MockSyncPlanRepository{}
		svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowSvc, nil)

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
		dataSourceRepo := NewMockDataSourceRepository()

		workflowSvc := NewMockWorkflowExecutor()
		syncPlanRepo := &MockSyncPlanRepository{}
		svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowSvc, nil)

		_, err := svc.GetDataStore(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data store")
		}
	})
}

func TestDataStoreApplicationService_ListDataStores(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockQuantDataStoreRepository()
	dataSourceRepo := NewMockDataSourceRepository()

	// Create multiple data stores
	for i := 0; i < 3; i++ {
		ds := datastore.NewQuantDataStore("Test", "Desc", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		dsRepo.Create(ds)
	}

	workflowSvc := NewMockWorkflowExecutor()
	syncPlanRepo := &MockSyncPlanRepository{}
	svc := impl.NewDataStoreApplicationService(dsRepo, dataSourceRepo, syncPlanRepo, workflowSvc, nil)

	stores, err := svc.ListDataStores(ctx)
	if err != nil {
		t.Fatalf("ListDataStores failed: %v", err)
	}
	if len(stores) != 3 {
		t.Errorf("Expected 3 stores, got %d", len(stores))
	}
}

// Note: Tests for deleted methods (UpdateDataStore, DeleteDataStore, TestConnection, 
// GenerateTableSchema, CreateTable, DropTable, GetTableSchema, GetTableSchemaByAPI,
// ListTableSchemas, UpdateTableSchema, SyncSchemaStatus, CreateMappingRule, GetMappingRules)
// have been removed as these methods are no longer part of the DataStoreApplicationService interface.
