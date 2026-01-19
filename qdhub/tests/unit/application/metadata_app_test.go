package application_test

import (
	"context"
	"errors"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// MockMetaWorkflowExecutor is a mock implementation of workflow.WorkflowExecutor.
type MockMetaWorkflowExecutor struct{}

func NewMockMetaWorkflowExecutor() *MockMetaWorkflowExecutor {
	return &MockMetaWorkflowExecutor{}
}

func (m *MockMetaWorkflowExecutor) ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockMetaWorkflowExecutor) ExecuteMetadataCrawl(ctx context.Context, req workflow.MetadataCrawlRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockMetaWorkflowExecutor) ExecuteCreateTables(ctx context.Context, req workflow.CreateTablesRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockMetaWorkflowExecutor) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

func (m *MockMetaWorkflowExecutor) ExecuteRealtimeDataSync(ctx context.Context, req workflow.RealtimeDataSyncRequest) (shared.ID, error) {
	return shared.ID("mock-instance-id"), nil
}

// ==================== Mock Implementations ====================

// MockMetaDataSourceRepository is a mock implementation of metadata.DataSourceRepository.
// Following DDD, this repository handles both DataSource (aggregate root) and its child entities.
type MockMetaDataSourceRepository struct {
	sources    map[shared.ID]*metadata.DataSource
	categories map[shared.ID]*metadata.APICategory
	apis       map[shared.ID]*metadata.APIMetadata
	tokens     map[shared.ID]*metadata.Token
	createErr  error
	updateErr  error
	deleteErr  error
}

func NewMockMetaDataSourceRepository() *MockMetaDataSourceRepository {
	return &MockMetaDataSourceRepository{
		sources:    make(map[shared.ID]*metadata.DataSource),
		categories: make(map[shared.ID]*metadata.APICategory),
		apis:       make(map[shared.ID]*metadata.APIMetadata),
		tokens:     make(map[shared.ID]*metadata.Token),
	}
}

func (m *MockMetaDataSourceRepository) Create(ds *metadata.DataSource) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.sources[ds.ID] = ds
	return nil
}

func (m *MockMetaDataSourceRepository) Get(id shared.ID) (*metadata.DataSource, error) {
	ds, exists := m.sources[id]
	if !exists {
		return nil, nil
	}
	return ds, nil
}

func (m *MockMetaDataSourceRepository) Update(ds *metadata.DataSource) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.sources[ds.ID] = ds
	return nil
}

func (m *MockMetaDataSourceRepository) Delete(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.sources, id)
	return nil
}

func (m *MockMetaDataSourceRepository) List() ([]*metadata.DataSource, error) {
	result := make([]*metadata.DataSource, 0, len(m.sources))
	for _, ds := range m.sources {
		result = append(result, ds)
	}
	return result, nil
}

// Child Entity Operations (APICategory)
func (m *MockMetaDataSourceRepository) AddCategory(cat *metadata.APICategory) error {
	m.categories[cat.ID] = cat
	return nil
}

func (m *MockMetaDataSourceRepository) GetCategory(id shared.ID) (*metadata.APICategory, error) {
	cat, exists := m.categories[id]
	if !exists {
		return nil, nil
	}
	return cat, nil
}

func (m *MockMetaDataSourceRepository) ListCategoriesByDataSource(dataSourceID shared.ID) ([]*metadata.APICategory, error) {
	result := make([]*metadata.APICategory, 0)
	for _, cat := range m.categories {
		if cat.DataSourceID == dataSourceID {
			result = append(result, cat)
		}
	}
	return result, nil
}

func (m *MockMetaDataSourceRepository) UpdateCategory(cat *metadata.APICategory) error {
	m.categories[cat.ID] = cat
	return nil
}

func (m *MockMetaDataSourceRepository) DeleteCategory(id shared.ID) error {
	delete(m.categories, id)
	return nil
}

// Child Entity Operations (APIMetadata)
func (m *MockMetaDataSourceRepository) AddAPIMetadata(meta *metadata.APIMetadata) error {
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockMetaDataSourceRepository) GetAPIMetadata(id shared.ID) (*metadata.APIMetadata, error) {
	api, exists := m.apis[id]
	if !exists {
		return nil, nil
	}
	return api, nil
}

func (m *MockMetaDataSourceRepository) ListAPIMetadataByDataSource(dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.DataSourceID == dataSourceID {
			result = append(result, api)
		}
	}
	return result, nil
}

func (m *MockMetaDataSourceRepository) ListAPIMetadataByCategory(categoryID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.CategoryID != nil && *api.CategoryID == categoryID {
			result = append(result, api)
		}
	}
	return result, nil
}

func (m *MockMetaDataSourceRepository) UpdateAPIMetadata(meta *metadata.APIMetadata) error {
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockMetaDataSourceRepository) DeleteAPIMetadata(id shared.ID) error {
	delete(m.apis, id)
	return nil
}

// Child Entity Operations (Token)
func (m *MockMetaDataSourceRepository) SetToken(token *metadata.Token) error {
	m.tokens[token.ID] = token
	return nil
}

func (m *MockMetaDataSourceRepository) GetToken(id shared.ID) (*metadata.Token, error) {
	token, exists := m.tokens[id]
	if !exists {
		return nil, nil
	}
	return token, nil
}

func (m *MockMetaDataSourceRepository) GetTokenByDataSource(dataSourceID shared.ID) (*metadata.Token, error) {
	for _, token := range m.tokens {
		if token.DataSourceID == dataSourceID {
			return token, nil
		}
	}
	return nil, nil
}

func (m *MockMetaDataSourceRepository) DeleteToken(id shared.ID) error {
	delete(m.tokens, id)
	return nil
}

// Extended Query Operations
func (m *MockMetaDataSourceRepository) FindBy(conditions ...shared.QueryCondition) ([]*metadata.DataSource, error) {
	return m.List()
}

func (m *MockMetaDataSourceRepository) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*metadata.DataSource, error) {
	return m.List()
}

func (m *MockMetaDataSourceRepository) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[metadata.DataSource], error) {
	sources, _ := m.List()
	return shared.NewPageResult(sources, int64(len(sources)), pagination), nil
}

func (m *MockMetaDataSourceRepository) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[metadata.DataSource], error) {
	return m.ListWithPagination(pagination)
}

func (m *MockMetaDataSourceRepository) Count(conditions ...shared.QueryCondition) (int64, error) {
	return int64(len(m.sources)), nil
}

func (m *MockMetaDataSourceRepository) Exists(conditions ...shared.QueryCondition) (bool, error) {
	return len(m.sources) > 0, nil
}

// MockDocumentParserFactory is a mock implementation of metadata.DocumentParserFactory.
type MockDocumentParserFactory struct {
	parser    metadata.DocumentParser
	parserErr error
}

func NewMockDocumentParserFactory() *MockDocumentParserFactory {
	return &MockDocumentParserFactory{
		parser: &MockDocumentParser{},
	}
}

func (m *MockDocumentParserFactory) GetParser(docType metadata.DocumentType) (metadata.DocumentParser, error) {
	if m.parserErr != nil {
		return nil, m.parserErr
	}
	return m.parser, nil
}

func (m *MockDocumentParserFactory) RegisterParser(parser metadata.DocumentParser) {
	// No-op for mock
}

// MockDocumentParser is a mock implementation of metadata.DocumentParser.
type MockDocumentParser struct {
	categories []metadata.APICategory
	apiURLs    []string
	parseErr   error
}

func (m *MockDocumentParser) ParseCatalog(content string) ([]metadata.APICategory, []string, error) {
	if m.parseErr != nil {
		return nil, nil, m.parseErr
	}
	return m.categories, m.apiURLs, nil
}

func (m *MockDocumentParser) ParseAPIDetail(content string) (*metadata.APIMetadata, error) {
	return nil, nil
}

func (m *MockDocumentParser) SupportedType() metadata.DocumentType {
	return metadata.DocumentTypeHTML
}

// ==================== Test Cases ====================

func TestMetadataApplicationService_CreateDataSource(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		req := contracts.CreateDataSourceRequest{
			Name:        "Tushare",
			Description: "Tushare数据源",
			BaseURL:     "https://api.tushare.pro",
			DocURL:      "https://tushare.pro/document/2",
		}

		ds, err := svc.CreateDataSource(ctx, req)
		if err != nil {
			t.Fatalf("CreateDataSource failed: %v", err)
		}
		if ds == nil {
			t.Fatal("Expected data source to be non-nil")
		}
		if ds.Name != req.Name {
			t.Errorf("Expected name %s, got %s", req.Name, ds.Name)
		}
	})

	t.Run("Repository error", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		dsRepo.createErr = errors.New("create error")
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		req := contracts.CreateDataSourceRequest{
			Name:        "Tushare",
			Description: "Tushare数据源",
			BaseURL:     "https://api.tushare.pro",
			DocURL:      "https://tushare.pro/document/2",
		}

		_, err := svc.CreateDataSource(ctx, req)
		if err == nil {
			t.Fatal("Expected error for repository failure")
		}
	})
}

func TestMetadataApplicationService_GetDataSource(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		result, err := svc.GetDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("GetDataSource failed: %v", err)
		}
		if result.ID != ds.ID {
			t.Errorf("Expected ID %s, got %s", ds.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		_, err := svc.GetDataSource(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}

func TestMetadataApplicationService_UpdateDataSource(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		newName := "Tushare Pro"
		newDesc := "Updated description"
		err := svc.UpdateDataSource(ctx, ds.ID, contracts.UpdateDataSourceRequest{
			Name:        &newName,
			Description: &newDesc,
		})
		if err != nil {
			t.Fatalf("UpdateDataSource failed: %v", err)
		}

		updated, _ := dsRepo.Get(ds.ID)
		if updated.Name != newName {
			t.Errorf("Expected name %s, got %s", newName, updated.Name)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		newName := "Updated"
		err := svc.UpdateDataSource(ctx, shared.NewID(), contracts.UpdateDataSourceRequest{
			Name: &newName,
		})
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}

func TestMetadataApplicationService_DeleteDataSource(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.DeleteDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("DeleteDataSource failed: %v", err)
		}

		deleted, _ := dsRepo.Get(ds.ID)
		if deleted != nil {
			t.Error("Data source should be deleted")
		}
	})

	t.Run("Deletes related entities", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		// Create related API and category
		api := metadata.NewAPIMetadata(ds.ID, "daily", "日线行情", "日线数据", "/api/daily")
		dsRepo.AddAPIMetadata(api)

		cat := metadata.NewAPICategory(ds.ID, "股票数据", "股票相关数据", "/stock", nil, 1)
		dsRepo.AddCategory(cat)

		token := metadata.NewToken(ds.ID, "test-token", nil)
		dsRepo.SetToken(token)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.DeleteDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("DeleteDataSource failed: %v", err)
		}

		// Verify data source is deleted
		deleted, _ := dsRepo.Get(ds.ID)
		if deleted != nil {
			t.Error("Data source should be deleted")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.DeleteDataSource(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}

func TestMetadataApplicationService_ListDataSources(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockMetaDataSourceRepository()
	parserFactory := NewMockDocumentParserFactory()

	// Create multiple data sources
	for i := 0; i < 3; i++ {
		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)
	}

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

	sources, err := svc.ListDataSources(ctx)
	if err != nil {
		t.Fatalf("ListDataSources failed: %v", err)
	}
	if len(sources) != 3 {
		t.Errorf("Expected 3 sources, got %d", len(sources))
	}
}

func TestMetadataApplicationService_CreateAPIMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		req := contracts.CreateAPIMetadataRequest{
			DataSourceID: ds.ID,
			Name:         "daily",
			DisplayName:  "日线行情",
			Description:  "获取股票日线行情数据",
			Endpoint:     "/api/daily",
			RequestParams: []metadata.ParamMeta{
				{Name: "ts_code", Type: "str", Required: true, Description: "股票代码"},
				{Name: "start_date", Type: "str", Required: false, Description: "开始日期"},
			},
			ResponseFields: []metadata.FieldMeta{
				{Name: "ts_code", Type: "str", Description: "股票代码", IsPrimary: true},
				{Name: "trade_date", Type: "str", Description: "交易日期", IsPrimary: true},
				{Name: "open", Type: "float", Description: "开盘价"},
			},
		}

		api, err := svc.CreateAPIMetadata(ctx, req)
		if err != nil {
			t.Fatalf("CreateAPIMetadata failed: %v", err)
		}
		if api == nil {
			t.Fatal("Expected API to be non-nil")
		}
		if api.Name != req.Name {
			t.Errorf("Expected name %s, got %s", req.Name, api.Name)
		}
		if len(api.RequestParams) != 2 {
			t.Errorf("Expected 2 request params, got %d", len(api.RequestParams))
		}
		if len(api.ResponseFields) != 3 {
			t.Errorf("Expected 3 response fields, got %d", len(api.ResponseFields))
		}
	})

	t.Run("DataSource not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		req := contracts.CreateAPIMetadataRequest{
			DataSourceID: shared.NewID(),
			Name:         "daily",
			DisplayName:  "日线行情",
			Description:  "获取股票日线行情数据",
			Endpoint:     "/api/daily",
		}

		_, err := svc.CreateAPIMetadata(ctx, req)
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}

func TestMetadataApplicationService_GetAPIMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		api := metadata.NewAPIMetadata(shared.NewID(), "daily", "日线行情", "日线数据", "/api/daily")
		dsRepo.AddAPIMetadata(api)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		result, err := svc.GetAPIMetadata(ctx, api.ID)
		if err != nil {
			t.Fatalf("GetAPIMetadata failed: %v", err)
		}
		if result.ID != api.ID {
			t.Errorf("Expected ID %s, got %s", api.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		_, err := svc.GetAPIMetadata(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent API metadata")
		}
	})
}

func TestMetadataApplicationService_UpdateAPIMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		api := metadata.NewAPIMetadata(shared.NewID(), "daily", "日线行情", "日线数据", "/api/daily")
		// API must have at least one response field for validation
		api.SetResponseFields([]metadata.FieldMeta{
			{Name: "ts_code", Type: "str", Description: "股票代码", IsPrimary: true},
		})
		dsRepo.AddAPIMetadata(api)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		newDisplayName := "每日行情"
		newDesc := "更新后的描述"
		err := svc.UpdateAPIMetadata(ctx, api.ID, contracts.UpdateAPIMetadataRequest{
			DisplayName: &newDisplayName,
			Description: &newDesc,
		})
		if err != nil {
			t.Fatalf("UpdateAPIMetadata failed: %v", err)
		}

		updated, _ := dsRepo.GetAPIMetadata(api.ID)
		if updated.DisplayName != newDisplayName {
			t.Errorf("Expected display name %s, got %s", newDisplayName, updated.DisplayName)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		newDisplayName := "更新"
		err := svc.UpdateAPIMetadata(ctx, shared.NewID(), contracts.UpdateAPIMetadataRequest{
			DisplayName: &newDisplayName,
		})
		if err == nil {
			t.Fatal("Expected error for non-existent API metadata")
		}
	})
}

func TestMetadataApplicationService_DeleteAPIMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		api := metadata.NewAPIMetadata(shared.NewID(), "daily", "日线行情", "日线数据", "/api/daily")
		dsRepo.AddAPIMetadata(api)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.DeleteAPIMetadata(ctx, api.ID)
		if err != nil {
			t.Fatalf("DeleteAPIMetadata failed: %v", err)
		}

		deleted, _ := dsRepo.GetAPIMetadata(api.ID)
		if deleted != nil {
			t.Error("API metadata should be deleted")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.DeleteAPIMetadata(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent API metadata")
		}
	})
}

func TestMetadataApplicationService_ListAPIMetadataByDataSource(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockMetaDataSourceRepository()
	parserFactory := NewMockDocumentParserFactory()

	dsID := shared.NewID()
	for i := 0; i < 3; i++ {
		api := metadata.NewAPIMetadata(dsID, "api", "API", "API Desc", "/api")
		dsRepo.AddAPIMetadata(api)
	}

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

	apis, err := svc.ListAPIMetadataByDataSource(ctx, dsID)
	if err != nil {
		t.Fatalf("ListAPIMetadataByDataSource failed: %v", err)
	}
	if len(apis) != 3 {
		t.Errorf("Expected 3 APIs, got %d", len(apis))
	}
}

func TestMetadataApplicationService_ParseAndImportMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("Success - triggers async workflow", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		// ParseAndImportMetadata now triggers an async workflow instead of synchronous parsing.
		// The result will have zeros because the actual parsing happens asynchronously.
		result, err := svc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
			DataSourceID: ds.ID,
			DocContent:   "<html>...</html>",
			DocType:      metadata.DocumentTypeHTML,
		})
		if err != nil {
			t.Fatalf("ParseAndImportMetadata failed: %v", err)
		}
		if result == nil {
			t.Fatal("Expected result to be non-nil")
		}
		// Since the workflow is async, the result fields are initially 0
		// The actual counts will be updated after workflow completion
		if result.CategoriesCreated != 0 {
			t.Errorf("Expected 0 categories created (async workflow), got %d", result.CategoriesCreated)
		}
		if result.APIsCreated != 0 {
			t.Errorf("Expected 0 APIs created (async workflow), got %d", result.APIsCreated)
		}
	})

	t.Run("DataSource not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		_, err := svc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
			DataSourceID: shared.NewID(),
			DocContent:   "<html>...</html>",
			DocType:      metadata.DocumentTypeHTML,
		})
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}

func TestMetadataApplicationService_SaveToken(t *testing.T) {
	ctx := context.Background()

	t.Run("Create new token", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.SaveToken(ctx, contracts.SaveTokenRequest{
			DataSourceID: ds.ID,
			TokenValue:   "new-token-value",
		})
		if err != nil {
			t.Fatalf("SaveToken failed: %v", err)
		}

		token, _ := dsRepo.GetTokenByDataSource(ds.ID)
		if token == nil {
			t.Fatal("Token should be created")
		}
		if token.TokenValue != "new-token-value" {
			t.Errorf("Expected token value 'new-token-value', got %s", token.TokenValue)
		}
	})

	t.Run("Update existing token", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		// Create existing token
		existingToken := metadata.NewToken(ds.ID, "old-token", nil)
		dsRepo.SetToken(existingToken)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.SaveToken(ctx, contracts.SaveTokenRequest{
			DataSourceID: ds.ID,
			TokenValue:   "updated-token",
		})
		if err != nil {
			t.Fatalf("SaveToken failed: %v", err)
		}

		token, _ := dsRepo.GetTokenByDataSource(ds.ID)
		if token.TokenValue != "updated-token" {
			t.Errorf("Expected token value 'updated-token', got %s", token.TokenValue)
		}
	})

	t.Run("DataSource not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.SaveToken(ctx, contracts.SaveTokenRequest{
			DataSourceID: shared.NewID(),
			TokenValue:   "test-token",
		})
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}

func TestMetadataApplicationService_GetToken(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		dsID := shared.NewID()
		token := metadata.NewToken(dsID, "test-token", nil)
		dsRepo.SetToken(token)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		result, err := svc.GetToken(ctx, dsID)
		if err != nil {
			t.Fatalf("GetToken failed: %v", err)
		}
		if result.TokenValue != "test-token" {
			t.Errorf("Expected token value 'test-token', got %s", result.TokenValue)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		_, err := svc.GetToken(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent token")
		}
	})
}

func TestMetadataApplicationService_DeleteToken(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		dsID := shared.NewID()
		token := metadata.NewToken(dsID, "test-token", nil)
		dsRepo.SetToken(token)

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.DeleteToken(ctx, dsID)
		if err != nil {
			t.Fatalf("DeleteToken failed: %v", err)
		}

		deleted, _ := dsRepo.GetTokenByDataSource(dsID)
		if deleted != nil {
			t.Error("Token should be deleted")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, parserFactory, NewMockMetaWorkflowExecutor())

		err := svc.DeleteToken(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent token")
		}
	})
}
