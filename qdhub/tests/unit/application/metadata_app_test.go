package application_test

import (
	"context"
	"errors"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// ==================== Mock Implementations ====================

// MockDataSourceRepository is a mock implementation of metadata.DataSourceRepository.
type MockDataSourceRepository struct {
	sources   map[shared.ID]*metadata.DataSource
	createErr error
	updateErr error
	deleteErr error
}

func NewMockDataSourceRepository() *MockDataSourceRepository {
	return &MockDataSourceRepository{
		sources: make(map[shared.ID]*metadata.DataSource),
	}
}

func (m *MockDataSourceRepository) Create(ds *metadata.DataSource) error {
	if m.createErr != nil {
		return m.createErr
	}
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
	if m.updateErr != nil {
		return m.updateErr
	}
	m.sources[ds.ID] = ds
	return nil
}

func (m *MockDataSourceRepository) Delete(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
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

// MockAPICategoryRepository is a mock implementation of metadata.APICategoryRepository.
type MockAPICategoryRepository struct {
	categories map[shared.ID]*metadata.APICategory
	createErr  error
}

func NewMockAPICategoryRepository() *MockAPICategoryRepository {
	return &MockAPICategoryRepository{
		categories: make(map[shared.ID]*metadata.APICategory),
	}
}

func (m *MockAPICategoryRepository) Create(cat *metadata.APICategory) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.categories[cat.ID] = cat
	return nil
}

func (m *MockAPICategoryRepository) Get(id shared.ID) (*metadata.APICategory, error) {
	cat, exists := m.categories[id]
	if !exists {
		return nil, nil
	}
	return cat, nil
}

func (m *MockAPICategoryRepository) Update(cat *metadata.APICategory) error {
	m.categories[cat.ID] = cat
	return nil
}

func (m *MockAPICategoryRepository) Delete(id shared.ID) error {
	delete(m.categories, id)
	return nil
}

func (m *MockAPICategoryRepository) ListByDataSource(dataSourceID shared.ID) ([]*metadata.APICategory, error) {
	result := make([]*metadata.APICategory, 0)
	for _, cat := range m.categories {
		if cat.DataSourceID == dataSourceID {
			result = append(result, cat)
		}
	}
	return result, nil
}

// MockMetaAPIMetadataRepository is a mock for metadata.APIMetadataRepository.
type MockMetaAPIMetadataRepository struct {
	apis      map[shared.ID]*metadata.APIMetadata
	createErr error
	updateErr error
	deleteErr error
}

func NewMockMetaAPIMetadataRepository() *MockMetaAPIMetadataRepository {
	return &MockMetaAPIMetadataRepository{
		apis: make(map[shared.ID]*metadata.APIMetadata),
	}
}

func (m *MockMetaAPIMetadataRepository) Create(meta *metadata.APIMetadata) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockMetaAPIMetadataRepository) Get(id shared.ID) (*metadata.APIMetadata, error) {
	api, exists := m.apis[id]
	if !exists {
		return nil, nil
	}
	return api, nil
}

func (m *MockMetaAPIMetadataRepository) Update(meta *metadata.APIMetadata) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.apis[meta.ID] = meta
	return nil
}

func (m *MockMetaAPIMetadataRepository) Delete(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.apis, id)
	return nil
}

func (m *MockMetaAPIMetadataRepository) ListByDataSource(dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.DataSourceID == dataSourceID {
			result = append(result, api)
		}
	}
	return result, nil
}

func (m *MockMetaAPIMetadataRepository) ListByCategory(categoryID shared.ID) ([]*metadata.APIMetadata, error) {
	result := make([]*metadata.APIMetadata, 0)
	for _, api := range m.apis {
		if api.CategoryID != nil && *api.CategoryID == categoryID {
			result = append(result, api)
		}
	}
	return result, nil
}

// MockTokenRepository is a mock implementation of metadata.TokenRepository.
type MockTokenRepository struct {
	tokens    map[shared.ID]*metadata.Token
	createErr error
	updateErr error
	deleteErr error
}

func NewMockTokenRepository() *MockTokenRepository {
	return &MockTokenRepository{
		tokens: make(map[shared.ID]*metadata.Token),
	}
}

func (m *MockTokenRepository) Create(token *metadata.Token) error {
	if m.createErr != nil {
		return m.createErr
	}
	m.tokens[token.ID] = token
	return nil
}

func (m *MockTokenRepository) Get(id shared.ID) (*metadata.Token, error) {
	token, exists := m.tokens[id]
	if !exists {
		return nil, nil
	}
	return token, nil
}

func (m *MockTokenRepository) GetByDataSource(dataSourceID shared.ID) (*metadata.Token, error) {
	for _, token := range m.tokens {
		if token.DataSourceID == dataSourceID {
			return token, nil
		}
	}
	return nil, nil
}

func (m *MockTokenRepository) Update(token *metadata.Token) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	m.tokens[token.ID] = token
	return nil
}

func (m *MockTokenRepository) Delete(id shared.ID) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.tokens, id)
	return nil
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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		dsRepo.createErr = errors.New("create error")
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		result, err := svc.GetDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("GetDataSource failed: %v", err)
		}
		if result.ID != ds.ID {
			t.Errorf("Expected ID %s, got %s", ds.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		_, err := svc.GetDataSource(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}

func TestMetadataApplicationService_UpdateDataSource(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		// Create related API and category
		api := metadata.NewAPIMetadata(ds.ID, "daily", "日线行情", "日线数据", "/api/daily")
		apiRepo.Create(api)

		cat := metadata.NewAPICategory(ds.ID, "股票数据", "股票相关数据", "/stock", nil, 1)
		catRepo.Create(cat)

		token := metadata.NewToken(ds.ID, "test-token", nil)
		tokenRepo.Create(token)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		err := svc.DeleteDataSource(ctx, ds.ID)
		if err != nil {
			t.Fatalf("DeleteDataSource failed: %v", err)
		}

		// Verify related entities are deleted
		apis, _ := apiRepo.ListByDataSource(ds.ID)
		if len(apis) != 0 {
			t.Error("Related APIs should be deleted")
		}

		cats, _ := catRepo.ListByDataSource(ds.ID)
		if len(cats) != 0 {
			t.Error("Related categories should be deleted")
		}
	})
}

func TestMetadataApplicationService_ListDataSources(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockDataSourceRepository()
	catRepo := NewMockAPICategoryRepository()
	apiRepo := NewMockMetaAPIMetadataRepository()
	tokenRepo := NewMockTokenRepository()
	parserFactory := NewMockDocumentParserFactory()

	// Create multiple data sources
	for i := 0; i < 3; i++ {
		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)
	}

	svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		api := metadata.NewAPIMetadata(shared.NewID(), "daily", "日线行情", "日线数据", "/api/daily")
		apiRepo.Create(api)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		result, err := svc.GetAPIMetadata(ctx, api.ID)
		if err != nil {
			t.Fatalf("GetAPIMetadata failed: %v", err)
		}
		if result.ID != api.ID {
			t.Errorf("Expected ID %s, got %s", api.ID, result.ID)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		_, err := svc.GetAPIMetadata(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent API metadata")
		}
	})
}

func TestMetadataApplicationService_UpdateAPIMetadata(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		api := metadata.NewAPIMetadata(shared.NewID(), "daily", "日线行情", "日线数据", "/api/daily")
		apiRepo.Create(api)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		newDisplayName := "每日行情"
		newDesc := "更新后的描述"
		err := svc.UpdateAPIMetadata(ctx, api.ID, contracts.UpdateAPIMetadataRequest{
			DisplayName: &newDisplayName,
			Description: &newDesc,
		})
		if err != nil {
			t.Fatalf("UpdateAPIMetadata failed: %v", err)
		}

		updated, _ := apiRepo.Get(api.ID)
		if updated.DisplayName != newDisplayName {
			t.Errorf("Expected display name %s, got %s", newDisplayName, updated.DisplayName)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		api := metadata.NewAPIMetadata(shared.NewID(), "daily", "日线行情", "日线数据", "/api/daily")
		apiRepo.Create(api)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		err := svc.DeleteAPIMetadata(ctx, api.ID)
		if err != nil {
			t.Fatalf("DeleteAPIMetadata failed: %v", err)
		}

		deleted, _ := apiRepo.Get(api.ID)
		if deleted != nil {
			t.Error("API metadata should be deleted")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		err := svc.DeleteAPIMetadata(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent API metadata")
		}
	})
}

func TestMetadataApplicationService_ListAPIMetadataByDataSource(t *testing.T) {
	ctx := context.Background()

	dsRepo := NewMockDataSourceRepository()
	catRepo := NewMockAPICategoryRepository()
	apiRepo := NewMockMetaAPIMetadataRepository()
	tokenRepo := NewMockTokenRepository()
	parserFactory := NewMockDocumentParserFactory()

	dsID := shared.NewID()
	for i := 0; i < 3; i++ {
		api := metadata.NewAPIMetadata(dsID, "api", "API", "API Desc", "/api")
		apiRepo.Create(api)
	}

	svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()

		mockParser := &MockDocumentParser{
			categories: []metadata.APICategory{
				{ID: shared.NewID(), Name: "股票数据", Description: "股票相关数据"},
				{ID: shared.NewID(), Name: "期货数据", Description: "期货相关数据"},
			},
			apiURLs: []string{"/api/daily", "/api/weekly"},
		}
		parserFactory := &MockDocumentParserFactory{parser: mockParser}

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		result, err := svc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
			DataSourceID: ds.ID,
			DocContent:   "<html>...</html>",
			DocType:      metadata.DocumentTypeHTML,
		})
		if err != nil {
			t.Fatalf("ParseAndImportMetadata failed: %v", err)
		}
		if result.CategoriesCreated != 2 {
			t.Errorf("Expected 2 categories created, got %d", result.CategoriesCreated)
		}
	})

	t.Run("DataSource not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		err := svc.SaveToken(ctx, contracts.SaveTokenRequest{
			DataSourceID: ds.ID,
			TokenValue:   "new-token-value",
		})
		if err != nil {
			t.Fatalf("SaveToken failed: %v", err)
		}

		token, _ := tokenRepo.GetByDataSource(ds.ID)
		if token == nil {
			t.Fatal("Token should be created")
		}
		if token.TokenValue != "new-token-value" {
			t.Errorf("Expected token value 'new-token-value', got %s", token.TokenValue)
		}
	})

	t.Run("Update existing token", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		ds := metadata.NewDataSource("Tushare", "Desc", "https://api.tushare.pro", "https://doc.tushare.pro")
		dsRepo.Create(ds)

		// Create existing token
		existingToken := metadata.NewToken(ds.ID, "old-token", nil)
		tokenRepo.Create(existingToken)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		err := svc.SaveToken(ctx, contracts.SaveTokenRequest{
			DataSourceID: ds.ID,
			TokenValue:   "updated-token",
		})
		if err != nil {
			t.Fatalf("SaveToken failed: %v", err)
		}

		token, _ := tokenRepo.GetByDataSource(ds.ID)
		if token.TokenValue != "updated-token" {
			t.Errorf("Expected token value 'updated-token', got %s", token.TokenValue)
		}
	})

	t.Run("DataSource not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

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
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		dsID := shared.NewID()
		token := metadata.NewToken(dsID, "test-token", nil)
		tokenRepo.Create(token)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		result, err := svc.GetToken(ctx, dsID)
		if err != nil {
			t.Fatalf("GetToken failed: %v", err)
		}
		if result.TokenValue != "test-token" {
			t.Errorf("Expected token value 'test-token', got %s", result.TokenValue)
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		_, err := svc.GetToken(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent token")
		}
	})
}

func TestMetadataApplicationService_DeleteToken(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		dsID := shared.NewID()
		token := metadata.NewToken(dsID, "test-token", nil)
		tokenRepo.Create(token)

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		err := svc.DeleteToken(ctx, dsID)
		if err != nil {
			t.Fatalf("DeleteToken failed: %v", err)
		}

		deleted, _ := tokenRepo.GetByDataSource(dsID)
		if deleted != nil {
			t.Error("Token should be deleted")
		}
	})

	t.Run("Not found", func(t *testing.T) {
		dsRepo := NewMockDataSourceRepository()
		catRepo := NewMockAPICategoryRepository()
		apiRepo := NewMockMetaAPIMetadataRepository()
		tokenRepo := NewMockTokenRepository()
		parserFactory := NewMockDocumentParserFactory()

		svc := impl.NewMetadataApplicationService(dsRepo, catRepo, apiRepo, tokenRepo, parserFactory)

		err := svc.DeleteToken(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent token")
		}
	})
}
