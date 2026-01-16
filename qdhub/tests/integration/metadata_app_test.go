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
	"qdhub/internal/infrastructure/persistence/repository"
)

// MockIntegrationDocumentParserFactory is a mock parser factory for integration testing.
type MockIntegrationDocumentParserFactory struct{}

func NewMockIntegrationDocumentParserFactory() *MockIntegrationDocumentParserFactory {
	return &MockIntegrationDocumentParserFactory{}
}

func (m *MockIntegrationDocumentParserFactory) GetParser(docType metadata.DocumentType) (metadata.DocumentParser, error) {
	return &MockIntegrationDocumentParser{}, nil
}

func (m *MockIntegrationDocumentParserFactory) RegisterParser(parser metadata.DocumentParser) {}

// MockIntegrationDocumentParser is a mock parser for integration testing.
type MockIntegrationDocumentParser struct{}

func (m *MockIntegrationDocumentParser) ParseCatalog(content string) ([]metadata.APICategory, []string, error) {
	return []metadata.APICategory{
		{ID: shared.NewID(), Name: "股票数据", Description: "股票相关数据"},
	}, []string{"/api/daily"}, nil
}

func (m *MockIntegrationDocumentParser) ParseAPIDetail(content string) (*metadata.APIMetadata, error) {
	return nil, nil
}

func (m *MockIntegrationDocumentParser) SupportedType() metadata.DocumentType {
	return metadata.DocumentTypeHTML
}

// ==================== Integration Tests ====================

func TestMetadataApplicationService_Integration_CreateAndGetDataSource(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Create repositories
	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create data source
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

	// Get data source
	retrieved, err := svc.GetDataSource(ctx, ds.ID)
	if err != nil {
		t.Fatalf("GetDataSource failed: %v", err)
	}
	if retrieved.Name != req.Name {
		t.Errorf("Expected name %s, got %s", req.Name, retrieved.Name)
	}
}

func TestMetadataApplicationService_Integration_UpdateDataSource(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create data source
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Update data source
	newName := "Tushare Pro"
	newDesc := "Updated description"
	err := svc.UpdateDataSource(ctx, ds.ID, contracts.UpdateDataSourceRequest{
		Name:        &newName,
		Description: &newDesc,
	})
	if err != nil {
		t.Fatalf("UpdateDataSource failed: %v", err)
	}

	// Verify update
	updated, _ := svc.GetDataSource(ctx, ds.ID)
	if updated.Name != newName {
		t.Errorf("Expected name %s, got %s", newName, updated.Name)
	}
}

func TestMetadataApplicationService_Integration_DeleteDataSource(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create data source
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Delete data source
	err := svc.DeleteDataSource(ctx, ds.ID)
	if err != nil {
		t.Fatalf("DeleteDataSource failed: %v", err)
	}

	// Verify deletion
	_, err = svc.GetDataSource(ctx, ds.ID)
	if err == nil {
		t.Fatal("Expected error for deleted data source")
	}
}

func TestMetadataApplicationService_Integration_ListDataSources(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create multiple data sources
	for i := 0; i < 3; i++ {
		svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        "Tushare",
			Description: "Test",
			BaseURL:     "https://api.tushare.pro",
			DocURL:      "https://doc.tushare.pro",
		})
	}

	// List data sources
	sources, err := svc.ListDataSources(ctx)
	if err != nil {
		t.Fatalf("ListDataSources failed: %v", err)
	}
	if len(sources) != 3 {
		t.Errorf("Expected 3 sources, got %d", len(sources))
	}
}

func TestMetadataApplicationService_Integration_APIMetadataLifecycle(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create data source first
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Create API metadata
	apiReq := contracts.CreateAPIMetadataRequest{
		DataSourceID: ds.ID,
		Name:         "daily",
		DisplayName:  "日线行情",
		Description:  "获取股票日线行情数据",
		Endpoint:     "/api/daily",
		RequestParams: []metadata.ParamMeta{
			{Name: "ts_code", Type: "str", Required: true, Description: "股票代码"},
		},
		ResponseFields: []metadata.FieldMeta{
			{Name: "ts_code", Type: "str", Description: "股票代码", IsPrimary: true},
			{Name: "open", Type: "float", Description: "开盘价"},
		},
	}

	api, err := svc.CreateAPIMetadata(ctx, apiReq)
	if err != nil {
		t.Fatalf("CreateAPIMetadata failed: %v", err)
	}
	if api == nil {
		t.Fatal("Expected API to be non-nil")
	}

	// Get API metadata
	retrieved, err := svc.GetAPIMetadata(ctx, api.ID)
	if err != nil {
		t.Fatalf("GetAPIMetadata failed: %v", err)
	}
	if retrieved.Name != apiReq.Name {
		t.Errorf("Expected name %s, got %s", apiReq.Name, retrieved.Name)
	}

	// Update API metadata
	newDisplayName := "每日行情"
	err = svc.UpdateAPIMetadata(ctx, api.ID, contracts.UpdateAPIMetadataRequest{
		DisplayName: &newDisplayName,
	})
	if err != nil {
		t.Fatalf("UpdateAPIMetadata failed: %v", err)
	}

	// List API metadata
	apis, err := svc.ListAPIMetadataByDataSource(ctx, ds.ID)
	if err != nil {
		t.Fatalf("ListAPIMetadataByDataSource failed: %v", err)
	}
	if len(apis) != 1 {
		t.Errorf("Expected 1 API, got %d", len(apis))
	}

	// Delete API metadata
	err = svc.DeleteAPIMetadata(ctx, api.ID)
	if err != nil {
		t.Fatalf("DeleteAPIMetadata failed: %v", err)
	}

	// Verify deletion
	_, err = svc.GetAPIMetadata(ctx, api.ID)
	if err == nil {
		t.Fatal("Expected error for deleted API metadata")
	}
}

func TestMetadataApplicationService_Integration_TokenLifecycle(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create data source first
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Save token
	err := svc.SaveToken(ctx, contracts.SaveTokenRequest{
		DataSourceID: ds.ID,
		TokenValue:   "test-token-value",
	})
	if err != nil {
		t.Fatalf("SaveToken failed: %v", err)
	}

	// Get token
	token, err := svc.GetToken(ctx, ds.ID)
	if err != nil {
		t.Fatalf("GetToken failed: %v", err)
	}
	if token.TokenValue != "test-token-value" {
		t.Errorf("Expected token value 'test-token-value', got %s", token.TokenValue)
	}

	// Update token
	err = svc.SaveToken(ctx, contracts.SaveTokenRequest{
		DataSourceID: ds.ID,
		TokenValue:   "updated-token",
	})
	if err != nil {
		t.Fatalf("SaveToken (update) failed: %v", err)
	}

	// Verify update
	updated, _ := svc.GetToken(ctx, ds.ID)
	if updated.TokenValue != "updated-token" {
		t.Errorf("Expected token value 'updated-token', got %s", updated.TokenValue)
	}

	// Delete token
	err = svc.DeleteToken(ctx, ds.ID)
	if err != nil {
		t.Fatalf("DeleteToken failed: %v", err)
	}

	// Verify deletion
	_, err = svc.GetToken(ctx, ds.ID)
	if err == nil {
		t.Fatal("Expected error for deleted token")
	}
}

func TestMetadataApplicationService_Integration_ParseAndImportMetadata(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create data source first
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Parse and import metadata
	result, err := svc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
		DataSourceID: ds.ID,
		DocContent:   "<html>test content</html>",
		DocType:      metadata.DocumentTypeHTML,
	})
	if err != nil {
		t.Fatalf("ParseAndImportMetadata failed: %v", err)
	}
	if result.CategoriesCreated != 1 {
		t.Errorf("Expected 1 category created, got %d", result.CategoriesCreated)
	}
}

func TestMetadataApplicationService_Integration_DeleteDataSourceWithRelatedEntities(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()

	svc := impl.NewMetadataApplicationService(dsRepo, parserFactory)

	// Create data source
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Create API metadata
	svc.CreateAPIMetadata(ctx, contracts.CreateAPIMetadataRequest{
		DataSourceID: ds.ID,
		Name:         "daily",
		DisplayName:  "日线行情",
		Description:  "日线数据",
		Endpoint:     "/api/daily",
	})

	// Save token
	svc.SaveToken(ctx, contracts.SaveTokenRequest{
		DataSourceID: ds.ID,
		TokenValue:   "test-token",
	})

	// Delete data source (should also delete related entities)
	err := svc.DeleteDataSource(ctx, ds.ID)
	if err != nil {
		t.Fatalf("DeleteDataSource failed: %v", err)
	}

	// Verify API metadata is deleted
	apis, _ := svc.ListAPIMetadataByDataSource(ctx, ds.ID)
	if len(apis) != 0 {
		t.Errorf("Expected 0 APIs after deletion, got %d", len(apis))
	}

	// Verify token is deleted
	_, err = svc.GetToken(ctx, ds.ID)
	if err == nil {
		t.Error("Expected error for deleted token")
	}
}
