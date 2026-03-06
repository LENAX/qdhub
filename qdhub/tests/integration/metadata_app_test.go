//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence/repository"
)

// MockMetadataWorkflowExecutor is a mock workflow executor for metadata integration testing.
type MockMetadataWorkflowExecutor struct{}

func (m *MockMetadataWorkflowExecutor) ExecuteBuiltInWorkflow(ctx context.Context, name string, params map[string]interface{}) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockMetadataWorkflowExecutor) ExecuteMetadataCrawl(ctx context.Context, req workflow.MetadataCrawlRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockMetadataWorkflowExecutor) ExecuteCreateTables(ctx context.Context, req workflow.CreateTablesRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockMetadataWorkflowExecutor) ExecuteBatchDataSync(ctx context.Context, req workflow.BatchDataSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockMetadataWorkflowExecutor) ExecuteRealtimeDataSync(ctx context.Context, req workflow.RealtimeDataSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

func (m *MockMetadataWorkflowExecutor) ExecuteFromExecutionGraph(ctx context.Context, req workflow.ExecutionGraphSyncRequest) (shared.ID, error) {
	return shared.NewID(), nil
}

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

func (m *MockIntegrationDocumentParser) ParseCatalog(content string) ([]metadata.APICategory, []string, []*shared.ID, error) {
	return []metadata.APICategory{
		{ID: shared.NewID(), Name: "股票数据", Description: "股票相关数据"},
	}, []string{"/api/daily"}, nil, nil
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
	metadataRepo := repository.NewMetadataRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()
	workflowExecutor := &MockMetadataWorkflowExecutor{}

	svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, workflowExecutor, nil)

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

func TestMetadataApplicationService_Integration_ListDataSources(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()
	workflowExecutor := &MockMetadataWorkflowExecutor{}

	svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, workflowExecutor, nil)

	// Create multiple data sources
	for i := 0; i < 3; i++ {
		_, err := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name:        fmt.Sprintf("Tushare-%d", i),
			Description: "Test",
			BaseURL:     "https://api.tushare.pro",
			DocURL:      "https://doc.tushare.pro",
		})
		if err != nil {
			t.Fatalf("CreateDataSource failed: %v", err)
		}
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

func TestMetadataApplicationService_Integration_TokenLifecycle(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()
	workflowExecutor := &MockMetadataWorkflowExecutor{}

	svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, workflowExecutor, nil)

	// Create data source first
	ds, err := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})
	if err != nil {
		t.Fatalf("CreateDataSource failed: %v", err)
	}

	// Save token
	err = svc.SaveToken(ctx, contracts.SaveTokenRequest{
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
}

func TestMetadataApplicationService_Integration_ParseAndImportMetadata(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()
	workflowExecutor := &MockMetadataWorkflowExecutor{}

	svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, workflowExecutor, nil)

	// Create data source first
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Parse and import metadata (now async via workflow)
	result, err := svc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
		DataSourceID: ds.ID,
		DocContent:   "<html>test content</html>",
		DocType:      metadata.DocumentTypeHTML,
	})
	if err != nil {
		t.Fatalf("ParseAndImportMetadata failed: %v", err)
	}
	// Result shows 0 because workflow is async - just verify it returns without error
	if result == nil {
		t.Fatal("Expected result to be non-nil")
	}
	// Instance ID should be set
	if result.InstanceID.IsEmpty() {
		t.Error("Expected non-empty instance ID")
	}
}

func TestMetadataApplicationService_Integration_ParseAndImportMetadata_DataSourceNotFound(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()
	workflowExecutor := &MockMetadataWorkflowExecutor{}

	svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, workflowExecutor, nil)

	// Try to parse with non-existent data source
	_, err := svc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
		DataSourceID: shared.NewID(), // Non-existent
		DocContent:   "<html>test content</html>",
		DocType:      metadata.DocumentTypeHTML,
	})
	if err == nil {
		t.Error("Expected error for non-existent data source")
	}
}

func TestMetadataApplicationService_Integration_APISyncStrategy(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	dsRepo := repository.NewDataSourceRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)
	parserFactory := NewMockIntegrationDocumentParserFactory()
	workflowExecutor := &MockMetadataWorkflowExecutor{}

	svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, workflowExecutor, nil)

	// Create data source first
	ds, _ := svc.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
		Name:        "Tushare",
		Description: "Test",
		BaseURL:     "https://api.tushare.pro",
		DocURL:      "https://doc.tushare.pro",
	})

	// Create API metadata using direct repository
	api := metadata.NewAPIMetadata(ds.ID, "daily", "日线行情", "日线数据", "/api/daily")
	if err := metadataRepo.SaveAPIMetadata(ctx, api); err != nil {
		t.Fatalf("Failed to create API metadata: %v", err)
	}

	// Create API sync strategy
	strategy, err := svc.CreateAPISyncStrategy(ctx, contracts.CreateAPISyncStrategyRequest{
		DataSourceID:     ds.ID,
		APIName:          "daily",
		PreferredParam:   metadata.SyncParamTradeDate,
		SupportDateRange: true,
		Description:      "Test strategy",
	})
	if err != nil {
		t.Fatalf("CreateAPISyncStrategy failed: %v", err)
	}
	if strategy == nil {
		t.Fatal("Expected strategy to be non-nil")
	}

	// Get API sync strategy
	retrieved, err := svc.GetAPISyncStrategy(ctx, contracts.GetAPISyncStrategyRequest{
		ID: &strategy.ID,
	})
	if err != nil {
		t.Fatalf("GetAPISyncStrategy failed: %v", err)
	}
	if retrieved.APIName != "daily" {
		t.Errorf("Expected API name 'daily', got %s", retrieved.APIName)
	}

	// List API sync strategies
	strategies, err := svc.ListAPISyncStrategies(ctx, ds.ID)
	if err != nil {
		t.Fatalf("ListAPISyncStrategies failed: %v", err)
	}
	if len(strategies) != 1 {
		t.Errorf("Expected 1 strategy, got %d", len(strategies))
	}

	// Update API sync strategy
	newDesc := "Updated description"
	err = svc.UpdateAPISyncStrategy(ctx, strategy.ID, contracts.UpdateAPISyncStrategyRequest{
		Description: &newDesc,
	})
	if err != nil {
		t.Fatalf("UpdateAPISyncStrategy failed: %v", err)
	}

	// Delete API sync strategy
	err = svc.DeleteAPISyncStrategy(ctx, strategy.ID)
	if err != nil {
		t.Fatalf("DeleteAPISyncStrategy failed: %v", err)
	}
}
