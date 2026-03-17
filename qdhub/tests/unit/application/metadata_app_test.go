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

func (m *MockMetaWorkflowExecutor) ExecuteFromExecutionGraph(ctx context.Context, req workflow.ExecutionGraphSyncRequest) (shared.ID, error) {
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

func (m *MockDocumentParser) ParseCatalog(content string) ([]metadata.APICategory, []string, []*shared.ID, error) {
	if m.parseErr != nil {
		return nil, nil, nil, m.parseErr
	}
	return m.categories, m.apiURLs, nil, nil
}

func (m *MockDocumentParser) ParseAPIDetail(content string) (*metadata.APIMetadata, error) {
	return nil, nil
}

func (m *MockDocumentParser) SupportedType() metadata.DocumentType {
	return metadata.DocumentTypeHTML
}

// MockMetadataRepository is a mock implementation of metadata.Repository.
type MockMetadataRepository struct {
	strategies map[shared.ID]*metadata.APISyncStrategy
}

func NewMockMetadataRepository() *MockMetadataRepository {
	return &MockMetadataRepository{
		strategies: make(map[shared.ID]*metadata.APISyncStrategy),
	}
}

func (m *MockMetadataRepository) SaveAPISyncStrategy(ctx context.Context, strategy *metadata.APISyncStrategy) error {
	m.strategies[strategy.ID] = strategy
	return nil
}

func (m *MockMetadataRepository) SaveAPISyncStrategyBatch(ctx context.Context, strategies []*metadata.APISyncStrategy) error {
	for _, s := range strategies {
		m.strategies[s.ID] = s
	}
	return nil
}

func (m *MockMetadataRepository) GetAPISyncStrategyByID(ctx context.Context, id shared.ID) (*metadata.APISyncStrategy, error) {
	s, exists := m.strategies[id]
	if !exists {
		return nil, nil
	}
	return s, nil
}

func (m *MockMetadataRepository) GetAPISyncStrategyByAPIName(ctx context.Context, dataSourceID shared.ID, apiName string) (*metadata.APISyncStrategy, error) {
	for _, s := range m.strategies {
		if s.DataSourceID == dataSourceID && s.APIName == apiName {
			return s, nil
		}
	}
	return nil, nil
}

func (m *MockMetadataRepository) ListAPISyncStrategiesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error) {
	result := make([]*metadata.APISyncStrategy, 0)
	for _, s := range m.strategies {
		if s.DataSourceID == dataSourceID {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *MockMetadataRepository) ListAPISyncStrategiesByAPINames(ctx context.Context, dataSourceID shared.ID, apiNames []string) ([]*metadata.APISyncStrategy, error) {
	result := make([]*metadata.APISyncStrategy, 0)
	apiNameSet := make(map[string]bool)
	for _, name := range apiNames {
		apiNameSet[name] = true
	}
	for _, s := range m.strategies {
		if s.DataSourceID == dataSourceID && apiNameSet[s.APIName] {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *MockMetadataRepository) DeleteAPISyncStrategy(ctx context.Context, id shared.ID) error {
	delete(m.strategies, id)
	return nil
}

func (m *MockMetadataRepository) DeleteAPISyncStrategiesByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	for id, s := range m.strategies {
		if s.DataSourceID == dataSourceID {
			delete(m.strategies, id)
		}
	}
	return nil
}

// Stub methods for other Repository interface methods (not used in tests)
func (m *MockMetadataRepository) SaveCategories(ctx context.Context, categories []metadata.APICategory) error {
	return nil
}
func (m *MockMetadataRepository) DeleteCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return nil
}
func (m *MockMetadataRepository) SaveAPIMetadata(ctx context.Context, meta *metadata.APIMetadata) error {
	return nil
}
func (m *MockMetadataRepository) SaveAPIMetadataBatch(ctx context.Context, metas []metadata.APIMetadata) error {
	return nil
}
func (m *MockMetadataRepository) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *MockMetadataRepository) DeleteAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) error {
	return nil
}
func (m *MockMetadataRepository) GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error) {
	return nil, nil
}
func (m *MockMetadataRepository) GetDataSourceByName(ctx context.Context, name string) (*metadata.DataSource, error) {
	return nil, nil
}
func (m *MockMetadataRepository) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	return nil, nil
}
func (m *MockMetadataRepository) GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error) {
	return nil, nil
}
func (m *MockMetadataRepository) ListCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APICategory, error) {
	return nil, nil
}
func (m *MockMetadataRepository) ListCategoriesByDataSourceWithAPIs(ctx context.Context, dataSourceID shared.ID) ([]metadata.APICategory, error) {
	return nil, nil
}
func (m *MockMetadataRepository) ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]metadata.APIMetadata, error) {
	return nil, nil
}
func (m *MockMetadataRepository) ListAPIMetadataByDataSourcePaginated(ctx context.Context, dataSourceID shared.ID, idFilter *shared.ID, nameFilter string, categoryIDFilter *shared.ID, page, pageSize int) ([]metadata.APIMetadata, int64, error) {
	return nil, 0, nil
}

// ==================== Test Cases ====================

func TestMetadataApplicationService_CreateDataSource(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		dsRepo := NewMockMetaDataSourceRepository()
		parserFactory := NewMockDocumentParserFactory()

		metadataRepo := NewMockMetadataRepository()
		svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, NewMockMetaWorkflowExecutor(), nil)

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

		metadataRepo := NewMockMetadataRepository()
		svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, NewMockMetaWorkflowExecutor(), nil)

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

		metadataRepo := NewMockMetadataRepository()
		svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, NewMockMetaWorkflowExecutor(), nil)

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

		metadataRepo := NewMockMetadataRepository()
		svc := impl.NewMetadataApplicationService(dsRepo, metadataRepo, parserFactory, NewMockMetaWorkflowExecutor(), nil)

		_, err := svc.GetDataSource(ctx, shared.NewID())
		if err == nil {
			t.Fatal("Expected error for non-existent data source")
		}
	})
}
