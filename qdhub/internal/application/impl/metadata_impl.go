// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// MetadataApplicationServiceImpl implements MetadataApplicationService.
type MetadataApplicationServiceImpl struct {
	dataSourceRepo metadata.DataSourceRepository
	metadataRepo   metadata.Repository // 用于APISyncStrategy操作

	metadataValidator   metadata.MetadataValidator
	parserFactory       metadata.DocumentParserFactory
	workflowExecutor    workflow.WorkflowExecutor   // 用于执行元数据爬取工作流（领域服务接口）
	tokenValidator      contracts.DataSourceTokenValidator
}

// NewMetadataApplicationService creates a new MetadataApplicationService implementation.
func NewMetadataApplicationService(
	dataSourceRepo metadata.DataSourceRepository,
	metadataRepo metadata.Repository,
	parserFactory metadata.DocumentParserFactory,
	workflowExecutor workflow.WorkflowExecutor,
	tokenValidator contracts.DataSourceTokenValidator,
) contracts.MetadataApplicationService {
	return &MetadataApplicationServiceImpl{
		dataSourceRepo:    dataSourceRepo,
		metadataRepo:      metadataRepo,
		metadataValidator: metadata.NewMetadataValidator(),
		parserFactory:     parserFactory,
		workflowExecutor:  workflowExecutor,
		tokenValidator:    tokenValidator,
	}
}

// ==================== Data Source Management ====================

// CreateDataSource creates a new data source.
func (s *MetadataApplicationServiceImpl) CreateDataSource(ctx context.Context, req contracts.CreateDataSourceRequest) (*metadata.DataSource, error) {
	// Create domain entity
	ds := metadata.NewDataSource(
		req.Name,
		req.Description,
		req.BaseURL,
		req.DocURL,
	)

	// Validate
	if err := s.metadataValidator.ValidateDataSource(ds); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.dataSourceRepo.Create(ds); err != nil {
		return nil, fmt.Errorf("failed to create data source: %w", err)
	}

	return ds, nil
}

// GetDataSource retrieves a data source by ID.
func (s *MetadataApplicationServiceImpl) GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error) {
	ds, err := s.dataSourceRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	return ds, nil
}


// ListDataSources lists all data sources.
func (s *MetadataApplicationServiceImpl) ListDataSources(ctx context.Context) ([]*metadata.DataSource, error) {
	sources, err := s.dataSourceRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list data sources: %w", err)
	}
	return sources, nil
}

// DeleteDataSource deletes a data source and cascades: api_sync_strategies (via metadataRepo), then token, api_metadata, api_categories, data_source (via dataSourceRepo).
func (s *MetadataApplicationServiceImpl) DeleteDataSource(ctx context.Context, id shared.ID) error {
	ds, err := s.dataSourceRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	if err := s.metadataRepo.DeleteAPISyncStrategiesByDataSource(ctx, id); err != nil {
		return fmt.Errorf("failed to delete API sync strategies: %w", err)
	}
	if err := s.dataSourceRepo.Delete(id); err != nil {
		return fmt.Errorf("failed to delete data source: %w", err)
	}
	return nil
}

// UpdateDataSourceCommonDataAPIs updates the common data APIs list for a data source.
func (s *MetadataApplicationServiceImpl) UpdateDataSourceCommonDataAPIs(ctx context.Context, id shared.ID, req contracts.UpdateDataSourceCommonDataAPIsRequest) error {
	ds, err := s.dataSourceRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	ds.SetCommonDataAPIs(req.CommonDataAPIs)
	if err := s.dataSourceRepo.Update(ds); err != nil {
		return fmt.Errorf("failed to update data source common_data_apis: %w", err)
	}
	return nil
}

// ==================== API Metadata Management ====================

// ListAPIMetadata returns a paginated list of API metadata for a data source.
func (s *MetadataApplicationServiceImpl) ListAPIMetadata(ctx context.Context, dataSourceID shared.ID, req contracts.ListAPIMetadataRequest) (*contracts.ListAPIMetadataResponse, error) {
	ds, err := s.dataSourceRepo.Get(dataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	page := req.Page
	if page < 1 {
		page = 1
	}
	pageSize := req.PageSize
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	items, total, err := s.metadataRepo.ListAPIMetadataByDataSourcePaginated(ctx, dataSourceID, req.ID, req.Name, req.CategoryID, page, pageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to list api metadata: %w", err)
	}
	ptrItems := make([]*metadata.APIMetadata, len(items))
	for i := range items {
		ptrItems[i] = &items[i]
	}
	return &contracts.ListAPIMetadataResponse{Items: ptrItems, Total: total}, nil
}

// ListAPINames returns all API names for a data source (for common-data-apis form).
func (s *MetadataApplicationServiceImpl) ListAPINames(ctx context.Context, dataSourceID shared.ID) ([]string, error) {
	ds, err := s.dataSourceRepo.Get(dataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	items, err := s.metadataRepo.ListAPIMetadataByDataSource(ctx, dataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list api metadata: %w", err)
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names, nil
}

// ListAPICategories returns API categories for a data source. When hasAPIsOnly is true, only categories with at least one api_metadata are returned.
func (s *MetadataApplicationServiceImpl) ListAPICategories(ctx context.Context, dataSourceID shared.ID, hasAPIsOnly bool) ([]metadata.APICategory, error) {
	ds, err := s.dataSourceRepo.Get(dataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	if hasAPIsOnly {
		return s.metadataRepo.ListCategoriesByDataSourceWithAPIs(ctx, dataSourceID)
	}
	return s.metadataRepo.ListCategoriesByDataSource(ctx, dataSourceID)
}

// DeleteAPIMetadata deletes a single API metadata by ID.
func (s *MetadataApplicationServiceImpl) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	if err := s.metadataRepo.DeleteAPIMetadata(ctx, id); err != nil {
		return fmt.Errorf("failed to delete API metadata: %w", err)
	}
	return nil
}

// ParseAndImportMetadata parses documentation and imports metadata.
// This method uses the built-in metadata_crawl workflow to perform the operation.
//
// Pre-conditions validated:
//   - Data source must exist (validated using req.DataSourceID)
//
// The same DataSourceID is used for both validation and workflow execution
// to ensure consistency.
func (s *MetadataApplicationServiceImpl) ParseAndImportMetadata(ctx context.Context, req contracts.ParseMetadataRequest) (*contracts.ParseMetadataResult, error) {
	// 1. 验证数据源是否存在（前置条件校验）
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// 2. 验证 workflow executor 是否可用
	if s.workflowExecutor == nil {
		return nil, fmt.Errorf("workflow executor is not available")
	}

	// 3. 执行内建的 metadata_crawl workflow
	// 使用类型安全的 ExecuteMetadataCrawl 方法
	// 注意：req.DataSourceID 既用于上面的校验，也用于 workflow 执行，确保一致性
	instanceID, err := s.workflowExecutor.ExecuteMetadataCrawl(ctx, workflow.MetadataCrawlRequest{
		DataSourceID:   req.DataSourceID, // 与校验时使用的 ID 一致
		DataSourceName: ds.Name,          // 从校验通过的数据源获取名称
		MaxAPICrawl:    req.MaxAPICrawl,  // 使用请求中的爬取数量限制
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute metadata crawl workflow: %w", err)
	}

	// 记录 workflow instance ID，方便用户查询执行状态
	logrus.Infof("Metadata crawl workflow started for data source %s, instance ID: %s", req.DataSourceID, instanceID)

	// 4. 返回结果
	// 由于 workflow 是异步执行的，这些字段暂时无法立即获取
	// 用户可以通过查询 workflow instance 状态来获取执行结果
	result := &contracts.ParseMetadataResult{
		InstanceID:        instanceID, // 返回 workflow instance ID，方便用户跟踪执行状态
		CategoriesCreated: 0,
		APIsCreated:       0,
		APIsUpdated:       0,
	}

	return result, nil
}


// ==================== Token Management ====================

// SaveToken saves or updates a token for a data source.
func (s *MetadataApplicationServiceImpl) SaveToken(ctx context.Context, req contracts.SaveTokenRequest) error {
	// Verify data source exists
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Parse expiration time if provided
	var expiresAt *time.Time
	if req.ExpiresAt != nil {
		t, err := time.Parse(time.RFC3339, *req.ExpiresAt)
		if err != nil {
			return fmt.Errorf("invalid expiration time format: %w", err)
		}
		expiresAt = &t
	}

	// Check if token exists
	existingToken, err := s.dataSourceRepo.GetTokenByDataSource(req.DataSourceID)
	if err != nil && !shared.IsNotFoundError(err) {
		return fmt.Errorf("failed to check existing token: %w", err)
	}

	if existingToken != nil {
		// Update existing token
		existingToken.TokenValue = req.TokenValue
		existingToken.ExpiresAt = expiresAt
		if err := s.dataSourceRepo.SetToken(existingToken); err != nil {
			return fmt.Errorf("failed to update token: %w", err)
		}
	} else {
		// Create new token
		token := metadata.NewToken(req.DataSourceID, req.TokenValue, expiresAt)
		if err := s.metadataValidator.ValidateToken(token); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
		if err := s.dataSourceRepo.SetToken(token); err != nil {
			return fmt.Errorf("failed to create token: %w", err)
		}
	}

	return nil
}

// GetToken retrieves a token for a data source.
func (s *MetadataApplicationServiceImpl) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	token, err := s.dataSourceRepo.GetTokenByDataSource(dataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}
	if token == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "token not found", nil)
	}
	return token, nil
}

// ValidateDataSourceToken checks if the data source has a token and validates it via adapter test request.
func (s *MetadataApplicationServiceImpl) ValidateDataSourceToken(ctx context.Context, dataSourceID shared.ID) (hasToken bool, valid bool, message string, err error) {
	ds, err := s.dataSourceRepo.Get(dataSourceID)
	if err != nil {
		return false, false, "", fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return false, false, "", shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	token, err := s.dataSourceRepo.GetTokenByDataSource(dataSourceID)
	if err != nil {
		return false, false, "", fmt.Errorf("failed to get token: %w", err)
	}
	if token == nil {
		return false, false, "未认证", nil
	}
	if s.tokenValidator == nil {
		return true, false, "校验器未配置", nil
	}
	valid, message, err = s.tokenValidator.Validate(ctx, ds.Name, ds.BaseURL, token.TokenValue)
	if err != nil {
		return true, false, err.Error(), nil
	}
	if !valid {
		if message == "" {
			message = "token无效"
		}
		return true, false, message, nil
	}
	return true, true, "", nil
}

// GetDataSourceConfig returns api_url and token for the config form.
func (s *MetadataApplicationServiceImpl) GetDataSourceConfig(ctx context.Context, dataSourceID shared.ID) (apiURL string, token string, err error) {
	ds, err := s.dataSourceRepo.Get(dataSourceID)
	if err != nil {
		return "", "", fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return "", "", shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}
	tok, err := s.dataSourceRepo.GetTokenByDataSource(dataSourceID)
	if err != nil {
		return "", "", fmt.Errorf("failed to get token: %w", err)
	}
	if tok != nil {
		return ds.BaseURL, tok.TokenValue, nil
	}
	return ds.BaseURL, "", nil
}

// ==================== API Sync Strategy Management ====================

// CreateAPISyncStrategy creates a new API sync strategy.
func (s *MetadataApplicationServiceImpl) CreateAPISyncStrategy(ctx context.Context, req contracts.CreateAPISyncStrategyRequest) (*metadata.APISyncStrategy, error) {
	// Verify data source exists
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Verify API exists by listing APIs for the data source
	apis, err := s.metadataRepo.ListAPIMetadataByDataSource(ctx, req.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list API metadata: %w", err)
	}
	apiFound := false
	for _, api := range apis {
		if api.Name == req.APIName {
			apiFound = true
			break
		}
	}
	if !apiFound {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, fmt.Sprintf("API metadata not found: %s", req.APIName), nil)
	}

	// Check if strategy already exists
	existing, err := s.metadataRepo.GetAPISyncStrategyByAPIName(ctx, req.DataSourceID, req.APIName)
	if err != nil && !shared.IsNotFoundError(err) {
		return nil, fmt.Errorf("failed to check existing strategy: %w", err)
	}
	if existing != nil {
		return nil, shared.NewDomainError(shared.ErrCodeInvalidState, fmt.Sprintf("API sync strategy already exists for API: %s", req.APIName), nil)
	}

	// Create domain entity
	strategy := metadata.NewAPISyncStrategy(req.DataSourceID, req.APIName, req.PreferredParam)
	strategy.SetSupportDateRange(req.SupportDateRange)
	if len(req.RequiredParams) > 0 {
		strategy.SetRequiredParams(req.RequiredParams)
	}
	if len(req.Dependencies) > 0 {
		strategy.SetDependencies(req.Dependencies)
	}
	if req.FixedParams != nil {
		strategy.FixedParams = req.FixedParams
	}
	if len(req.FixedParamKeys) > 0 {
		strategy.FixedParamKeys = req.FixedParamKeys
	}
	if req.Description != "" {
		strategy.SetDescription(req.Description)
	}

	// Persist
	if err := s.metadataRepo.SaveAPISyncStrategy(ctx, strategy); err != nil {
		return nil, fmt.Errorf("failed to create API sync strategy: %w", err)
	}

	return strategy, nil
}

// GetAPISyncStrategy retrieves an API sync strategy by ID or by (DataSourceID, APIName).
func (s *MetadataApplicationServiceImpl) GetAPISyncStrategy(ctx context.Context, req contracts.GetAPISyncStrategyRequest) (*metadata.APISyncStrategy, error) {
	if req.ID != nil {
		strategy, err := s.metadataRepo.GetAPISyncStrategyByID(ctx, *req.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get API sync strategy: %w", err)
		}
		if strategy == nil {
			return nil, shared.NewDomainError(shared.ErrCodeNotFound, "API sync strategy not found", nil)
		}
		return strategy, nil
	}

	if req.DataSourceID == nil || req.APIName == nil {
		return nil, fmt.Errorf("either ID or (DataSourceID + APIName) must be provided")
	}

	strategy, err := s.metadataRepo.GetAPISyncStrategyByAPIName(ctx, *req.DataSourceID, *req.APIName)
	if err != nil {
		return nil, fmt.Errorf("failed to get API sync strategy: %w", err)
	}
	if strategy == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "API sync strategy not found", nil)
	}

	return strategy, nil
}

// UpdateAPISyncStrategy updates an API sync strategy.
func (s *MetadataApplicationServiceImpl) UpdateAPISyncStrategy(ctx context.Context, id shared.ID, req contracts.UpdateAPISyncStrategyRequest) error {
	// Get existing strategy
	strategy, err := s.metadataRepo.GetAPISyncStrategyByID(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get API sync strategy: %w", err)
	}
	if strategy == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "API sync strategy not found", nil)
	}

	// Apply updates
	if req.PreferredParam != nil {
		strategy.PreferredParam = *req.PreferredParam
	}
	if req.SupportDateRange != nil {
		strategy.SetSupportDateRange(*req.SupportDateRange)
	}
	if req.RequiredParams != nil {
		strategy.SetRequiredParams(*req.RequiredParams)
	}
	if req.Dependencies != nil {
		strategy.SetDependencies(*req.Dependencies)
	}
	if req.FixedParams != nil {
		strategy.FixedParams = *req.FixedParams
	}
	if req.FixedParamKeys != nil {
		strategy.FixedParamKeys = *req.FixedParamKeys
	}
	if req.Description != nil {
		strategy.SetDescription(*req.Description)
	}

	// Persist
	if err := s.metadataRepo.SaveAPISyncStrategy(ctx, strategy); err != nil {
		return fmt.Errorf("failed to update API sync strategy: %w", err)
	}

	return nil
}

// DeleteAPISyncStrategy deletes an API sync strategy.
func (s *MetadataApplicationServiceImpl) DeleteAPISyncStrategy(ctx context.Context, id shared.ID) error {
	if err := s.metadataRepo.DeleteAPISyncStrategy(ctx, id); err != nil {
		return fmt.Errorf("failed to delete API sync strategy: %w", err)
	}
	return nil
}

// ListAPISyncStrategies lists all API sync strategies for a data source.
func (s *MetadataApplicationServiceImpl) ListAPISyncStrategies(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error) {
	strategies, err := s.metadataRepo.ListAPISyncStrategiesByDataSource(ctx, dataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list API sync strategies: %w", err)
	}
	return strategies, nil
}
