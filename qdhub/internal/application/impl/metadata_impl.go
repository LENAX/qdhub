// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"time"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// MetadataApplicationServiceImpl implements MetadataApplicationService.
type MetadataApplicationServiceImpl struct {
	dataSourceRepo metadata.DataSourceRepository

	metadataValidator metadata.MetadataValidator
	parserFactory     metadata.DocumentParserFactory
}

// NewMetadataApplicationService creates a new MetadataApplicationService implementation.
func NewMetadataApplicationService(
	dataSourceRepo metadata.DataSourceRepository,
	parserFactory metadata.DocumentParserFactory,
) contracts.MetadataApplicationService {
	return &MetadataApplicationServiceImpl{
		dataSourceRepo:    dataSourceRepo,
		metadataValidator: metadata.NewMetadataValidator(),
		parserFactory:     parserFactory,
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

// UpdateDataSource updates a data source.
func (s *MetadataApplicationServiceImpl) UpdateDataSource(ctx context.Context, id shared.ID, req contracts.UpdateDataSourceRequest) error {
	ds, err := s.dataSourceRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Apply updates
	name := ds.Name
	description := ds.Description
	baseURL := ds.BaseURL
	docURL := ds.DocURL

	if req.Name != nil {
		name = *req.Name
	}
	if req.Description != nil {
		description = *req.Description
	}
	if req.BaseURL != nil {
		baseURL = *req.BaseURL
	}
	if req.DocURL != nil {
		docURL = *req.DocURL
	}

	ds.UpdateInfo(name, description, baseURL, docURL)

	// Validate
	if err := s.metadataValidator.ValidateDataSource(ds); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.dataSourceRepo.Update(ds); err != nil {
		return fmt.Errorf("failed to update data source: %w", err)
	}

	return nil
}

// DeleteDataSource deletes a data source and its related entities.
func (s *MetadataApplicationServiceImpl) DeleteDataSource(ctx context.Context, id shared.ID) error {
	ds, err := s.dataSourceRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Delete data source
	if err := s.dataSourceRepo.Delete(id); err != nil {
		return fmt.Errorf("failed to delete data source: %w", err)
	}

	return nil
}

// ListDataSources lists all data sources.
func (s *MetadataApplicationServiceImpl) ListDataSources(ctx context.Context) ([]*metadata.DataSource, error) {
	sources, err := s.dataSourceRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list data sources: %w", err)
	}
	return sources, nil
}

// ==================== API Metadata Management ====================

// ParseAndImportMetadata parses documentation and imports metadata.
func (s *MetadataApplicationServiceImpl) ParseAndImportMetadata(ctx context.Context, req contracts.ParseMetadataRequest) (*contracts.ParseMetadataResult, error) {
	// Verify data source exists
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Get parser for document type
	parser, err := s.parserFactory.GetParser(req.DocType)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	// Parse catalog
	categories, apiURLs, err := parser.ParseCatalog(req.DocContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse catalog: %w", err)
	}

	result := &contracts.ParseMetadataResult{}

	// Save categories
	for i := range categories {
		categories[i].DataSourceID = req.DataSourceID
		if err := s.dataSourceRepo.AddCategory(&categories[i]); err != nil {
			return nil, fmt.Errorf("failed to create category: %w", err)
		}
		result.CategoriesCreated++
	}

	// Parse and save API metadata (simplified - in real implementation would fetch each URL)
	_ = apiURLs // URLs would be fetched and parsed in full implementation
	// For now, APIs would be created via CreateAPIMetadata

	return result, nil
}

// CreateAPIMetadata creates a new API metadata.
func (s *MetadataApplicationServiceImpl) CreateAPIMetadata(ctx context.Context, req contracts.CreateAPIMetadataRequest) (*metadata.APIMetadata, error) {
	// Verify data source exists
	ds, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Create domain entity
	api := metadata.NewAPIMetadata(
		req.DataSourceID,
		req.Name,
		req.DisplayName,
		req.Description,
		req.Endpoint,
	)

	// Set optional fields
	api.CategoryID = req.CategoryID
	api.Permission = req.Permission
	api.SetRequestParams(req.RequestParams)
	api.SetResponseFields(req.ResponseFields)
	api.SetRateLimit(req.RateLimit)

	// Validate
	if err := s.metadataValidator.ValidateAPIMetadata(api); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.dataSourceRepo.AddAPIMetadata(api); err != nil {
		return nil, fmt.Errorf("failed to create API metadata: %w", err)
	}

	return api, nil
}

// GetAPIMetadata retrieves an API metadata by ID.
func (s *MetadataApplicationServiceImpl) GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error) {
	api, err := s.dataSourceRepo.GetAPIMetadata(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get API metadata: %w", err)
	}
	if api == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "API metadata not found", nil)
	}
	return api, nil
}

// UpdateAPIMetadata updates an API metadata.
func (s *MetadataApplicationServiceImpl) UpdateAPIMetadata(ctx context.Context, id shared.ID, req contracts.UpdateAPIMetadataRequest) error {
	api, err := s.dataSourceRepo.GetAPIMetadata(id)
	if err != nil {
		return fmt.Errorf("failed to get API metadata: %w", err)
	}
	if api == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "API metadata not found", nil)
	}

	// Apply updates
	if req.DisplayName != nil {
		api.DisplayName = *req.DisplayName
	}
	if req.Description != nil {
		api.Description = *req.Description
	}
	if req.Endpoint != nil {
		api.Endpoint = *req.Endpoint
	}
	if req.Permission != nil {
		api.Permission = *req.Permission
	}
	if req.RequestParams != nil {
		api.SetRequestParams(*req.RequestParams)
	}
	if req.ResponseFields != nil {
		api.SetResponseFields(*req.ResponseFields)
	}
	if req.RateLimit != nil {
		api.SetRateLimit(req.RateLimit)
	}

	api.UpdatedAt = shared.Now()

	// Validate
	if err := s.metadataValidator.ValidateAPIMetadata(api); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.dataSourceRepo.UpdateAPIMetadata(api); err != nil {
		return fmt.Errorf("failed to update API metadata: %w", err)
	}

	return nil
}

// DeleteAPIMetadata deletes an API metadata.
func (s *MetadataApplicationServiceImpl) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	api, err := s.dataSourceRepo.GetAPIMetadata(id)
	if err != nil {
		return fmt.Errorf("failed to get API metadata: %w", err)
	}
	if api == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "API metadata not found", nil)
	}

	if err := s.dataSourceRepo.DeleteAPIMetadata(id); err != nil {
		return fmt.Errorf("failed to delete API metadata: %w", err)
	}

	return nil
}

// ListAPIMetadataByDataSource lists all API metadata for a data source.
func (s *MetadataApplicationServiceImpl) ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	apis, err := s.dataSourceRepo.ListAPIMetadataByDataSource(dataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to list API metadata: %w", err)
	}
	return apis, nil
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

// DeleteToken deletes a token.
func (s *MetadataApplicationServiceImpl) DeleteToken(ctx context.Context, dataSourceID shared.ID) error {
	token, err := s.dataSourceRepo.GetTokenByDataSource(dataSourceID)
	if err != nil {
		return fmt.Errorf("failed to get token: %w", err)
	}
	if token == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "token not found", nil)
	}

	if err := s.dataSourceRepo.DeleteToken(token.ID); err != nil {
		return fmt.Errorf("failed to delete token: %w", err)
	}

	return nil
}
