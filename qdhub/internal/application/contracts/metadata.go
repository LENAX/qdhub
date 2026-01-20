// Package contracts defines application service interfaces and DTOs.
package contracts

import (
	"context"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// MetadataApplicationService defines application service for metadata management.
// Responsibilities:
//   - Orchestrate complete use cases
//   - Manage transactions
//   - Coordinate domain services and repositories
//   - Call external adapters
type MetadataApplicationService interface {
	// ==================== Data Source Management ====================

	// CreateDataSource creates a new data source.
	CreateDataSource(ctx context.Context, req CreateDataSourceRequest) (*metadata.DataSource, error)

	// GetDataSource retrieves a data source by ID.
	GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error)

	// UpdateDataSource updates a data source.
	UpdateDataSource(ctx context.Context, id shared.ID, req UpdateDataSourceRequest) error

	// DeleteDataSource deletes a data source and its related entities.
	DeleteDataSource(ctx context.Context, id shared.ID) error

	// ListDataSources lists all data sources.
	ListDataSources(ctx context.Context) ([]*metadata.DataSource, error)

	// ==================== API Metadata Management ====================

	// ParseAndImportMetadata parses documentation and imports metadata.
	// This is a complex use case involving:
	//   1. Fetch documentation content
	//   2. Parse using DocumentParser
	//   3. Create/Update APICategory entities
	//   4. Create/Update APIMetadata entities
	//   5. Validate all entities
	ParseAndImportMetadata(ctx context.Context, req ParseMetadataRequest) (*ParseMetadataResult, error)

	// CreateAPIMetadata creates a new API metadata.
	CreateAPIMetadata(ctx context.Context, req CreateAPIMetadataRequest) (*metadata.APIMetadata, error)

	// GetAPIMetadata retrieves an API metadata by ID.
	GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error)

	// UpdateAPIMetadata updates an API metadata.
	UpdateAPIMetadata(ctx context.Context, id shared.ID, req UpdateAPIMetadataRequest) error

	// DeleteAPIMetadata deletes an API metadata.
	DeleteAPIMetadata(ctx context.Context, id shared.ID) error

	// ListAPIMetadataByDataSource lists all API metadata for a data source.
	ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APIMetadata, error)

	// ==================== Token Management ====================

	// SaveToken saves or updates a token for a data source.
	// The token value will be encrypted before storage.
	SaveToken(ctx context.Context, req SaveTokenRequest) error

	// GetToken retrieves a token for a data source.
	// The token value will be decrypted before return.
	GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error)

	// DeleteToken deletes a token.
	DeleteToken(ctx context.Context, dataSourceID shared.ID) error
}

// ==================== Request/Response DTOs ====================

// CreateDataSourceRequest represents a request to create a data source.
type CreateDataSourceRequest struct {
	Name        string
	Description string
	BaseURL     string
	DocURL      string
}

// UpdateDataSourceRequest represents a request to update a data source.
type UpdateDataSourceRequest struct {
	Name        *string
	Description *string
	BaseURL     *string
	DocURL      *string
}

// ParseMetadataRequest represents a request to parse and import metadata.
type ParseMetadataRequest struct {
	DataSourceID shared.ID
	DocContent   string
	DocType      metadata.DocumentType
	MaxAPICrawl  int // 最大爬取 API 数量（0=不限制）
}

// ParseMetadataResult represents the result of parsing metadata.
type ParseMetadataResult struct {
	InstanceID        shared.ID // Workflow instance ID for tracking execution status
	CategoriesCreated int
	APIsCreated       int
	APIsUpdated       int
}

// CreateAPIMetadataRequest represents a request to create API metadata.
type CreateAPIMetadataRequest struct {
	DataSourceID   shared.ID
	CategoryID     *shared.ID
	Name           string
	DisplayName    string
	Description    string
	Endpoint       string
	RequestParams  []metadata.ParamMeta
	ResponseFields []metadata.FieldMeta
	RateLimit      *metadata.RateLimit
	Permission     string
}

// UpdateAPIMetadataRequest represents a request to update API metadata.
type UpdateAPIMetadataRequest struct {
	DisplayName    *string
	Description    *string
	Endpoint       *string
	RequestParams  *[]metadata.ParamMeta
	ResponseFields *[]metadata.FieldMeta
	RateLimit      *metadata.RateLimit
	Permission     *string
}

// SaveTokenRequest represents a request to save a token.
type SaveTokenRequest struct {
	DataSourceID shared.ID
	TokenValue   string
	ExpiresAt    *string // RFC3339 format
}
