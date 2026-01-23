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

	// ListDataSources lists all data sources.
	ListDataSources(ctx context.Context) ([]*metadata.DataSource, error)

	// ==================== API Metadata Management ====================

	// ParseAndImportMetadata parses documentation and imports metadata.
	// This method uses the built-in metadata_crawl workflow to perform the operation.
	ParseAndImportMetadata(ctx context.Context, req ParseMetadataRequest) (*ParseMetadataResult, error)

	// ==================== Token Management ====================

	// SaveToken saves or updates a token for a data source.
	// The token value will be encrypted before storage.
	SaveToken(ctx context.Context, req SaveTokenRequest) error

	// GetToken retrieves a token for a data source.
	// The token value will be decrypted before return.
	GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error)

	// ==================== API Sync Strategy Management ====================

	// CreateAPISyncStrategy creates a new API sync strategy.
	CreateAPISyncStrategy(ctx context.Context, req CreateAPISyncStrategyRequest) (*metadata.APISyncStrategy, error)

	// GetAPISyncStrategy retrieves an API sync strategy by ID or by (DataSourceID, APIName).
	GetAPISyncStrategy(ctx context.Context, req GetAPISyncStrategyRequest) (*metadata.APISyncStrategy, error)

	// UpdateAPISyncStrategy updates an API sync strategy.
	UpdateAPISyncStrategy(ctx context.Context, id shared.ID, req UpdateAPISyncStrategyRequest) error

	// DeleteAPISyncStrategy deletes an API sync strategy.
	DeleteAPISyncStrategy(ctx context.Context, id shared.ID) error

	// ListAPISyncStrategies lists all API sync strategies for a data source.
	ListAPISyncStrategies(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error)
}

// ==================== Request/Response DTOs ====================

// CreateDataSourceRequest represents a request to create a data source.
type CreateDataSourceRequest struct {
	Name        string
	Description string
	BaseURL     string
	DocURL      string
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
	InstanceID        shared.ID `json:"instance_id"` // Workflow instance ID for tracking execution status
	CategoriesCreated int       `json:"categories_created"`
	APIsCreated       int       `json:"apis_created"`
	APIsUpdated       int       `json:"apis_updated"`
}

// SaveTokenRequest represents a request to save a token.
type SaveTokenRequest struct {
	DataSourceID shared.ID
	TokenValue   string
	ExpiresAt    *string // RFC3339 format
}

// CreateAPISyncStrategyRequest represents a request to create an API sync strategy.
type CreateAPISyncStrategyRequest struct {
	DataSourceID     shared.ID
	APIName          string
	PreferredParam   metadata.SyncParamType
	SupportDateRange bool
	RequiredParams   []string
	Dependencies     []string
	Description      string
}

// GetAPISyncStrategyRequest represents a request to get an API sync strategy.
// Either ID or (DataSourceID + APIName) must be provided.
type GetAPISyncStrategyRequest struct {
	ID           *shared.ID
	DataSourceID *shared.ID
	APIName      *string
}

// UpdateAPISyncStrategyRequest represents a request to update an API sync strategy.
type UpdateAPISyncStrategyRequest struct {
	PreferredParam   *metadata.SyncParamType
	SupportDateRange *bool
	RequiredParams   *[]string
	Dependencies     *[]string
	Description      *string
}
