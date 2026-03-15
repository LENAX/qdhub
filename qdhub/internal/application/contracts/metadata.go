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

	// DeleteDataSource deletes a data source and cascades to api_metadata, api_sync_strategies, api_categories, token. Admin only.
	DeleteDataSource(ctx context.Context, id shared.ID) error

	// UpdateDataSourceCommonDataAPIs updates the list of API names treated as common data for a data source (e.g. trade_cal, stock_basic for tushare).
	UpdateDataSourceCommonDataAPIs(ctx context.Context, id shared.ID, req UpdateDataSourceCommonDataAPIsRequest) error

	// ==================== API Metadata Management ====================

	// ListAPIMetadata returns a paginated list of API metadata for a data source, with optional filter by id, name and category.
	ListAPIMetadata(ctx context.Context, dataSourceID shared.ID, req ListAPIMetadataRequest) (*ListAPIMetadataResponse, error)

	// ListAPINames returns all API names for a data source (e.g. for common-data-apis checkbox form).
	ListAPINames(ctx context.Context, dataSourceID shared.ID) ([]string, error)

	// ListAPICategories returns all API categories for a data source (for filter dropdown).
	// When hasAPIsOnly is true, returns only categories that have at least one api_metadata.
	ListAPICategories(ctx context.Context, dataSourceID shared.ID, hasAPIsOnly bool) ([]metadata.APICategory, error)

	// DeleteAPIMetadata deletes a single API metadata by ID. Admin only.
	DeleteAPIMetadata(ctx context.Context, id shared.ID) error

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

	// ValidateDataSourceToken checks if the data source has a token and if it is valid.
	// If no token: hasToken=false, message="未认证". If has token: uses the data source adapter to send a test request;
	// success -> valid=true, failure -> valid=false and message set to the concrete error.
	ValidateDataSourceToken(ctx context.Context, dataSourceID shared.ID) (hasToken bool, valid bool, message string, err error)

	// GetDataSourceConfig returns api_url and token for the config form (e.g. when opening configure modal). Token is only set if present.
	GetDataSourceConfig(ctx context.Context, dataSourceID shared.ID) (apiURL string, token string, err error)

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

// DataSourceTokenValidator validates a data source token by making a test request with the data source adapter.
// Implemented in infrastructure (e.g. datasource adapter layer).
type DataSourceTokenValidator interface {
	Validate(ctx context.Context, dataSourceName, baseURL, token string) (valid bool, message string, err error)
}

// ==================== Request/Response DTOs ====================

// CreateDataSourceRequest represents a request to create a data source.
type CreateDataSourceRequest struct {
	Name        string
	Description string
	BaseURL     string
	DocURL      string
}

// UpdateDataSourceCommonDataAPIsRequest represents a request to update common data APIs for a data source.
type UpdateDataSourceCommonDataAPIsRequest struct {
	CommonDataAPIs []string // e.g. ["trade_cal", "stock_basic"] for tushare
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
	FixedParams      map[string]interface{}
	FixedParamKeys   []string
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
	FixedParams      *map[string]interface{}
	FixedParamKeys   *[]string
	Description      *string
}

// ListAPIMetadataRequest represents query params for listing API metadata (paginated, filter by id/name/category).
type ListAPIMetadataRequest struct {
	Page       int        // 1-based
	PageSize   int        // default 20, max 100
	ID         *shared.ID // optional exact match
	Name       string     // optional, name contains (LIKE %name%)
	CategoryID *shared.ID // optional, filter by API category
}

// ListAPIMetadataResponse is the paginated response for ListAPIMetadata.
type ListAPIMetadataResponse struct {
	Items []*metadata.APIMetadata
	Total int64
}
