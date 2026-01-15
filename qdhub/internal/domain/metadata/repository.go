// Package metadata contains the metadata domain repository interfaces.
package metadata

import (
	"context"

	"qdhub/internal/domain/shared"
)

// DataSourceRepository defines the repository interface for DataSource aggregate.
// Following DDD principles, this repository handles both the aggregate root (DataSource)
// and its child entities (APICategory, APIMetadata, Token) to maintain aggregate boundaries.
//
// Embeds shared.Repository[DataSource] to inherit common CRUD operations.
type DataSourceRepository interface {
	// Embed base repository for common CRUD operations
	shared.Repository[DataSource]

	// ==================== Child Entity Operations (APICategory) ====================

	// AddCategory adds a new APICategory to a DataSource.
	AddCategory(cat *APICategory) error

	// GetCategory retrieves an APICategory by ID.
	GetCategory(id shared.ID) (*APICategory, error)

	// ListCategoriesByDataSource retrieves all APICategories for a DataSource.
	ListCategoriesByDataSource(dataSourceID shared.ID) ([]*APICategory, error)

	// UpdateCategory updates an APICategory.
	UpdateCategory(cat *APICategory) error

	// DeleteCategory deletes an APICategory by ID.
	DeleteCategory(id shared.ID) error

	// ==================== Child Entity Operations (APIMetadata) ====================

	// AddAPIMetadata adds a new APIMetadata to a DataSource.
	AddAPIMetadata(meta *APIMetadata) error

	// GetAPIMetadata retrieves an APIMetadata by ID.
	GetAPIMetadata(id shared.ID) (*APIMetadata, error)

	// ListAPIMetadataByDataSource retrieves all APIMetadata for a DataSource.
	ListAPIMetadataByDataSource(dataSourceID shared.ID) ([]*APIMetadata, error)

	// ListAPIMetadataByCategory retrieves all APIMetadata for a category.
	ListAPIMetadataByCategory(categoryID shared.ID) ([]*APIMetadata, error)

	// UpdateAPIMetadata updates an APIMetadata.
	UpdateAPIMetadata(meta *APIMetadata) error

	// DeleteAPIMetadata deletes an APIMetadata by ID.
	DeleteAPIMetadata(id shared.ID) error

	// ==================== Child Entity Operations (Token) ====================

	// SetToken sets the token for a DataSource (creates or updates).
	SetToken(token *Token) error

	// GetToken retrieves a Token by ID.
	GetToken(id shared.ID) (*Token, error)

	// GetTokenByDataSource retrieves the Token for a DataSource.
	GetTokenByDataSource(dataSourceID shared.ID) (*Token, error)

	// DeleteToken deletes a Token by ID.
	DeleteToken(id shared.ID) error
}

// Repository defines the aggregated repository interface for metadata domain.
// This interface is used by job functions and services that need to access
// multiple entity types within the metadata domain.
type Repository interface {
	// ==================== Category 操作 ====================

	// SaveCategories batch saves API categories.
	SaveCategories(ctx context.Context, categories []APICategory) error

	// DeleteCategoriesByDataSource deletes all categories for a data source.
	// Used for SAGA compensation to rollback SaveCategories.
	DeleteCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) error

	// ==================== API Metadata 操作 ====================

	// SaveAPIMetadata saves a single API metadata.
	SaveAPIMetadata(ctx context.Context, meta *APIMetadata) error

	// SaveAPIMetadataBatch batch saves API metadata.
	SaveAPIMetadataBatch(ctx context.Context, metas []APIMetadata) error

	// DeleteAPIMetadata deletes a single API metadata by ID.
	// Used for SAGA compensation to rollback SaveAPIMetadata.
	DeleteAPIMetadata(ctx context.Context, id shared.ID) error

	// DeleteAPIMetadataByDataSource deletes all API metadata for a data source.
	// Used for SAGA compensation to rollback batch operations.
	DeleteAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) error

	// ==================== 查询操作 ====================

	// GetDataSource returns a data source by ID.
	GetDataSource(ctx context.Context, id shared.ID) (*DataSource, error)

	// GetToken returns a token by data source ID.
	GetToken(ctx context.Context, dataSourceID shared.ID) (*Token, error)

	// GetAPIMetadata returns API metadata by ID.
	GetAPIMetadata(ctx context.Context, id shared.ID) (*APIMetadata, error)

	// ListCategoriesByDataSource returns all categories for a data source.
	ListCategoriesByDataSource(ctx context.Context, dataSourceID shared.ID) ([]APICategory, error)

	// ListAPIMetadataByDataSource returns all API metadata for a data source.
	ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]APIMetadata, error)
}
