// Package metadata contains the metadata domain repository interfaces.
package metadata

import (
	"context"

	"qdhub/internal/domain/shared"
)

// DataSourceRepository defines the repository interface for DataSource aggregate.
type DataSourceRepository interface {
	Create(ds *DataSource) error
	Get(id shared.ID) (*DataSource, error)
	Update(ds *DataSource) error
	Delete(id shared.ID) error
	List() ([]*DataSource, error)
}

// APICategoryRepository defines the repository interface for APICategory.
type APICategoryRepository interface {
	Create(cat *APICategory) error
	Get(id shared.ID) (*APICategory, error)
	Update(cat *APICategory) error
	Delete(id shared.ID) error
	ListByDataSource(dataSourceID shared.ID) ([]*APICategory, error)
}

// APIMetadataRepository defines the repository interface for APIMetadata.
type APIMetadataRepository interface {
	Create(meta *APIMetadata) error
	Get(id shared.ID) (*APIMetadata, error)
	Update(meta *APIMetadata) error
	Delete(id shared.ID) error
	ListByDataSource(dataSourceID shared.ID) ([]*APIMetadata, error)
	ListByCategory(categoryID shared.ID) ([]*APIMetadata, error)
}

// TokenRepository defines the repository interface for Token.
type TokenRepository interface {
	Create(token *Token) error
	Get(id shared.ID) (*Token, error)
	GetByDataSource(dataSourceID shared.ID) (*Token, error)
	Update(token *Token) error
	Delete(id shared.ID) error
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
