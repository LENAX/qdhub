// Package metadata contains the metadata domain repository interfaces.
package metadata

import "qdhub/internal/domain/shared"

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
