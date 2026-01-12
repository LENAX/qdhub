// Package datastore contains the datastore domain repository interfaces.
package datastore

import "qdhub/internal/domain/shared"

// QuantDataStoreRepository defines the repository interface for QuantDataStore aggregate.
type QuantDataStoreRepository interface {
	Create(ds *QuantDataStore) error
	Get(id shared.ID) (*QuantDataStore, error)
	Update(ds *QuantDataStore) error
	Delete(id shared.ID) error
	List() ([]*QuantDataStore, error)
}

// TableSchemaRepository defines the repository interface for TableSchema.
type TableSchemaRepository interface {
	Create(schema *TableSchema) error
	Get(id shared.ID) (*TableSchema, error)
	GetByDataStore(dataStoreID shared.ID) ([]*TableSchema, error)
	GetByAPIMetadata(apiMetadataID shared.ID) (*TableSchema, error)
	Update(schema *TableSchema) error
	Delete(id shared.ID) error
}
