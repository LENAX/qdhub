// Package datastore contains the datastore domain repository interfaces.
package datastore

import "qdhub/internal/domain/shared"

// QuantDataStoreRepository defines the repository interface for QuantDataStore aggregate.
// Following DDD principles, this repository handles both the aggregate root (QuantDataStore)
// and its child entities (TableSchema) to maintain aggregate boundaries.
//
// Embeds shared.Repository[QuantDataStore] to inherit common CRUD operations.
type QuantDataStoreRepository interface {
	// Embed base repository for common CRUD operations
	shared.Repository[QuantDataStore]

	// ==================== Child Entity Operations (TableSchema) ====================

	// AddSchema adds a new TableSchema to a QuantDataStore.
	AddSchema(schema *TableSchema) error

	// GetSchema retrieves a TableSchema by ID.
	GetSchema(id shared.ID) (*TableSchema, error)

	// GetSchemaByAPIMetadata retrieves a TableSchema by API metadata ID.
	GetSchemaByAPIMetadata(apiMetadataID shared.ID) (*TableSchema, error)

	// GetSchemasByDataStore retrieves all TableSchemas for a QuantDataStore.
	GetSchemasByDataStore(dataStoreID shared.ID) ([]*TableSchema, error)

	// UpdateSchema updates a TableSchema.
	UpdateSchema(schema *TableSchema) error

	// DeleteSchema deletes a TableSchema by ID.
	DeleteSchema(id shared.ID) error
}

// DataTypeMappingRuleRepository defines the repository interface for DataTypeMappingRule.
//
// Embeds shared.Repository[DataTypeMappingRule] to inherit common CRUD operations.
type DataTypeMappingRuleRepository interface {
	// Embed base repository for common CRUD operations
	shared.Repository[DataTypeMappingRule]

	// GetBySourceAndTarget retrieves rules by source and target types.
	// Returns rules ordered by priority (descending).
	GetBySourceAndTarget(dataSourceType, targetDBType string) ([]*DataTypeMappingRule, error)

	// SaveBatch saves multiple rules in a batch.
	SaveBatch(rules []*DataTypeMappingRule) error

	// InitDefaultRules initializes default mapping rules.
	InitDefaultRules() error
}
