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

// DataTypeMappingRuleRepository defines the repository interface for DataTypeMappingRule.
type DataTypeMappingRuleRepository interface {
	// Create creates a new mapping rule.
	Create(rule *DataTypeMappingRule) error

	// Get retrieves a mapping rule by ID.
	Get(id shared.ID) (*DataTypeMappingRule, error)

	// GetBySourceAndTarget retrieves rules by source and target types.
	// Returns rules ordered by priority (descending).
	GetBySourceAndTarget(dataSourceType, targetDBType string) ([]*DataTypeMappingRule, error)

	// SaveBatch saves multiple rules in a batch.
	SaveBatch(rules []*DataTypeMappingRule) error

	// InitDefaultRules initializes default mapping rules.
	InitDefaultRules() error

	// List retrieves all rules.
	List() ([]*DataTypeMappingRule, error)

	// Update updates an existing rule.
	Update(rule *DataTypeMappingRule) error

	// Delete deletes a rule by ID.
	Delete(id shared.ID) error
}
