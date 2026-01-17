// Package contracts defines application service interfaces and DTOs.
package contracts

import (
	"context"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// DataStoreApplicationService defines application service for data store management.
// Responsibilities:
//   - Orchestrate complete use cases
//   - Manage transactions
//   - Coordinate domain services and repositories
//   - Call QuantDB adapters
type DataStoreApplicationService interface {
	// ==================== Data Store Management ====================

	// CreateDataStore creates a new data store.
	CreateDataStore(ctx context.Context, req CreateDataStoreRequest) (*datastore.QuantDataStore, error)

	// GetDataStore retrieves a data store by ID.
	GetDataStore(ctx context.Context, id shared.ID) (*datastore.QuantDataStore, error)

	// UpdateDataStore updates a data store.
	UpdateDataStore(ctx context.Context, id shared.ID, req UpdateDataStoreRequest) error

	// DeleteDataStore deletes a data store.
	DeleteDataStore(ctx context.Context, id shared.ID) error

	// ListDataStores lists all data stores.
	ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error)

	// TestConnection tests the connection to a data store.
	TestConnection(ctx context.Context, id shared.ID) error

	// ==================== Table Schema Management ====================

	// GenerateTableSchema generates table schema from API metadata.
	// This is a complex use case involving:
	//   1. Retrieve API metadata
	//   2. Apply type mapping rules
	//   3. Generate column definitions
	//   4. Create TableSchema entity
	//   5. Generate and execute DDL
	GenerateTableSchema(ctx context.Context, req GenerateSchemaRequest) (*datastore.TableSchema, error)

	// CreateTable creates a table in the data store.
	CreateTable(ctx context.Context, schemaID shared.ID) error

	// CreateTablesForDatasource creates tables for all APIs of a data source in the data store.
	// This is an asynchronous operation that uses the built-in create_tables workflow.
	// Returns the workflow instance ID for tracking the execution status.
	CreateTablesForDatasource(ctx context.Context, req CreateTablesForDatasourceRequest) (shared.ID, error)

	// DropTable drops a table from the data store.
	DropTable(ctx context.Context, schemaID shared.ID) error

	// GetTableSchema retrieves a table schema by ID.
	GetTableSchema(ctx context.Context, id shared.ID) (*datastore.TableSchema, error)

	// GetTableSchemaByAPI retrieves table schema by API metadata ID.
	GetTableSchemaByAPI(ctx context.Context, apiMetadataID shared.ID) (*datastore.TableSchema, error)

	// ListTableSchemas lists all table schemas for a data store.
	ListTableSchemas(ctx context.Context, dataStoreID shared.ID) ([]*datastore.TableSchema, error)

	// UpdateTableSchema updates a table schema.
	UpdateTableSchema(ctx context.Context, id shared.ID, req UpdateSchemaRequest) error

	// SyncSchemaStatus synchronizes schema status with actual database state.
	SyncSchemaStatus(ctx context.Context, dataStoreID shared.ID) error

	// ==================== Type Mapping Rule Management ====================

	// CreateMappingRule creates a new type mapping rule.
	CreateMappingRule(ctx context.Context, req CreateMappingRuleRequest) (*datastore.DataTypeMappingRule, error)

	// GetMappingRules retrieves mapping rules for data source and target DB.
	GetMappingRules(ctx context.Context, dataSourceType, targetDBType string) ([]*datastore.DataTypeMappingRule, error)
}

// ==================== Request/Response DTOs ====================

// CreateDataStoreRequest represents a request to create a data store.
type CreateDataStoreRequest struct {
	Name        string
	Description string
	Type        datastore.DataStoreType
	DSN         string
	StoragePath string
}

// UpdateDataStoreRequest represents a request to update a data store.
type UpdateDataStoreRequest struct {
	Name        *string
	Description *string
	DSN         *string
	StoragePath *string
}

// GenerateSchemaRequest represents a request to generate table schema.
type GenerateSchemaRequest struct {
	APIMetadataID shared.ID
	DataStoreID   shared.ID
	TableName     string
	AutoCreate    bool // Whether to automatically create the table
}

// UpdateSchemaRequest represents a request to update table schema.
type UpdateSchemaRequest struct {
	Columns     *[]datastore.ColumnDef
	PrimaryKeys *[]string
	Indexes     *[]datastore.IndexDef
}

// CreateTablesForDatasourceRequest represents a request to create tables for a data source.
type CreateTablesForDatasourceRequest struct {
	DataSourceID shared.ID // 数据源 ID
	DataStoreID  shared.ID // 数据存储 ID
	MaxTables    *int      // 最大建表数量（可选，nil 或 0 表示不限制）
}

// CreateMappingRuleRequest represents a request to create a mapping rule.
type CreateMappingRuleRequest struct {
	DataSourceType string
	SourceType     string
	TargetDBType   string
	TargetType     string
	FieldPattern   *string
	Priority       int
}
