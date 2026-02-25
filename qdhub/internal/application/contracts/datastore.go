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

	// CreateDataStore creates a new data store. For DuckDB, creates the DB file if missing; for others, attempts connection. Runs validation after create.
	CreateDataStore(ctx context.Context, req CreateDataStoreRequest) (*datastore.QuantDataStore, error)

	// GetDataStore retrieves a data store by ID.
	GetDataStore(ctx context.Context, id shared.ID) (*datastore.QuantDataStore, error)

	// ListDataStores lists all data stores.
	ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error)

	// UpdateDataStore updates an existing data store (name, description, type, dsn, storage_path).
	UpdateDataStore(ctx context.Context, id shared.ID, req UpdateDataStoreRequest) (*datastore.QuantDataStore, error)

	// DeleteDataStore deletes a data store. Fails if any sync plan references it.
	DeleteDataStore(ctx context.Context, id shared.ID) error

	// ValidateDataStore checks whether the data store's database actually exists and is reachable.
	ValidateDataStore(ctx context.Context, id shared.ID) (*ValidateDataStoreResult, error)

	// CreateTablesForDatasource creates tables for all APIs of a data source in the data store.
	// This is an asynchronous operation that uses the built-in create_tables workflow.
	// Returns the workflow instance ID for tracking the execution status.
	CreateTablesForDatasource(ctx context.Context, req CreateTablesForDatasourceRequest) (shared.ID, error)
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
	Type        *datastore.DataStoreType
	DSN         *string
	StoragePath *string
}

// ValidateDataStoreResult is the result of validating a data store (DB existence/reachability).
type ValidateDataStoreResult struct {
	Valid   bool   `json:"valid"`
	Message string `json:"message,omitempty"` // error message when valid is false
}

// CreateTablesForDatasourceRequest represents a request to create tables for a data source.
type CreateTablesForDatasourceRequest struct {
	DataSourceID shared.ID // 数据源 ID
	DataStoreID  shared.ID // 数据存储 ID
	MaxTables    *int      // 最大建表数量（可选，nil 或 0 表示不限制）
}
