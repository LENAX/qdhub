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

	// ==================== Data Browser ====================

	// ListDatastoreTables lists table names in the given data store's database.
	ListDatastoreTables(ctx context.Context, id shared.ID) ([]string, error)

	// GetDatastoreTableData returns a page of rows from a table and the total row count.
	// tableName must be one of the names returned by ListDatastoreTables (whitelist).
	// If searchQ is non-empty, filters rows by searchQ (ILIKE); searchColumn restricts to one column (must be in table) or empty for all columns.
	// orderBy is a table column name for ORDER BY (whitelisted); order is "asc" or "desc" (default "asc").
	GetDatastoreTableData(ctx context.Context, id shared.ID, tableName string, page, pageSize int, searchQ, searchColumn, orderBy, order string) (rows []map[string]any, total int64, err error)
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

// TableDataPage holds a page of table rows and total count for data browser.
type TableDataPage struct {
	Rows  []map[string]any `json:"data"`
	Total int64            `json:"total"`
}
