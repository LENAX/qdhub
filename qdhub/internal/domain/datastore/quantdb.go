// Package datastore contains domain entities and interfaces for data store management.
package datastore

import (
	"context"
	"errors"
)

var (
	// ErrUnsupportedQuantDBType is returned when QuantDBFactory does not support the given type.
	ErrUnsupportedQuantDBType = func(t DataStoreType) error {
		return errors.New("unsupported quant db type: " + string(t))
	}
	// ErrQuantDBPathRequired is returned when StoragePath and DSN are both empty.
	ErrQuantDBPathRequired = errors.New("quant db storage path or DSN is required")
)

// QuantDB defines the interface for quant database operations.
// This interface abstracts the underlying database implementation (DuckDB, ClickHouse, etc.)
// and provides unified methods for table management and data operations.
//
// Implementation: infrastructure/quantdb/
type QuantDB interface {
	// ==================== Connection Management ====================

	// Connect establishes connection to the database.
	Connect(ctx context.Context) error

	// Close closes the database connection.
	Close() error

	// Ping tests the database connection.
	Ping(ctx context.Context) error

	// ==================== Table Operations ====================

	// CreateTable creates a table based on the schema definition.
	CreateTable(ctx context.Context, schema *TableSchema) error

	// DropTable drops a table by name.
	DropTable(ctx context.Context, tableName string) error

	// TableExists checks if a table exists.
	TableExists(ctx context.Context, tableName string) (bool, error)

	// ListTables returns table names in the database (e.g. main schema).
	ListTables(ctx context.Context) ([]string, error)

	// GetTableStats returns statistics for a table.
	GetTableStats(ctx context.Context, tableName string) (*TableStats, error)

	// ==================== Data Operations ====================

	// BulkInsert inserts multiple rows into a table.
	// Returns the number of rows inserted.
	BulkInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error)

	// BulkInsertWithBatchID inserts multiple rows with a sync batch ID for rollback support.
	// Returns the number of rows inserted.
	BulkInsertWithBatchID(ctx context.Context, tableName string, data []map[string]any, syncBatchID string) (int64, error)

	// DeleteBySyncBatchID deletes all rows with the given sync batch ID.
	// Used for rollback during failed sync operations.
	DeleteBySyncBatchID(ctx context.Context, tableName string, syncBatchID string) (int64, error)

	// Query executes a SQL query and returns the results.
	Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error)

	// Execute executes a SQL statement (INSERT, UPDATE, DELETE) and returns affected rows.
	Execute(ctx context.Context, sql string, args ...any) (int64, error)

	// ==================== Transaction Support ====================

	// BeginTx begins a transaction and returns a transaction context.
	BeginTx(ctx context.Context) (QuantDBTx, error)
}

// QuantDBTx represents a database transaction.
type QuantDBTx interface {
	// Commit commits the transaction.
	Commit() error

	// Rollback rolls back the transaction.
	Rollback() error

	// BulkInsert inserts multiple rows within the transaction.
	BulkInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error)

	// Execute executes a SQL statement within the transaction.
	Execute(ctx context.Context, sql string, args ...any) (int64, error)
}

// TableStats holds statistics for a table.
type TableStats struct {
	RowCount      int64  `json:"row_count"`
	SizeBytes     int64  `json:"size_bytes"`
	LastUpdatedAt string `json:"last_updated_at,omitempty"`
}

// InsertResult holds the result of a bulk insert operation.
type InsertResult struct {
	RowsInserted int64  `json:"rows_inserted"`
	SyncBatchID  string `json:"sync_batch_id,omitempty"`
}

// QuantDBConfig holds configuration for database connection.
type QuantDBConfig struct {
	Type        DataStoreType `json:"type"`
	DSN         string        `json:"dsn,omitempty"`
	StoragePath string        `json:"storage_path,omitempty"`
}

// QuantDBFactory creates QuantDB instances based on configuration.
type QuantDBFactory interface {
	// Create creates a new QuantDB instance based on the configuration.
	Create(config QuantDBConfig) (QuantDB, error)
}

// QuantDBBatchWriteRequest holds parameters for a batch write operation.
type QuantDBBatchWriteRequest struct {
	Path        string           // Target DuckDB path
	TableName   string           // Table to write into
	Data        []map[string]any // Data to write
	SyncBatchID string           // Optional batch ID for rollback support
}

// QuantDBWriteQueue provides an asynchronous queue for batching writes to QuantDB.
// It handles concurrent writes to the same DB path by serializing them and flushing
// them according to a batch size or time limit.
type QuantDBWriteQueue interface {
	// Enqueue adds a write request to the queue. Returns an error if the queue rejects it
	// (e.g., due to critical memory pressure). This method does not wait for the write to complete.
	Enqueue(ctx context.Context, req QuantDBBatchWriteRequest) error

	// EnqueueAndWait adds a write request and blocks until the data is actually written to the DB.
	// Returns the number of rows inserted and any error that occurred during the write.
	EnqueueAndWait(ctx context.Context, req QuantDBBatchWriteRequest) (int64, error)

	// Close shuts down the queue and flushes all pending writes.
	Close() error
}
