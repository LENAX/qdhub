// Package duckdb provides DuckDB adapter implementation for QuantDB interface.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/marcboeker/go-duckdb" // DuckDB driver

	"qdhub/internal/domain/datastore"
)

// Adapter implements the QuantDB interface for DuckDB.
type Adapter struct {
	db          *sql.DB
	storagePath string
	mu          sync.RWMutex
	connected   bool
}

// NewAdapter creates a new DuckDB adapter.
func NewAdapter(storagePath string) *Adapter {
	return &Adapter{
		storagePath: storagePath,
	}
}

// ==================== Connection Management ====================

// Connect establishes connection to the DuckDB database.
func (a *Adapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.connected {
		return nil
	}

	db, err := sql.Open("duckdb", a.storagePath)
	if err != nil {
		return fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping duckdb: %w", err)
	}

	a.db = db
	a.connected = true
	return nil
}

// Close closes the database connection.
func (a *Adapter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.connected || a.db == nil {
		return nil
	}

	err := a.db.Close()
	a.connected = false
	a.db = nil
	return err
}

// Ping tests the database connection.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if !a.connected || a.db == nil {
		return fmt.Errorf("database not connected")
	}

	return a.db.PingContext(ctx)
}

// ==================== Table Operations ====================

// CreateTable creates a table based on the schema definition.
func (a *Adapter) CreateTable(ctx context.Context, schema *datastore.TableSchema) error {
	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	ddl := a.generateDDL(schema)

	_, err := a.db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", schema.TableName, err)
	}

	// Create indexes
	for _, idx := range schema.Indexes {
		indexDDL := a.generateIndexDDL(schema.TableName, idx)
		if _, err := a.db.ExecContext(ctx, indexDDL); err != nil {
			return fmt.Errorf("failed to create index %s: %w", idx.Name, err)
		}
	}

	return nil
}

// DropTable drops a table by name.
func (a *Adapter) DropTable(ctx context.Context, tableName string) error {
	ddl := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := a.db.ExecContext(ctx, ddl)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}
	return nil
}

// TableExists checks if a table exists.
func (a *Adapter) TableExists(ctx context.Context, tableName string) (bool, error) {
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`
	var count int
	err := a.db.QueryRowContext(ctx, query, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}
	return count > 0, nil
}

// GetTableStats returns statistics for a table.
func (a *Adapter) GetTableStats(ctx context.Context, tableName string) (*datastore.TableStats, error) {
	// Get row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var rowCount int64
	if err := a.db.QueryRowContext(ctx, countQuery).Scan(&rowCount); err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	// DuckDB doesn't have a direct way to get table size, so we estimate
	stats := &datastore.TableStats{
		RowCount:  rowCount,
		SizeBytes: 0, // Not directly available in DuckDB
	}

	return stats, nil
}

// ==================== Data Operations ====================

// BulkInsert inserts multiple rows into a table.
func (a *Adapter) BulkInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Get column names from first row
	columns := make([]string, 0)
	for k := range data[0] {
		columns = append(columns, k)
	}

	// Build INSERT statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	// Use prepared statement for batch insert
	stmt, err := a.db.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	var totalInserted int64
	for _, row := range data {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row[col]
		}

		result, err := stmt.ExecContext(ctx, args...)
		if err != nil {
			return totalInserted, fmt.Errorf("failed to insert row: %w", err)
		}

		affected, _ := result.RowsAffected()
		totalInserted += affected
	}

	return totalInserted, nil
}

// BulkInsertWithBatchID inserts multiple rows with a sync batch ID for rollback support.
func (a *Adapter) BulkInsertWithBatchID(ctx context.Context, tableName string, data []map[string]any, syncBatchID string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Add sync_batch_id to each row
	for i := range data {
		data[i]["sync_batch_id"] = syncBatchID
	}

	return a.BulkInsert(ctx, tableName, data)
}

// DeleteBySyncBatchID deletes all rows with the given sync batch ID.
func (a *Adapter) DeleteBySyncBatchID(ctx context.Context, tableName string, syncBatchID string) (int64, error) {
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE sync_batch_id = ?", tableName)
	result, err := a.db.ExecContext(ctx, deleteSQL, syncBatchID)
	if err != nil {
		return 0, fmt.Errorf("failed to delete by sync batch ID: %w", err)
	}
	return result.RowsAffected()
}

// Query executes a SQL query and returns the results.
func (a *Adapter) Query(ctx context.Context, sqlQuery string, args ...any) ([]map[string]any, error) {
	rows, err := a.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]any)
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return results, nil
}

// Execute executes a SQL statement and returns affected rows.
func (a *Adapter) Execute(ctx context.Context, sqlStmt string, args ...any) (int64, error) {
	result, err := a.db.ExecContext(ctx, sqlStmt, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute statement: %w", err)
	}
	return result.RowsAffected()
}

// ==================== Transaction Support ====================

// BeginTx begins a transaction.
func (a *Adapter) BeginTx(ctx context.Context) (datastore.QuantDBTx, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &duckDBTx{tx: tx}, nil
}

// ==================== DDL Generation ====================

// generateDDL generates CREATE TABLE DDL for DuckDB.
func (a *Adapter) generateDDL(schema *datastore.TableSchema) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", schema.TableName))

	// Add columns
	for i, col := range schema.Columns {
		if i > 0 {
			ddl.WriteString(",\n")
		}
		ddl.WriteString(fmt.Sprintf("  %s %s", col.Name, col.TargetType))
		if !col.Nullable {
			ddl.WriteString(" NOT NULL")
		}
		if col.Default != nil {
			ddl.WriteString(fmt.Sprintf(" DEFAULT %s", *col.Default))
		}
	}

	// Add primary key constraint
	if len(schema.PrimaryKeys) > 0 {
		ddl.WriteString(",\n")
		ddl.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(schema.PrimaryKeys, ", ")))
	}

	ddl.WriteString("\n)")

	return ddl.String()
}

// generateIndexDDL generates CREATE INDEX DDL for DuckDB.
func (a *Adapter) generateIndexDDL(tableName string, index datastore.IndexDef) string {
	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		uniqueStr,
		index.Name,
		tableName,
		strings.Join(index.Columns, ", "))
}

// ==================== Transaction Implementation ====================

type duckDBTx struct {
	tx *sql.Tx
}

// Commit commits the transaction.
func (t *duckDBTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction.
func (t *duckDBTx) Rollback() error {
	return t.tx.Rollback()
}

// BulkInsert inserts multiple rows within the transaction.
func (t *duckDBTx) BulkInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Get column names from first row
	columns := make([]string, 0)
	for k := range data[0] {
		columns = append(columns, k)
	}

	// Build INSERT statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := t.tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	var totalInserted int64
	for _, row := range data {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row[col]
		}

		result, err := stmt.ExecContext(ctx, args...)
		if err != nil {
			return totalInserted, fmt.Errorf("failed to insert row: %w", err)
		}

		affected, _ := result.RowsAffected()
		totalInserted += affected
	}

	return totalInserted, nil
}

// Execute executes a SQL statement within the transaction.
func (t *duckDBTx) Execute(ctx context.Context, sqlStmt string, args ...any) (int64, error) {
	result, err := t.tx.ExecContext(ctx, sqlStmt, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute statement: %w", err)
	}
	return result.RowsAffected()
}

// Ensure Adapter implements QuantDB interface
var _ datastore.QuantDB = (*Adapter)(nil)
