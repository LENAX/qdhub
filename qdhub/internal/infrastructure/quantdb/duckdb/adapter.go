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
	ddl := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(tableName))
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

// ListTables returns table names in the main schema.
func (a *Adapter) ListTables(ctx context.Context) ([]string, error) {
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name`
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}
	return names, nil
}

// GetTableStats returns statistics for a table.
func (a *Adapter) GetTableStats(ctx context.Context, tableName string) (*datastore.TableStats, error) {
	// Get row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(tableName))
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

// tableHasPrimaryKeyOrUnique checks if a table has a primary key or unique constraint.
// This is needed to determine whether to use INSERT OR REPLACE (upsert) or regular INSERT.
func (a *Adapter) tableHasPrimaryKeyOrUnique(ctx context.Context, tableName string) (bool, error) {
	// Query DuckDB's table constraints
	// DuckDB stores constraint info in duckdb_constraints() function
	query := `
		SELECT COUNT(*) > 0 as has_pk
		FROM duckdb_constraints()
		WHERE table_name = ? AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
	`
	var hasPK bool
	err := a.db.QueryRowContext(ctx, query, tableName).Scan(&hasPK)
	if err != nil {
		// If query fails, try alternative method using table info
		return a.tableHasPrimaryKeyFallback(ctx, tableName)
	}
	return hasPK, nil
}

// tableHasPrimaryKeyFallback is a fallback method to check for primary key.
func (a *Adapter) tableHasPrimaryKeyFallback(ctx context.Context, tableName string) (bool, error) {
	// Try using PRAGMA table_info
	// Note: DuckDB returns BOOLEAN for notnull and pk columns, not INT
	query := fmt.Sprintf("PRAGMA table_info('%s')", tableName)
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull bool
		var dflt_value interface{}
		var pk bool
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt_value, &pk); err != nil {
			continue
		}
		if pk {
			return true, nil
		}
	}
	return false, nil
}

// ==================== Data Operations ====================

// isConflictError 判断是否为唯一约束冲突/更新冲突（可重试为 update 或跳过）
func isConflictError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "Conflict") ||
		strings.Contains(s, "UNIQUE constraint") ||
		strings.Contains(s, "duplicate key") ||
		strings.Contains(s, "Duplicate")
}

// getTableColumns returns the column names of a table.
// This is used to filter out unknown columns when inserting data.
func (a *Adapter) getTableColumns(ctx context.Context, tableName string) (map[string]bool, error) {
	// Note: DuckDB returns BOOLEAN for notnull and pk columns, not INT
	query := fmt.Sprintf("PRAGMA table_info('%s')", tableName)
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull bool
		var dfltValue interface{}
		var pk bool
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk); err != nil {
			continue
		}
		columns[name] = true
	}
	return columns, nil
}

// BulkInsert inserts multiple rows into a table within a transaction.
// If the table has a primary key or unique index, uses INSERT OR REPLACE for upsert.
// Otherwise, uses regular INSERT INTO to avoid DuckDB's "ON CONFLICT is a no-op" error.
// Note: Only columns that exist in the table will be inserted; unknown columns from data are ignored.
func (a *Adapter) BulkInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Get actual table columns to filter out unknown columns from data
	tableColumns, err := a.getTableColumns(ctx, tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to get table columns: %w", err)
	}

	// Get column names from first row, but only include columns that exist in the table
	columns := make([]string, 0)
	skippedColumns := make([]string, 0)
	for k := range data[0] {
		if tableColumns[k] {
			columns = append(columns, k)
		} else {
			skippedColumns = append(skippedColumns, k)
		}
	}

	// Log skipped columns for debugging (only log once per batch)
	if len(skippedColumns) > 0 {
		fmt.Printf("⚠️ [BulkInsert] Table %s: skipping %d unknown columns: %v\n",
			tableName, len(skippedColumns), skippedColumns)
	}

	if len(columns) == 0 {
		return 0, fmt.Errorf("no valid columns to insert into table %s", tableName)
	}

	// Check if table has primary key or unique index
	hasPK, err := a.tableHasPrimaryKeyOrUnique(ctx, tableName)
	if err != nil {
		// If check fails, fall back to regular INSERT
		hasPK = false
	}

	// Build INSERT statement
	// Use INSERT OR REPLACE only if table has PK/unique constraint
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	var insertSQL string
	if hasPK {
		insertSQL = fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
			quoteIdentifier(tableName),
			quoteIdentifiers(columns),
			strings.Join(placeholders, ", "))
	} else {
		insertSQL = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			quoteIdentifier(tableName),
			quoteIdentifiers(columns),
			strings.Join(placeholders, ", "))
	}

	// Begin transaction for batch insert (improves performance and ensures atomicity)
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Use prepared statement within transaction
	stmt, err := tx.PrepareContext(ctx, insertSQL)
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

		result, execErr := stmt.ExecContext(ctx, args...)
		if execErr != nil {
			if isConflictError(execErr) {
				_ = tx.Rollback()
				// 冲突时改为逐行 INSERT OR REPLACE / INSERT，失败则跳过
				n, fallbackErr := a.bulkInsertRowByRow(ctx, tableName, data, columns, hasPK)
				if fallbackErr != nil {
					return n, fallbackErr
				}
				return n, nil
			}
			err = execErr
			return totalInserted, fmt.Errorf("failed to insert row: %w", execErr)
		}

		affected, _ := result.RowsAffected()
		totalInserted += affected
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return totalInserted, nil
}

// bulkInsertRowByRow 无事务逐行插入，冲突时用 INSERT OR REPLACE 尝试更新，仍失败则跳过该行。
func (a *Adapter) bulkInsertRowByRow(ctx context.Context, tableName string, data []map[string]any, columns []string, useReplace bool) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	verb := "INSERT"
	if useReplace {
		verb = "INSERT OR REPLACE"
	}
	insertSQL := fmt.Sprintf("%s INTO %s (%s) VALUES (%s)",
		verb,
		quoteIdentifier(tableName),
		quoteIdentifiers(columns),
		strings.Join(placeholders, ", "))

	var totalInserted int64
	for _, row := range data {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row[col]
		}
		result, err := a.db.ExecContext(ctx, insertSQL, args...)
		if err != nil {
			if isConflictError(err) {
				// 已用 OR REPLACE 仍冲突则跳过
				continue
			}
			return totalInserted, fmt.Errorf("failed to insert row: %w", err)
		}
		affected, _ := result.RowsAffected()
		totalInserted += affected
	}
	return totalInserted, nil
}

// BulkInsertWithBatchID inserts multiple rows with a sync batch ID for rollback support.
// Copies data internally so the caller's slice is not mutated.
func (a *Adapter) BulkInsertWithBatchID(ctx context.Context, tableName string, data []map[string]any, syncBatchID string) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Copy rows and add sync_batch_id to avoid mutating caller's data (e.g. result.Data used later for cache/extractKeyFields).
	dataCopy := make([]map[string]any, len(data))
	for i, row := range data {
		rowCopy := make(map[string]any, len(row)+1)
		for k, v := range row {
			rowCopy[k] = v
		}
		rowCopy["sync_batch_id"] = syncBatchID
		dataCopy[i] = rowCopy
	}
	return a.BulkInsert(ctx, tableName, dataCopy)
}

// DeleteBySyncBatchID deletes all rows with the given sync batch ID.
func (a *Adapter) DeleteBySyncBatchID(ctx context.Context, tableName string, syncBatchID string) (int64, error) {
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quoteIdentifier(tableName), quoteIdentifier("sync_batch_id"))
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

// quoteIdentifier wraps an identifier (table name, column name) in double quotes
// to handle SQL reserved words and special characters safely.
// DuckDB uses double quotes for identifier quoting (SQL standard).
func quoteIdentifier(name string) string {
	// Escape any existing double quotes by doubling them
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// quoteIdentifiers wraps multiple identifiers and joins them with comma.
func quoteIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteIdentifier(name)
	}
	return strings.Join(quoted, ", ")
}

// generateDDL generates CREATE TABLE DDL for DuckDB.
func (a *Adapter) generateDDL(schema *datastore.TableSchema) string {
	var ddl strings.Builder
	ddl.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", quoteIdentifier(schema.TableName)))

	// Add columns
	for i, col := range schema.Columns {
		if i > 0 {
			ddl.WriteString(",\n")
		}
		ddl.WriteString(fmt.Sprintf("  %s %s", quoteIdentifier(col.Name), col.TargetType))
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
		ddl.WriteString(fmt.Sprintf("  PRIMARY KEY (%s)", quoteIdentifiers(schema.PrimaryKeys)))
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
		quoteIdentifier(index.Name),
		quoteIdentifier(tableName),
		quoteIdentifiers(index.Columns))
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
// Note: Transaction BulkInsert uses regular INSERT as we can't easily check
// for primary keys within a transaction context without additional complexity.
func (t *duckDBTx) BulkInsert(ctx context.Context, tableName string, data []map[string]any) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Get column names from first row
	columns := make([]string, 0)
	for k := range data[0] {
		columns = append(columns, k)
	}

	// Build INSERT statement (use regular INSERT in transaction context)
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(tableName),
		quoteIdentifiers(columns),
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
			if isConflictError(err) {
				// 冲突时跳过该行
				continue
			}
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
