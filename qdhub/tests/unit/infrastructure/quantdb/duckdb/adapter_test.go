package duckdb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/infrastructure/quantdb/duckdb"
)

func TestAdapter_Connect(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	// Test connect
	err := adapter.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Test ping
	err = adapter.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// Test close
	err = adapter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify db file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file was not created")
	}
}

func TestAdapter_CreateTable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	schema := &datastore.TableSchema{
		TableName: "test_table",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
			{Name: "value", TargetType: "DOUBLE", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}

	err := adapter.CreateTable(ctx, schema)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify table exists
	exists, err := adapter.TableExists(ctx, "test_table")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Error("Table should exist after creation")
	}
}

func TestAdapter_DropTable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Create table first
	schema := &datastore.TableSchema{
		TableName: "drop_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Drop table
	err := adapter.DropTable(ctx, "drop_test")
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// Verify table no longer exists
	exists, err := adapter.TableExists(ctx, "drop_test")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if exists {
		t.Error("Table should not exist after drop")
	}
}

func TestAdapter_BulkInsert(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Create table
	schema := &datastore.TableSchema{
		TableName: "insert_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
			{Name: "value", TargetType: "DOUBLE", Nullable: true},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert data
	data := []map[string]any{
		{"id": int64(1), "name": "test1", "value": 1.1},
		{"id": int64(2), "name": "test2", "value": 2.2},
		{"id": int64(3), "name": "test3", "value": 3.3},
	}

	inserted, err := adapter.BulkInsert(ctx, "insert_test", data)
	if err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}
	if inserted != 3 {
		t.Errorf("Expected 3 rows inserted, got %d", inserted)
	}

	// Verify data
	stats, err := adapter.GetTableStats(ctx, "insert_test")
	if err != nil {
		t.Fatalf("GetTableStats failed: %v", err)
	}
	if stats.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", stats.RowCount)
	}
}

func TestAdapter_BulkInsertWithBatchID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Create table with sync_batch_id column
	schema := &datastore.TableSchema{
		TableName: "batch_insert_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
			{Name: "sync_batch_id", TargetType: "VARCHAR", Nullable: true},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert data with batch ID
	data := []map[string]any{
		{"id": int64(1), "name": "test1"},
		{"id": int64(2), "name": "test2"},
	}
	batchID := "batch-123"

	inserted, err := adapter.BulkInsertWithBatchID(ctx, "batch_insert_test", data, batchID)
	if err != nil {
		t.Fatalf("BulkInsertWithBatchID failed: %v", err)
	}
	if inserted != 2 {
		t.Errorf("Expected 2 rows inserted, got %d", inserted)
	}

	// Verify batch ID is set
	results, err := adapter.Query(ctx, "SELECT sync_batch_id FROM batch_insert_test LIMIT 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(results))
	}
	if results[0]["sync_batch_id"] != batchID {
		t.Errorf("Expected batch ID %s, got %v", batchID, results[0]["sync_batch_id"])
	}
}

func TestAdapter_DeleteBySyncBatchID(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Create table
	schema := &datastore.TableSchema{
		TableName: "delete_batch_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
			{Name: "sync_batch_id", TargetType: "VARCHAR", Nullable: true},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert data with different batch IDs
	data1 := []map[string]any{
		{"id": int64(1), "sync_batch_id": "batch-1"},
		{"id": int64(2), "sync_batch_id": "batch-1"},
	}
	data2 := []map[string]any{
		{"id": int64(3), "sync_batch_id": "batch-2"},
	}

	adapter.BulkInsert(ctx, "delete_batch_test", data1)
	adapter.BulkInsert(ctx, "delete_batch_test", data2)

	// Delete batch-1
	deleted, err := adapter.DeleteBySyncBatchID(ctx, "delete_batch_test", "batch-1")
	if err != nil {
		t.Fatalf("DeleteBySyncBatchID failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Expected 2 rows deleted, got %d", deleted)
	}

	// Verify only batch-2 remains
	stats, err := adapter.GetTableStats(ctx, "delete_batch_test")
	if err != nil {
		t.Fatalf("GetTableStats failed: %v", err)
	}
	if stats.RowCount != 1 {
		t.Errorf("Expected 1 row remaining, got %d", stats.RowCount)
	}
}

func TestAdapter_Query(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Create and populate table
	schema := &datastore.TableSchema{
		TableName: "query_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	data := []map[string]any{
		{"id": int64(1), "name": "Alice"},
		{"id": int64(2), "name": "Bob"},
	}
	adapter.BulkInsert(ctx, "query_test", data)

	// Query data
	results, err := adapter.Query(ctx, "SELECT * FROM query_test WHERE id = ?", int64(1))
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0]["name"] != "Alice" {
		t.Errorf("Expected name 'Alice', got %v", results[0]["name"])
	}
}

func TestAdapter_Transaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Create table
	schema := &datastore.TableSchema{
		TableName: "tx_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Test commit
	tx, err := adapter.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	data := []map[string]any{
		{"id": int64(1), "name": "test"},
	}
	_, err = tx.BulkInsert(ctx, "tx_test", data)
	if err != nil {
		t.Fatalf("BulkInsert in tx failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data was committed
	stats, err := adapter.GetTableStats(ctx, "tx_test")
	if err != nil {
		t.Fatalf("GetTableStats failed: %v", err)
	}
	if stats.RowCount != 1 {
		t.Errorf("Expected 1 row after commit, got %d", stats.RowCount)
	}
}

func TestAdapter_TransactionRollback(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Create table
	schema := &datastore.TableSchema{
		TableName: "rollback_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Test rollback
	tx, err := adapter.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}

	data := []map[string]any{
		{"id": int64(1)},
	}
	_, err = tx.BulkInsert(ctx, "rollback_test", data)
	if err != nil {
		t.Fatalf("BulkInsert in tx failed: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify data was rolled back
	stats, err := adapter.GetTableStats(ctx, "rollback_test")
	if err != nil {
		t.Fatalf("GetTableStats failed: %v", err)
	}
	if stats.RowCount != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", stats.RowCount)
	}
}

func TestAdapter_CreateTableWithIndex(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	schema := &datastore.TableSchema{
		TableName: "index_test",
		Columns: []datastore.ColumnDef{
			{Name: "id", TargetType: "BIGINT", Nullable: false},
			{Name: "ts_code", TargetType: "VARCHAR(16)", Nullable: false},
			{Name: "trade_date", TargetType: "DATE", Nullable: false},
		},
		PrimaryKeys: []string{"id"},
		Indexes: []datastore.IndexDef{
			{Name: "idx_ts_code", Columns: []string{"ts_code"}, Unique: false},
			{Name: "idx_trade_date", Columns: []string{"trade_date"}, Unique: false},
		},
	}

	err := adapter.CreateTable(ctx, schema)
	if err != nil {
		t.Fatalf("CreateTable with indexes failed: %v", err)
	}

	// Verify table exists
	exists, err := adapter.TableExists(ctx, "index_test")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Error("Table should exist after creation")
	}
}
