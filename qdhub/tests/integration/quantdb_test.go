//go:build integration

package integration

import (
	"context"
	"path/filepath"
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/infrastructure/quantdb/duckdb"
	"qdhub/pkg/typemap"
)

// TestQuantDB_EndToEnd tests the full workflow from schema generation to data operations.
func TestQuantDB_EndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "quant.db")

	// Create DuckDB adapter
	adapter := duckdb.NewAdapter(dbPath)
	ctx := context.Background()

	// Connect
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer adapter.Close()

	// Test ping
	if err := adapter.Ping(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	// Define API metadata fields (simulating Tushare daily API)
	fields := []metadata.FieldMeta{
		{Name: "ts_code", Type: "str", IsPrimary: true, Description: "股票代码"},
		{Name: "trade_date", Type: "str", IsPrimary: true, Description: "交易日期"},
		{Name: "open", Type: "float", Description: "开盘价"},
		{Name: "high", Type: "float", Description: "最高价"},
		{Name: "low", Type: "float", Description: "最低价"},
		{Name: "close", Type: "float", Description: "收盘价"},
		{Name: "vol", Type: "float", Description: "成交量"},
		{Name: "amount", Type: "float", Description: "成交额"},
		{Name: "pct_chg", Type: "float", Description: "涨跌幅"},
	}

	// Use type mapper to generate columns
	typeMapper := typemap.NewDefaultTypeMapper()
	columns := typeMapper.MapAllFields(fields, "tushare", datastore.DataStoreTypeDuckDB)

	// Create table schema
	schema := &datastore.TableSchema{
		TableName:   "daily",
		Columns:     columns,
		PrimaryKeys: []string{"ts_code", "trade_date"},
		Indexes: []datastore.IndexDef{
			{Name: "idx_ts_code", Columns: []string{"ts_code"}, Unique: false},
			{Name: "idx_trade_date", Columns: []string{"trade_date"}, Unique: false},
		},
	}

	// Add sync_batch_id column for rollback support
	schema.Columns = append(schema.Columns, datastore.ColumnDef{
		Name:       "sync_batch_id",
		SourceType: "str",
		TargetType: "VARCHAR",
		Nullable:   true,
	})

	// Create table
	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify table exists
	exists, err := adapter.TableExists(ctx, "daily")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Fatal("Table 'daily' should exist")
	}

	// Insert test data (using YYYY-MM-DD format for DuckDB DATE type)
	testData := []map[string]any{
		{
			"ts_code":    "000001.SZ",
			"trade_date": "2024-01-01",
			"open":       10.5,
			"high":       11.0,
			"low":        10.2,
			"close":      10.8,
			"vol":        1000000.0,
			"amount":     10800000.0,
			"pct_chg":    2.86,
		},
		{
			"ts_code":    "000001.SZ",
			"trade_date": "2024-01-02",
			"open":       10.8,
			"high":       11.2,
			"low":        10.6,
			"close":      11.0,
			"vol":        1200000.0,
			"amount":     13200000.0,
			"pct_chg":    1.85,
		},
	}

	batchID := "sync-batch-001"
	inserted, err := adapter.BulkInsertWithBatchID(ctx, "daily", testData, batchID)
	if err != nil {
		t.Fatalf("BulkInsertWithBatchID failed: %v", err)
	}
	if inserted != 2 {
		t.Errorf("Expected 2 rows inserted, got %d", inserted)
	}

	// Query data
	results, err := adapter.Query(ctx, "SELECT ts_code, trade_date, close FROM daily ORDER BY trade_date")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Verify first row
	if results[0]["ts_code"] != "000001.SZ" {
		t.Errorf("Expected ts_code '000001.SZ', got %v", results[0]["ts_code"])
	}

	// Get table stats
	stats, err := adapter.GetTableStats(ctx, "daily")
	if err != nil {
		t.Fatalf("GetTableStats failed: %v", err)
	}
	if stats.RowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", stats.RowCount)
	}

	// Test rollback by deleting batch
	deleted, err := adapter.DeleteBySyncBatchID(ctx, "daily", batchID)
	if err != nil {
		t.Fatalf("DeleteBySyncBatchID failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Expected 2 rows deleted, got %d", deleted)
	}

	// Verify table is empty
	stats, err = adapter.GetTableStats(ctx, "daily")
	if err != nil {
		t.Fatalf("GetTableStats failed: %v", err)
	}
	if stats.RowCount != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", stats.RowCount)
	}

	// Drop table
	if err := adapter.DropTable(ctx, "daily"); err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// Verify table no longer exists
	exists, err = adapter.TableExists(ctx, "daily")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if exists {
		t.Error("Table 'daily' should not exist after drop")
	}
}

// TestQuantDB_Transaction tests transaction commit and rollback.
func TestQuantDB_Transaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tx_test.db")

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
			{Name: "value", TargetType: "VARCHAR", Nullable: true},
		},
	}

	if err := adapter.CreateTable(ctx, schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	t.Run("Commit", func(t *testing.T) {
		tx, err := adapter.BeginTx(ctx)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		data := []map[string]any{
			{"id": int64(1), "value": "committed"},
		}
		_, err = tx.BulkInsert(ctx, "tx_test", data)
		if err != nil {
			t.Fatalf("BulkInsert failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		stats, _ := adapter.GetTableStats(ctx, "tx_test")
		if stats.RowCount != 1 {
			t.Errorf("Expected 1 row after commit, got %d", stats.RowCount)
		}
	})

	t.Run("Rollback", func(t *testing.T) {
		tx, err := adapter.BeginTx(ctx)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		data := []map[string]any{
			{"id": int64(2), "value": "should_be_rolled_back"},
		}
		_, err = tx.BulkInsert(ctx, "tx_test", data)
		if err != nil {
			t.Fatalf("BulkInsert failed: %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		// Should still have only 1 row from commit test
		stats, _ := adapter.GetTableStats(ctx, "tx_test")
		if stats.RowCount != 1 {
			t.Errorf("Expected 1 row after rollback, got %d", stats.RowCount)
		}
	})
}

// TestTypeMapper_Integration tests type mapper with real schema generation.
func TestTypeMapper_Integration(t *testing.T) {
	// Test with rule-based mapper
	pattern := `^custom_.*$`
	rules := []*datastore.DataTypeMappingRule{
		{
			DataSourceType: "tushare",
			SourceType:     "str",
			TargetDBType:   "duckdb",
			TargetType:     "VARCHAR(200)",
			FieldPattern:   &pattern,
			Priority:       100,
		},
	}

	typeMappingSvc := datastore.NewTypeMappingService()
	mapper := typemap.NewRuleBasedTypeMapper(rules, typeMappingSvc)

	fields := []metadata.FieldMeta{
		{Name: "custom_field", Type: "str"},
		{Name: "normal_field", Type: "str"},
		{Name: "ts_code", Type: "str", IsPrimary: true},
	}

	columns := mapper.MapAllFields(fields, "tushare", datastore.DataStoreTypeDuckDB)

	// custom_field should use rule
	if columns[0].TargetType != "VARCHAR(200)" {
		t.Errorf("Expected 'VARCHAR(200)' for custom_field, got '%s'", columns[0].TargetType)
	}

	// normal_field should use default
	if columns[1].TargetType != "VARCHAR" {
		t.Errorf("Expected 'VARCHAR' for normal_field, got '%s'", columns[1].TargetType)
	}

	// ts_code - RuleBasedTypeMapper doesn't have ts_code pattern rule, falls back to default
	// When no matching rule found, it uses default VARCHAR
	if columns[2].TargetType != "VARCHAR" {
		t.Errorf("Expected 'VARCHAR' for ts_code (no matching rule), got '%s'", columns[2].TargetType)
	}
}
