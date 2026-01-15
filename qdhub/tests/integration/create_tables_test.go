// +build integration

package integration

import (
	"database/sql"
	"os"
	"testing"

	"qdhub/internal/infrastructure/taskengine/workflows"
)

// ==================== Create Tables Workflow 集成测试 ====================

// TestCreateTablesWorkflow_Build 测试工作流构建
func TestCreateTablesWorkflow_Build(t *testing.T) {
	t.Run("Valid params should create builder", func(t *testing.T) {
		builder := workflows.NewCreateTablesWorkflowBuilder(nil).
			WithDataSource("ds-001", "tushare").
			WithTargetDB("/tmp/stock.db").
			WithMaxTables(50)

		if builder == nil {
			t.Error("Builder should not be nil")
		}
	})

	t.Run("Builder chaining", func(t *testing.T) {
		builder := workflows.NewCreateTablesWorkflowBuilder(nil).
			WithParams(workflows.CreateTablesParams{
				DataSourceID:   "ds-001",
				DataSourceName: "tushare",
				TargetDBPath:   "/tmp/stock.db",
				MaxTables:      100,
			})

		if builder == nil {
			t.Error("Builder with params should not be nil")
		}
	})
}

// TestCreateTablesParams_Defaults 测试参数默认值
func TestCreateTablesParams_Defaults(t *testing.T) {
	params := workflows.CreateTablesParams{
		DataSourceID:   "ds-001",
		DataSourceName: "tushare",
		TargetDBPath:   "/tmp/stock.db",
	}

	if params.DataSourceID != "ds-001" {
		t.Errorf("Expected DataSourceID 'ds-001', got '%s'", params.DataSourceID)
	}
	if params.TargetDBPath != "/tmp/stock.db" {
		t.Errorf("Expected TargetDBPath '/tmp/stock.db', got '%s'", params.TargetDBPath)
	}
	if params.MaxTables != 0 {
		t.Errorf("Expected MaxTables 0 (no limit), got %d", params.MaxTables)
	}
}

// TestWorkflowFactory_CreateTables 测试工作流工厂
func TestWorkflowFactory_CreateTables(t *testing.T) {
	factory := workflows.NewWorkflowFactory(nil)

	t.Run("CreateTables builder method", func(t *testing.T) {
		builder := factory.CreateTables()
		if builder == nil {
			t.Error("CreateTables() should return non-nil builder")
		}
	})

	t.Run("Builder with all options", func(t *testing.T) {
		builder := factory.CreateTables().
			WithDataSource("ds-001", "tushare").
			WithTargetDB("/tmp/stock.db").
			WithMaxTables(100)

		if builder == nil {
			t.Error("Builder with all options should not be nil")
		}
	})
}

// TestCreateTable_Integration 测试建表操作
func TestCreateTable_Integration(t *testing.T) {
	// 创建临时数据库
	tmpfile, err := os.CreateTemp("", "create_tables_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	db, err := sql.Open("sqlite3", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("Create table with schema", func(t *testing.T) {
		createTableSQL := `
			CREATE TABLE IF NOT EXISTS "daily" (
				ts_code TEXT,
				trade_date TEXT,
				open REAL,
				high REAL,
				low REAL,
				close REAL,
				vol REAL,
				amount REAL,
				sync_batch_id TEXT,
				PRIMARY KEY (ts_code, trade_date)
			)
		`
		_, err := db.Exec(createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// 验证表存在
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='daily'`).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to check table existence: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected table 'daily' to exist, got count %d", count)
		}
	})

	t.Run("Drop table", func(t *testing.T) {
		_, err := db.Exec(`DROP TABLE IF EXISTS "daily"`)
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		// 验证表不存在
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='daily'`).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to check table existence: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected table 'daily' to be dropped, got count %d", count)
		}
	})
}
