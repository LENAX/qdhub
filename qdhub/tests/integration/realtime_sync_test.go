// +build integration

package integration

import (
	"database/sql"
	"os"
	"testing"

	"qdhub/internal/infrastructure/taskengine/workflows"
)

// ==================== Realtime Data Sync Workflow 集成测试 ====================

// TestRealtimeDataSyncWorkflow_Build 测试工作流构建
func TestRealtimeDataSyncWorkflow_Build(t *testing.T) {
	t.Run("Valid params should build successfully", func(t *testing.T) {
		builder := workflows.NewRealtimeDataSyncWorkflowBuilder(nil).
			WithDataSource("tushare", "test-token").
			WithTargetDB("/tmp/test.db").
			WithCheckpointTable("sync_checkpoint").
			WithAPIs("daily", "adj_factor").
			WithMaxStocks(10)

		// Build should fail with nil registry for tasks requiring job functions
		// but the builder itself should work
		if builder == nil {
			t.Error("Builder should not be nil")
		}
	})

	t.Run("With CronExpr", func(t *testing.T) {
		builder := workflows.NewRealtimeDataSyncWorkflowBuilder(nil).
			WithDataSource("tushare", "test-token").
			WithTargetDB("/tmp/test.db").
			WithCheckpointTable("sync_checkpoint").
			WithAPIs("daily").
			WithCronExpr("0 0 18 * * 1-5") // 每个工作日18:00

		if builder == nil {
			t.Error("Builder with CronExpr should not be nil")
		}
	})
}

// TestRealtimeDataSyncParams_Validate 测试参数验证
func TestRealtimeDataSyncParams_Validate(t *testing.T) {
	tests := []struct {
		name    string
		params  workflows.RealtimeDataSyncParams
		wantErr bool
		errType error
	}{
		{
			name: "Valid params",
			params: workflows.RealtimeDataSyncParams{
				DataSourceName:  "tushare",
				Token:           "test-token",
				TargetDBPath:    "/tmp/stock.db",
				CheckpointTable: "sync_checkpoint",
				APINames:        []string{"daily"},
			},
			wantErr: false,
		},
		{
			name: "Missing DataSourceName",
			params: workflows.RealtimeDataSyncParams{
				Token:           "test-token",
				TargetDBPath:    "/tmp/stock.db",
				CheckpointTable: "sync_checkpoint",
				APINames:        []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyDataSourceName,
		},
		{
			name: "Missing Token",
			params: workflows.RealtimeDataSyncParams{
				DataSourceName:  "tushare",
				TargetDBPath:    "/tmp/stock.db",
				CheckpointTable: "sync_checkpoint",
				APINames:        []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyToken,
		},
		{
			name: "Missing TargetDBPath",
			params: workflows.RealtimeDataSyncParams{
				DataSourceName:  "tushare",
				Token:           "test-token",
				CheckpointTable: "sync_checkpoint",
				APINames:        []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyTargetDBPath,
		},
		{
			name: "Missing CheckpointTable",
			params: workflows.RealtimeDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				APINames:       []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyCheckpointTable,
		},
		{
			name: "Empty APINames",
			params: workflows.RealtimeDataSyncParams{
				DataSourceName:  "tushare",
				Token:           "test-token",
				TargetDBPath:    "/tmp/stock.db",
				CheckpointTable: "sync_checkpoint",
				APINames:        []string{},
			},
			wantErr: true,
			errType: workflows.ErrEmptyAPINames,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != tt.errType {
				t.Errorf("Validate() error = %v, want %v", err, tt.errType)
			}
		})
	}
}

// TestCheckpointTable_Integration 测试检查点表操作
func TestCheckpointTable_Integration(t *testing.T) {
	// 创建临时数据库
	tmpfile, err := os.CreateTemp("", "checkpoint_test_*.db")
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

	checkpointTable := "sync_checkpoint"

	t.Run("Create checkpoint table", func(t *testing.T) {
		createTableSQL := `
			CREATE TABLE IF NOT EXISTS "` + checkpointTable + `" (
				api_name TEXT PRIMARY KEY,
				last_sync_date TEXT NOT NULL,
				last_sync_time DATETIME DEFAULT CURRENT_TIMESTAMP,
				record_count INTEGER DEFAULT 0
			)
		`
		_, err := db.Exec(createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create checkpoint table: %v", err)
		}
	})

	t.Run("Insert checkpoint", func(t *testing.T) {
		insertSQL := `INSERT INTO "` + checkpointTable + `" (api_name, last_sync_date) VALUES (?, ?)`
		_, err := db.Exec(insertSQL, "daily", "20251215")
		if err != nil {
			t.Fatalf("Failed to insert checkpoint: %v", err)
		}
	})

	t.Run("Query checkpoint", func(t *testing.T) {
		var lastSyncDate string
		query := `SELECT last_sync_date FROM "` + checkpointTable + `" WHERE api_name = ?`
		err := db.QueryRow(query, "daily").Scan(&lastSyncDate)
		if err != nil {
			t.Fatalf("Failed to query checkpoint: %v", err)
		}
		if lastSyncDate != "20251215" {
			t.Errorf("Expected last_sync_date '20251215', got '%s'", lastSyncDate)
		}
	})

	t.Run("Update checkpoint", func(t *testing.T) {
		upsertSQL := `
			INSERT INTO "` + checkpointTable + `" (api_name, last_sync_date, last_sync_time, record_count)
			VALUES (?, ?, CURRENT_TIMESTAMP, 0)
			ON CONFLICT(api_name) DO UPDATE SET
				last_sync_date = excluded.last_sync_date,
				last_sync_time = CURRENT_TIMESTAMP
		`
		_, err := db.Exec(upsertSQL, "daily", "20251216")
		if err != nil {
			t.Fatalf("Failed to upsert checkpoint: %v", err)
		}

		// Verify update
		var lastSyncDate string
		query := `SELECT last_sync_date FROM "` + checkpointTable + `" WHERE api_name = ?`
		err = db.QueryRow(query, "daily").Scan(&lastSyncDate)
		if err != nil {
			t.Fatalf("Failed to query updated checkpoint: %v", err)
		}
		if lastSyncDate != "20251216" {
			t.Errorf("Expected updated last_sync_date '20251216', got '%s'", lastSyncDate)
		}
	})

	t.Run("Insert new API checkpoint", func(t *testing.T) {
		upsertSQL := `
			INSERT INTO "` + checkpointTable + `" (api_name, last_sync_date, last_sync_time, record_count)
			VALUES (?, ?, CURRENT_TIMESTAMP, 0)
			ON CONFLICT(api_name) DO UPDATE SET
				last_sync_date = excluded.last_sync_date,
				last_sync_time = CURRENT_TIMESTAMP
		`
		_, err := db.Exec(upsertSQL, "adj_factor", "20251216")
		if err != nil {
			t.Fatalf("Failed to insert new API checkpoint: %v", err)
		}

		// Verify insert
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM "` + checkpointTable + `"`).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count checkpoints: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 checkpoints, got %d", count)
		}
	})

	t.Run("Query non-existent checkpoint", func(t *testing.T) {
		var lastSyncDate string
		query := `SELECT last_sync_date FROM "` + checkpointTable + `" WHERE api_name = ?`
		err := db.QueryRow(query, "non_existent_api").Scan(&lastSyncDate)
		if err != sql.ErrNoRows {
			t.Errorf("Expected ErrNoRows for non-existent API, got %v", err)
		}
	})
}

// TestWorkflowFactory_RealtimeDataSync 测试工作流工厂
func TestWorkflowFactory_RealtimeDataSync(t *testing.T) {
	factory := workflows.NewWorkflowFactory(nil)

	t.Run("RealtimeDataSync builder method", func(t *testing.T) {
		builder := factory.RealtimeDataSync()
		if builder == nil {
			t.Error("RealtimeDataSync() should return non-nil builder")
		}
	})

	t.Run("CreateRealtimeDataSyncWorkflow with invalid params", func(t *testing.T) {
		params := workflows.RealtimeDataSyncParams{
			// Missing required fields
		}

		_, err := factory.CreateRealtimeDataSyncWorkflow(params)
		if err == nil {
			t.Error("CreateRealtimeDataSyncWorkflow should fail with invalid params")
		}
	})
}
