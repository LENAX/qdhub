//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"os"
	"testing"

	"qdhub/internal/infrastructure/taskengine/workflows"
)

// ==================== Batch Data Sync Workflow 集成测试 ====================

// TestBatchDataSyncWorkflow_Build 测试工作流构建
func TestBatchDataSyncWorkflow_Build(t *testing.T) {
	t.Run("Valid params should create builder", func(t *testing.T) {
		builder := workflows.NewBatchDataSyncWorkflowBuilder(nil).
			WithDataSource("tushare", "test-token").
			WithTargetDB("/tmp/stock.db").
			WithDateRange("20251201", "20251231").
			WithAPIs("daily", "adj_factor").
			WithMaxStocks(10)

		if builder == nil {
			t.Error("Builder should not be nil")
		}
	})

	t.Run("Builder with time range", func(t *testing.T) {
		builder := workflows.NewBatchDataSyncWorkflowBuilder(nil).
			WithDataSource("tushare", "test-token").
			WithTargetDB("/tmp/stock.db").
			WithDateRange("20251201", "20251231").
			WithTimeRange("09:30:00", "15:00:00").
			WithAPIs("daily")

		if builder == nil {
			t.Error("Builder with time range should not be nil")
		}
	})

	t.Run("Builder with datetime range", func(t *testing.T) {
		builder := workflows.NewBatchDataSyncWorkflowBuilder(nil).
			WithDataSource("tushare", "test-token").
			WithTargetDB("/tmp/stock.db").
			WithDateTimeRange("20251201", "09:30:00", "20251231", "15:00:00").
			WithAPIs("daily")

		if builder == nil {
			t.Error("Builder with datetime range should not be nil")
		}
	})
}

// TestBatchDataSyncParams_Validate 测试参数验证
func TestBatchDataSyncParams_Validate(t *testing.T) {
	tests := []struct {
		name    string
		params  workflows.BatchDataSyncParams
		wantErr bool
		errType error
	}{
		{
			name: "Valid params",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "20251201",
				EndDate:        "20251231",
				APINames:       []string{"daily"},
			},
			wantErr: false,
		},
		{
			name: "Missing DataSourceName",
			params: workflows.BatchDataSyncParams{
				Token:        "test-token",
				TargetDBPath: "/tmp/stock.db",
				StartDate:    "20251201",
				EndDate:      "20251231",
				APINames:     []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyDataSourceName,
		},
		{
			name: "Missing Token",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "20251201",
				EndDate:        "20251231",
				APINames:       []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyToken,
		},
		{
			name: "Missing TargetDBPath",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				StartDate:      "20251201",
				EndDate:        "20251231",
				APINames:       []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyTargetDBPath,
		},
		{
			name: "Missing StartDate",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				EndDate:        "20251231",
				APINames:       []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyStartDate,
		},
		{
			name: "Missing EndDate",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "20251201",
				APINames:       []string{"daily"},
			},
			wantErr: true,
			errType: workflows.ErrEmptyEndDate,
		},
		{
			name: "Empty APINames",
			params: workflows.BatchDataSyncParams{
				DataSourceName: "tushare",
				Token:          "test-token",
				TargetDBPath:   "/tmp/stock.db",
				StartDate:      "20251201",
				EndDate:        "20251231",
				APINames:       []string{},
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

// TestBatchDataSyncParams_DateTime 测试日期时间格式化
func TestBatchDataSyncParams_DateTime(t *testing.T) {
	t.Run("Date only", func(t *testing.T) {
		params := workflows.BatchDataSyncParams{
			StartDate: "20251201",
			EndDate:   "20251231",
		}

		if got := params.GetStartDateTime(); got != "20251201" {
			t.Errorf("GetStartDateTime() = %s, want 20251201", got)
		}
		if got := params.GetEndDateTime(); got != "20251231" {
			t.Errorf("GetEndDateTime() = %s, want 20251231", got)
		}
	})

	t.Run("Date with time", func(t *testing.T) {
		params := workflows.BatchDataSyncParams{
			StartDate: "20251201",
			StartTime: "09:30:00",
			EndDate:   "20251231",
			EndTime:   "15:00:00",
		}

		if got := params.GetStartDateTime(); got != "20251201 09:30:00" {
			t.Errorf("GetStartDateTime() = %s, want '20251201 09:30:00'", got)
		}
		if got := params.GetEndDateTime(); got != "20251231 15:00:00" {
			t.Errorf("GetEndDateTime() = %s, want '20251231 15:00:00'", got)
		}
	})
}

// TestWorkflowFactory_BatchDataSync 测试工作流工厂
func TestWorkflowFactory_BatchDataSync(t *testing.T) {
	factory := workflows.NewWorkflowFactory(nil)

	t.Run("BatchDataSync builder method", func(t *testing.T) {
		builder := factory.BatchDataSync()
		if builder == nil {
			t.Error("BatchDataSync() should return non-nil builder")
		}
	})

	t.Run("CreateBatchDataSyncWorkflow with invalid params", func(t *testing.T) {
		params := workflows.BatchDataSyncParams{
			// Missing required fields
		}

		_, err := factory.CreateBatchDataSyncWorkflow(params)
		if err == nil {
			t.Error("CreateBatchDataSyncWorkflow should fail with invalid params")
		}
	})
}

// TestSyncBatchID_Integration 测试同步批次 ID 功能
func TestSyncBatchID_Integration(t *testing.T) {
	// 创建临时数据库
	tmpfile, err := os.CreateTemp("", "batch_sync_test_*.db")
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

	// 创建测试表
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS "daily" (
			ts_code TEXT,
			trade_date TEXT,
			close REAL,
			sync_batch_id TEXT,
			PRIMARY KEY (ts_code, trade_date)
		)
	`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("Insert with sync_batch_id", func(t *testing.T) {
		batchID := "workflow-instance-001"

		insertSQL := `INSERT INTO "daily" (ts_code, trade_date, close, sync_batch_id) VALUES (?, ?, ?, ?)`
		_, err := db.Exec(insertSQL, "000001.SZ", "20251215", 10.50, batchID)
		if err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}

		_, err = db.Exec(insertSQL, "000002.SZ", "20251215", 20.30, batchID)
		if err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}

		// 验证插入
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM "daily" WHERE sync_batch_id = ?`, batchID).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count records: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 records with batch_id, got %d", count)
		}
	})

	t.Run("Delete by sync_batch_id (rollback)", func(t *testing.T) {
		batchID := "workflow-instance-001"

		deleteSQL := `DELETE FROM "daily" WHERE sync_batch_id = ?`
		result, err := db.Exec(deleteSQL, batchID)
		if err != nil {
			t.Fatalf("Failed to delete records: %v", err)
		}

		affected, _ := result.RowsAffected()
		if affected != 2 {
			t.Errorf("Expected 2 rows affected, got %d", affected)
		}

		// 验证删除
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM "daily"`).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count records: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 records after rollback, got %d", count)
		}
	})
}
