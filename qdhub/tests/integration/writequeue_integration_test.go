//go:build integration
// +build integration

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/infrastructure/quantdb/duckdb"
	"qdhub/internal/infrastructure/quantdb/writequeue"
	"qdhub/pkg/config"
	"qdhub/pkg/typemap"
)

// dailySchema creates the same schema as quantdb_test (daily + sync_batch_id).
func dailySchema() *datastore.TableSchema {
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
	typeMapper := typemap.NewDefaultTypeMapper()
	columns := typeMapper.MapAllFields(fields, "tushare", datastore.DataStoreTypeDuckDB)
	schema := &datastore.TableSchema{
		TableName:   "daily",
		Columns:     columns,
		PrimaryKeys: []string{"ts_code", "trade_date"},
	}
	schema.Columns = append(schema.Columns, datastore.ColumnDef{
		Name:       "sync_batch_id",
		SourceType: "str",
		TargetType: "VARCHAR",
		Nullable:   true,
	})
	return schema
}

// adjSchema creates a second table for multi-table tests.
func adjSchema() *datastore.TableSchema {
	return &datastore.TableSchema{
		TableName:   "adj_factor",
		PrimaryKeys: []string{"ts_code", "trade_date"},
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
			{Name: "adj_factor", TargetType: "DOUBLE", Nullable: true},
			{Name: "sync_batch_id", TargetType: "VARCHAR", Nullable: true},
		},
	}
}

func absPath(t *testing.T, p string) string {
	t.Helper()
	abs, err := filepath.Abs(p)
	require.NoError(t, err)
	return abs
}

// setupQueueWithTable creates a temp DB path, factory, creates "daily" table, and returns queue + path.
// Caller must call queue.Close() when done.
func setupQueueWithTable(t *testing.T, ctx context.Context, wqCfg config.WriteQueueConfig) (datastore.QuantDBWriteQueue, string, datastore.QuantDBFactory) {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "wq.db")
	path := absPath(t, dbPath)

	factory := duckdb.NewFactory()
	db, err := factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: path,
	})
	require.NoError(t, err)
	require.NoError(t, db.Connect(ctx))
	require.NoError(t, db.CreateTable(ctx, dailySchema()))
	db.Close()

	q := writequeue.NewQueue(wqCfg, factory)
	return q, path, factory
}

func getTableRowCount(t *testing.T, ctx context.Context, factory datastore.QuantDBFactory, path, table string) int64 {
	t.Helper()
	db, err := factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: path,
	})
	require.NoError(t, err)
	require.NoError(t, db.Connect(ctx))
	defer db.Close()
	stats, err := db.GetTableStats(ctx, table)
	require.NoError(t, err)
	return stats.RowCount
}

func queryDistinctSyncBatchIDs(t *testing.T, ctx context.Context, factory datastore.QuantDBFactory, path, table string) []string {
	t.Helper()
	db, err := factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: path,
	})
	require.NoError(t, err)
	require.NoError(t, db.Connect(ctx))
	defer db.Close()
	rows, err := db.Query(ctx, "SELECT DISTINCT sync_batch_id FROM "+table+" ORDER BY sync_batch_id")
	require.NoError(t, err)
	var ids []string
	for _, r := range rows {
		if v, ok := r["sync_batch_id"]; ok && v != nil {
			ids = append(ids, v.(string))
		}
	}
	return ids
}

// TestWriteQueue_EnqueueAndWait_RowCountAndSyncBatchID writes multiple batches and verifies row count and sync_batch_id.
func TestWriteQueue_EnqueueAndWait_RowCountAndSyncBatchID(t *testing.T) {
	ctx := context.Background()
	cfg := config.WriteQueueConfig{
		Enabled:            true,
		BatchSize:          10,
		MaxWaitSec:         1,
		MemoryCheckEnabled: false,
		MemoryHighMB:       0,
		MemoryCriticalMB:   0,
	}
	q, path, factory := setupQueueWithTable(t, ctx, cfg)
	defer q.Close()

	// Batch 1
	n1, err := q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "daily",
		SyncBatchID: "batch-1",
		Data: []map[string]any{
			{"ts_code": "000001.SZ", "trade_date": "2024-01-01", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 1e6, "amount": 1e7, "pct_chg": 1.0},
			{"ts_code": "000001.SZ", "trade_date": "2024-01-02", "open": 10.5, "high": 11.0, "low": 10.0, "close": 10.8, "vol": 1.2e6, "amount": 1.2e7, "pct_chg": 2.0},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), n1)

	// Batch 2
	n2, err := q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "daily",
		SyncBatchID: "batch-2",
		Data: []map[string]any{
			{"ts_code": "000002.SZ", "trade_date": "2024-01-01", "open": 20.0, "high": 21.0, "low": 19.5, "close": 20.5, "vol": 2e6, "amount": 2e7, "pct_chg": 0.5},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), n2)

	count := getTableRowCount(t, ctx, factory, path, "daily")
	require.Equal(t, int64(3), count)

	ids := queryDistinctSyncBatchIDs(t, ctx, factory, path, "daily")
	require.Len(t, ids, 2)
	require.Contains(t, ids, "batch-1")
	require.Contains(t, ids, "batch-2")
}

// TestWriteQueue_EnqueueThenClose_DataVisible verifies async Enqueue then Close flushes and data is visible.
func TestWriteQueue_EnqueueThenClose_DataVisible(t *testing.T) {
	ctx := context.Background()
	cfg := config.WriteQueueConfig{
		Enabled:            true,
		BatchSize:          100,
		MaxWaitSec:         2,
		MemoryCheckEnabled: false,
		MemoryHighMB:       0,
		MemoryCriticalMB:   0,
	}
	q, path, _ := setupQueueWithTable(t, ctx, cfg)

	err := q.Enqueue(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "daily",
		SyncBatchID: "async-batch",
		Data: []map[string]any{
			{"ts_code": "000001.SZ", "trade_date": "2024-01-01", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 1e6, "amount": 1e7, "pct_chg": 1.0},
		},
	})
	require.NoError(t, err)
	// Let pathWriter loop receive the request and add to buffer before we Close
	time.Sleep(100 * time.Millisecond)

	require.NoError(t, q.Close())
	// Queue.Close() only signals writers; the pathWriter loop flushes in its own goroutine. Wait for flush.
	time.Sleep(2 * time.Second)

	// Use a new factory to read so we get a fresh connection and see committed data
	readFactory := duckdb.NewFactory()
	defer readFactory.Close()
	count := getTableRowCount(t, ctx, readFactory, path, "daily")
	require.Equal(t, int64(1), count)
}

// TestWriteQueue_MultiTableMultiBatch verifies (tableName, syncBatchID) aggregation.
func TestWriteQueue_MultiTableMultiBatch(t *testing.T) {
	ctx := context.Background()
	cfg := config.WriteQueueConfig{
		Enabled:            true,
		BatchSize:          10,
		MaxWaitSec:         1,
		MemoryCheckEnabled: false,
		MemoryHighMB:       0,
		MemoryCriticalMB:   0,
	}
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "multi.db")
	path := absPath(t, dbPath)

	factory := duckdb.NewFactory()
	db, err := factory.Create(datastore.QuantDBConfig{
		Type:        datastore.DataStoreTypeDuckDB,
		StoragePath: path,
	})
	require.NoError(t, err)
	require.NoError(t, db.Connect(ctx))
	require.NoError(t, db.CreateTable(ctx, dailySchema()))
	require.NoError(t, db.CreateTable(ctx, adjSchema()))
	db.Close()

	q := writequeue.NewQueue(cfg, factory)
	defer q.Close()

	// daily, batch A (2 rows)
	_, err = q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "daily",
		SyncBatchID: "daily-a",
		Data: []map[string]any{
			{"ts_code": "000001.SZ", "trade_date": "2024-01-01", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 1e6, "amount": 1e7, "pct_chg": 1.0},
			{"ts_code": "000001.SZ", "trade_date": "2024-01-02", "open": 10.5, "high": 11.0, "low": 10.0, "close": 10.8, "vol": 1.2e6, "amount": 1.2e7, "pct_chg": 2.0},
		},
	})
	require.NoError(t, err)

	// daily, batch B (1 row)
	_, err = q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "daily",
		SyncBatchID: "daily-b",
		Data: []map[string]any{
			{"ts_code": "000002.SZ", "trade_date": "2024-01-01", "open": 20.0, "high": 21.0, "low": 19.5, "close": 20.5, "vol": 2e6, "amount": 2e7, "pct_chg": 0.5},
		},
	})
	require.NoError(t, err)

	// adj_factor, batch A (1 row)
	_, err = q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "adj_factor",
		SyncBatchID: "adj-a",
		Data: []map[string]any{
			{"ts_code": "000001.SZ", "trade_date": "2024-01-01", "adj_factor": 1.0},
		},
	})
	require.NoError(t, err)

	// adj_factor, batch B (2 rows)
	_, err = q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "adj_factor",
		SyncBatchID: "adj-b",
		Data: []map[string]any{
			{"ts_code": "000001.SZ", "trade_date": "2024-01-02", "adj_factor": 1.0},
			{"ts_code": "000002.SZ", "trade_date": "2024-01-01", "adj_factor": 1.0},
		},
	})
	require.NoError(t, err)

	require.Equal(t, int64(3), getTableRowCount(t, ctx, factory, path, "daily"))
	require.Equal(t, int64(3), getTableRowCount(t, ctx, factory, path, "adj_factor"))

	dailyIDs := queryDistinctSyncBatchIDs(t, ctx, factory, path, "daily")
	require.Len(t, dailyIDs, 2)
	require.Contains(t, dailyIDs, "daily-a")
	require.Contains(t, dailyIDs, "daily-b")

	adjIDs := queryDistinctSyncBatchIDs(t, ctx, factory, path, "adj_factor")
	require.Len(t, adjIDs, 2)
	require.Contains(t, adjIDs, "adj-a")
	require.Contains(t, adjIDs, "adj-b")
}

// TestWriteQueue_BatchSizeTriggersFlush uses small BatchSize and verifies final row count.
func TestWriteQueue_BatchSizeTriggersFlush(t *testing.T) {
	ctx := context.Background()
	cfg := config.WriteQueueConfig{
		Enabled:            true,
		BatchSize:          2,
		MaxWaitSec:         2,
		MemoryCheckEnabled: false,
		MemoryHighMB:       0,
		MemoryCriticalMB:   0,
	}
	q, path, factory := setupQueueWithTable(t, ctx, cfg)
	defer q.Close()

	// Use distinct rows (different trade_date) so we get 3 rows; same key would overwrite
	row1 := map[string]any{"ts_code": "000001.SZ", "trade_date": "2024-01-01", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 1e6, "amount": 1e7, "pct_chg": 1.0}
	row2 := map[string]any{"ts_code": "000001.SZ", "trade_date": "2024-01-02", "open": 10.5, "high": 11.0, "low": 10.0, "close": 10.8, "vol": 1.2e6, "amount": 1.2e7, "pct_chg": 2.0}
	row3 := map[string]any{"ts_code": "000001.SZ", "trade_date": "2024-01-03", "open": 10.8, "high": 11.2, "low": 10.5, "close": 11.0, "vol": 1.1e6, "amount": 1.2e7, "pct_chg": 1.5}

	// 3 batches of 1 row each; with BatchSize=2 we may flush after 2 then 1
	_, err := q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{Path: path, TableName: "daily", SyncBatchID: "b1", Data: []map[string]any{row1}})
	require.NoError(t, err)
	_, err = q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{Path: path, TableName: "daily", SyncBatchID: "b2", Data: []map[string]any{row2}})
	require.NoError(t, err)
	_, err = q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{Path: path, TableName: "daily", SyncBatchID: "b3", Data: []map[string]any{row3}})
	require.NoError(t, err)

	require.Equal(t, int64(3), getTableRowCount(t, ctx, factory, path, "daily"))
}

// TestWriteQueue_MaxWaitTriggersFlush uses large BatchSize and short MaxWait; one EnqueueAndWait(1 row) blocks until flush.
func TestWriteQueue_MaxWaitTriggersFlush(t *testing.T) {
	ctx := context.Background()
	cfg := config.WriteQueueConfig{
		Enabled:            true,
		BatchSize:          1000,
		MaxWaitSec:         1,
		MemoryCheckEnabled: false,
		MemoryHighMB:       0,
		MemoryCriticalMB:   0,
	}
	q, path, factory := setupQueueWithTable(t, ctx, cfg)
	defer q.Close()

	n, err := q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "daily",
		SyncBatchID: "maxwait-batch",
		Data: []map[string]any{
			{"ts_code": "000001.SZ", "trade_date": "2024-01-01", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 1e6, "amount": 1e7, "pct_chg": 1.0},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// EnqueueAndWait returns after flush (triggered by maxWait), so data should be visible
	count := getTableRowCount(t, ctx, factory, path, "daily")
	require.Equal(t, int64(1), count)
}

// TestWriteQueue_EnabledFalse_DirectWrite verifies bypass path when queue is disabled.
func TestWriteQueue_EnabledFalse_DirectWrite(t *testing.T) {
	ctx := context.Background()
	cfg := config.WriteQueueConfig{
		Enabled:            false,
		BatchSize:          5000,
		MaxWaitSec:         30,
		MemoryCheckEnabled: false,
		MemoryHighMB:       0,
		MemoryCriticalMB:   0,
	}
	q, path, factory := setupQueueWithTable(t, ctx, cfg)
	defer q.Close()

	n, err := q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path:        path,
		TableName:   "daily",
		SyncBatchID: "direct-batch",
		Data: []map[string]any{
			{"ts_code": "000001.SZ", "trade_date": "2024-01-01", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 1e6, "amount": 1e7, "pct_chg": 1.0},
			{"ts_code": "000001.SZ", "trade_date": "2024-01-02", "open": 10.5, "high": 11.0, "low": 10.0, "close": 10.8, "vol": 1.2e6, "amount": 1.2e7, "pct_chg": 2.0},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	count := getTableRowCount(t, ctx, factory, path, "daily")
	require.Equal(t, int64(2), count)

	ids := queryDistinctSyncBatchIDs(t, ctx, factory, path, "daily")
	require.Len(t, ids, 1)
	require.Equal(t, "direct-batch", ids[0])
}

// TestWriteQueue_CloseThenEnqueue_Rejects verifies that after Close, Enqueue/EnqueueAndWait return error.
func TestWriteQueue_CloseThenEnqueue_Rejects(t *testing.T) {
	ctx := context.Background()
	cfg := config.WriteQueueConfig{
		Enabled:            true,
		BatchSize:          10,
		MaxWaitSec:         1,
		MemoryCheckEnabled: false,
		MemoryHighMB:       0,
		MemoryCriticalMB:   0,
	}
	q, path, _ := setupQueueWithTable(t, ctx, cfg)
	require.NoError(t, q.Close())

	err := q.Enqueue(ctx, datastore.QuantDBBatchWriteRequest{
		Path: path, TableName: "daily", Data: []map[string]any{},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed")

	_, err = q.EnqueueAndWait(ctx, datastore.QuantDBBatchWriteRequest{
		Path: path, TableName: "daily", Data: []map[string]any{},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed")
}
