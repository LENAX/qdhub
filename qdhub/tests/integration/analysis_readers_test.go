//go:build integration

package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/domain/analysis"
	"qdhub/internal/domain/datastore"
	analysisinfra "qdhub/internal/infrastructure/analysis"
	"qdhub/internal/infrastructure/quantdb/duckdb"
)

// TestAnalysisReaders_GetDailyWithAdjFactor 使用 DuckDB + 模拟数据测试 Readers.GetDailyWithAdjFactor
func TestAnalysisReaders_GetDailyWithAdjFactor(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "analysis_int.db")

	adapter := duckdb.NewAdapter(dbPath)
	require.NoError(t, adapter.Connect(ctx))
	defer adapter.Close()

	// 建表 daily（与 readers_impl 中 SQL 字段一致）
	dailySchema := &datastore.TableSchema{
		TableName: "daily",
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
			{Name: "open", TargetType: "DOUBLE", Nullable: true},
			{Name: "high", TargetType: "DOUBLE", Nullable: true},
			{Name: "low", TargetType: "DOUBLE", Nullable: true},
			{Name: "close", TargetType: "DOUBLE", Nullable: true},
			{Name: "vol", TargetType: "DOUBLE", Nullable: true},
			{Name: "amount", TargetType: "DOUBLE", Nullable: true},
			{Name: "pre_close", TargetType: "DOUBLE", Nullable: true},
			{Name: "change", TargetType: "DOUBLE", Nullable: true},
			{Name: "pct_chg", TargetType: "DOUBLE", Nullable: true},
		},
		PrimaryKeys: []string{"ts_code", "trade_date"},
	}
	require.NoError(t, adapter.CreateTable(ctx, dailySchema))

	// 建表 adj_factor
	adjSchema := &datastore.TableSchema{
		TableName: "adj_factor",
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
			{Name: "adj_factor", TargetType: "DOUBLE", Nullable: true},
		},
		PrimaryKeys: []string{"ts_code", "trade_date"},
	}
	require.NoError(t, adapter.CreateTable(ctx, adjSchema))

	// 插入 fixture：000001.SZ 两日
	dailyData := []map[string]any{
		{"ts_code": "000001.SZ", "trade_date": "20240102", "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5, "vol": 1e6, "amount": 1e7, "pre_close": 10.0, "change": 0.5, "pct_chg": 5.0},
		{"ts_code": "000001.SZ", "trade_date": "20240103", "open": 11.0, "high": 12.0, "low": 10.5, "close": 11.5, "vol": 1.2e6, "amount": 1.2e7, "pre_close": 10.5, "change": 1.0, "pct_chg": 9.52},
	}
	_, err := adapter.BulkInsert(ctx, "daily", dailyData)
	require.NoError(t, err)

	adjData := []map[string]any{
		{"ts_code": "000001.SZ", "trade_date": "20240102", "adj_factor": 1.0},
		{"ts_code": "000001.SZ", "trade_date": "20240103", "adj_factor": 2.0},
	}
	_, err = adapter.BulkInsert(ctx, "adj_factor", adjData)
	require.NoError(t, err)

	// GetDailyWithAdjFactor 的 SQL 使用 LEFT JOIN stock_basic，需存在该表
	stockBasicSchema := &datastore.TableSchema{
		TableName: "stock_basic",
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
		},
		PrimaryKeys: []string{"ts_code"},
	}
	require.NoError(t, adapter.CreateTable(ctx, stockBasicSchema))
	_, err = adapter.BulkInsert(ctx, "stock_basic", []map[string]any{
		{"ts_code": "000001.SZ", "name": "平安银行"},
	})
	require.NoError(t, err)

	readers := analysisinfra.NewReaders(adapter)

	t.Run("has_data", func(t *testing.T) {
		rows, err := readers.GetDailyWithAdjFactor(ctx, "000001.SZ", "20240101", "20240131")
		require.NoError(t, err)
		require.Len(t, rows, 2)
		assert.Equal(t, "20240102", rows[0].TradeDate)
		assert.Equal(t, 10.5, rows[0].Close)
		assert.Equal(t, 1.0, rows[0].AdjFactor)
		assert.Equal(t, "20240103", rows[1].TradeDate)
		assert.Equal(t, 11.5, rows[1].Close)
		assert.Equal(t, 2.0, rows[1].AdjFactor)
	})

	t.Run("empty_range", func(t *testing.T) {
		rows, err := readers.GetDailyWithAdjFactor(ctx, "000001.SZ", "20230101", "20231231")
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("no_code", func(t *testing.T) {
		rows, err := readers.GetDailyWithAdjFactor(ctx, "999999.SZ", "20240101", "20240131")
		require.NoError(t, err)
		assert.Empty(t, rows)
	})
}

// mockFallbackForIntegration 集成测试用：本地无数据时返回固定行
type mockFallbackForIntegration struct {
	rows []analysis.RawDailyRow
}

func (m *mockFallbackForIntegration) FetchDaily(ctx context.Context, tsCode, startDate, endDate string) ([]analysis.RawDailyRow, error) {
	return m.rows, nil
}

// TestAnalysisReaders_GetDailyWithAdjFactor_Fallback 测试无数据时走 fallback
func TestAnalysisReaders_GetDailyWithAdjFactor_Fallback(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "analysis_fallback.db")

	adapter := duckdb.NewAdapter(dbPath)
	require.NoError(t, adapter.Connect(ctx))
	defer adapter.Close()

	dailySchema := &datastore.TableSchema{
		TableName: "daily",
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
			{Name: "open", TargetType: "DOUBLE", Nullable: true},
			{Name: "high", TargetType: "DOUBLE", Nullable: true},
			{Name: "low", TargetType: "DOUBLE", Nullable: true},
			{Name: "close", TargetType: "DOUBLE", Nullable: true},
			{Name: "vol", TargetType: "DOUBLE", Nullable: true},
			{Name: "amount", TargetType: "DOUBLE", Nullable: true},
			{Name: "pre_close", TargetType: "DOUBLE", Nullable: true},
			{Name: "change", TargetType: "DOUBLE", Nullable: true},
			{Name: "pct_chg", TargetType: "DOUBLE", Nullable: true},
		},
		PrimaryKeys: []string{"ts_code", "trade_date"},
	}
	require.NoError(t, adapter.CreateTable(ctx, dailySchema))
	adjSchema := &datastore.TableSchema{
		TableName: "adj_factor",
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
			{Name: "adj_factor", TargetType: "DOUBLE", Nullable: true},
		},
		PrimaryKeys: []string{"ts_code", "trade_date"},
	}
	require.NoError(t, adapter.CreateTable(ctx, adjSchema))
	stockBasicSchema := &datastore.TableSchema{
		TableName: "stock_basic",
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
		},
		PrimaryKeys: []string{"ts_code"},
	}
	require.NoError(t, adapter.CreateTable(ctx, stockBasicSchema))

	fallbackRows := []analysis.RawDailyRow{
		{TradeDate: "20240115", Open: 12, High: 13, Low: 11.5, Close: 12.5, Vol: 2e6, Amount: 2e7, PreClose: 12, Change: 0.5, PctChg: 4.17, AdjFactor: 1.0},
	}
	fallback := &mockFallbackForIntegration{rows: fallbackRows}
	readers := analysisinfra.NewReadersWithFallback(adapter, fallback)

	// 本地无 000002.SZ 数据，应走 fallback
	rows, err := readers.GetDailyWithAdjFactor(ctx, "000002.SZ", "20240101", "20240131")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "20240115", rows[0].TradeDate)
	assert.Equal(t, 12.5, rows[0].Close)
}

// TestAnalysisReaders_ListStocks_Cnspell 测试拼音缩写搜索：仅按 cnspell 匹配
func TestAnalysisReaders_ListStocks_Cnspell(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "list_stocks_cnspell.db")

	adapter := duckdb.NewAdapter(dbPath)
	require.NoError(t, adapter.Connect(ctx))
	defer adapter.Close()

	// stock_basic 含 cnspell（与 readers_impl ListStocks 查询字段一致）
	schema := &datastore.TableSchema{
		TableName: "stock_basic",
		Columns: []datastore.ColumnDef{
			{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
			{Name: "symbol", TargetType: "VARCHAR", Nullable: true},
			{Name: "name", TargetType: "VARCHAR", Nullable: true},
			{Name: "area", TargetType: "VARCHAR", Nullable: true},
			{Name: "industry", TargetType: "VARCHAR", Nullable: true},
			{Name: "market", TargetType: "VARCHAR", Nullable: true},
			{Name: "list_date", TargetType: "VARCHAR", Nullable: true},
			{Name: "is_hs", TargetType: "VARCHAR", Nullable: true},
			{Name: "cnspell", TargetType: "VARCHAR", Nullable: true},
		},
		PrimaryKeys: []string{"ts_code"},
	}
	require.NoError(t, adapter.CreateTable(ctx, schema))

	// 平安银行 cnspell 常用缩写 PA、PINGAN 等
	data := []map[string]any{
		{"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行", "area": "深圳", "industry": "银行", "market": "主板", "list_date": "19910403", "is_hs": "S", "cnspell": "PAYH"},
		{"ts_code": "600519.SH", "symbol": "600519", "name": "贵州茅台", "area": "贵州", "industry": "白酒", "market": "主板", "list_date": "20010827", "is_hs": "N", "cnspell": "GZMT"},
		{"ts_code": "000002.SZ", "symbol": "000002", "name": "万科A", "area": "深圳", "industry": "地产", "market": "主板", "list_date": "19910129", "is_hs": "N", "cnspell": "WKA"},
	}
	_, err := adapter.BulkInsert(ctx, "stock_basic", data)
	require.NoError(t, err)

	readers := analysisinfra.NewReaders(adapter)

	t.Run("search_type_cnspell_match", func(t *testing.T) {
		st := "cnspell"
		req := analysis.StockListRequest{Query: strPtr("PA"), SearchType: &st, Limit: 10, Offset: 0}
		rows, err := readers.ListStocks(ctx, req)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "000001.SZ", rows[0].TsCode)
		assert.Equal(t, "平安银行", rows[0].Name)
	})

	t.Run("search_type_cnspell_partial", func(t *testing.T) {
		st := "cnspell"
		req := analysis.StockListRequest{Query: strPtr("GZ"), SearchType: &st, Limit: 10, Offset: 0}
		rows, err := readers.ListStocks(ctx, req)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "600519.SH", rows[0].TsCode)
	})

	t.Run("default_search_includes_name", func(t *testing.T) {
		req := analysis.StockListRequest{Query: strPtr("平安"), Limit: 10, Offset: 0}
		rows, err := readers.ListStocks(ctx, req)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "000001.SZ", rows[0].TsCode)
	})
}

func strPtr(s string) *string { return &s }
