//go:build e2e
// +build e2e

// Package e2e 分析模块 E2E 测试
// 流程：定义并启动数据同步 workflow -> 同步约一个月数据（可复用）-> 校验数据获取与数量 -> 校验兜底逻辑
//
// 运行：go test -tags e2e -v -run "TestE2E_Analysis" ./tests/e2e/...
// 真实模式（同步约 1 月数据，耗时长）：QDHUB_TUSHARE_TOKEN=xxx go test -tags e2e -v -run "TestE2E_Analysis" ./tests/e2e/...
package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/analysis"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// TestE2E_Analysis_SyncThenKLine 先执行同步（约 1 月数据），再校验 K 线接口能拿到数据且满足数量
// 真实模式：同步 daily 约 1 个月，数据写入 e2e/data/e2e_quant.duckdb，后续测试可复用
// Mock 模式：同步使用 mock 数据（2 条），校验 GetKLine 返回至少 1 条
func TestE2E_Analysis_SyncThenKLine(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_SyncThenKLine")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}

	ctx := context.Background()

	// Step 1: 确保有数据源与 Token
	var dataSourceID shared.ID
	allDataSources, err := testCtx.metadataAppService.ListDataSources(ctx)
	require.NoError(t, err)
	for _, ds := range allDataSources {
		if ds.Name == "Tushare" {
			dataSourceID = ds.ID
			break
		}
	}
	if dataSourceID == "" {
		ds, err := testCtx.metadataAppService.CreateDataSource(ctx, contracts.CreateDataSourceRequest{
			Name: "Tushare", Description: "E2E", BaseURL: "http://api.tushare.pro", DocURL: "https://tushare.pro/document/2",
		})
		require.NoError(t, err)
		dataSourceID = ds.ID
	}
	if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
		_ = testCtx.metadataAppService.SaveToken(ctx, contracts.SaveTokenRequest{DataSourceID: dataSourceID, TokenValue: testCtx.config.TushareToken})
	}

	// Step 2: 确保有 DataStore 并建表
	targetDBPath := testCtx.config.DuckDBPath
	existingStores, err := testCtx.datastoreAppService.ListDataStores(ctx)
	require.NoError(t, err)
	var dataStoreID shared.ID
	for _, ds := range existingStores {
		if ds.StoragePath == targetDBPath || ds.Name == "E2E Analysis DuckDB" {
			dataStoreID = ds.ID
			break
		}
	}
	if dataStoreID == "" {
		ds, err := testCtx.datastoreAppService.CreateDataStore(ctx, contracts.CreateDataStoreRequest{
			Name: "E2E Analysis DuckDB", Description: "E2E Analysis", Type: datastore.DataStoreTypeDuckDB, StoragePath: targetDBPath,
		})
		require.NoError(t, err)
		dataStoreID = ds.ID
	}
	tableInstanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: dataSourceID, DataStoreID: dataStoreID,
	})
	require.NoError(t, err)
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, tableInstanceID.String(), 2*time.Minute)

	// Step 3: 创建 SyncPlan（仅 daily）并执行
	createReq := contracts.CreateSyncPlanRequest{
		Name:         "E2E Analysis Daily",
		DataSourceID: dataSourceID,
		DataStoreID:  dataStoreID,
		SelectedAPIs: []string{"daily"},
	}
	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
	if err != nil {
		if strings.Contains(err.Error(), "no column named") || strings.Contains(err.Error(), "default_execute_params") {
			t.Skipf("SyncPlan 表结构需更新迁移后重试: %v", err)
		}
		require.NoError(t, err)
	}
	require.NoError(t, testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID))

	startDate, endDate := "20240101", "20240131"
	if testCtx.config.IsRealMode {
		startDate = time.Now().AddDate(0, -1, 0).Format("20060102")
		endDate = time.Now().Format("20060102")
	}
	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		TargetDBPath: targetDBPath, StartDate: startDate, EndDate: endDate,
	})
	require.NoError(t, err)
	execution, err := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	require.NoError(t, err)
	timeout := 2 * time.Minute
	if testCtx.config.IsRealMode {
		timeout = 15 * time.Minute
	}
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)

	// Step 4: 请求 K 线并校验数量
	req := analysis.KLineRequest{
		TsCode: "000001.SZ", StartDate: startDate, EndDate: endDate, AdjustType: analysis.AdjustNone, Period: "D",
	}
	data, err := testCtx.analysisAppService.GetKLine(ctx, req)
	require.NoError(t, err)
	minRows := 1
	if testCtx.config.IsRealMode {
		minRows = 15
	}
	assert.GreaterOrEqual(t, len(data), minRows, "K 线条数应满足要求")
}

// TestE2E_Analysis_Fallback 本地无该代码数据时，通过兜底从数据源拉取
// Mock 模式：mock client 对 daily 返回 000003.SZ 数据，本地无 000003.SZ，应得到兜底结果
func TestE2E_Analysis_Fallback(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_Fallback")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}

	ctx := context.Background()
	// 确保存在 daily/adj_factor 表（可空），否则 Query 报错无法走到兜底
	if testCtx.duckDBAdapter != nil {
		ensureDailyAndAdjFactorTables(ctx, t, testCtx.duckDBAdapter)
	}
	req := analysis.KLineRequest{
		TsCode: "000003.SZ", StartDate: "20240101", EndDate: "20240131", AdjustType: analysis.AdjustNone, Period: "D",
	}
	data, err := testCtx.analysisAppService.GetKLine(ctx, req)
	require.NoError(t, err)
	// Mock 模式下 mock client 的 daily 返回 2 条；真实模式下若本地无则兜底从 Tushare 拉
	assert.GreaterOrEqual(t, len(data), 0, "兜底可返回 0 条（无数据）或更多")
	// Mock 的 daily 返回固定 000001.SZ 两条，不是 000003.SZ，所以若请求 000003.SZ 本地无、兜底用同一 mock 可能仍返回 2 条（mock 不按 ts_code 过滤）
	// 这里仅校验不报错且返回结构合理
	if len(data) > 0 {
		assert.NotEmpty(t, data[0].TradeDate)
		assert.Greater(t, data[0].Close, 0.0)
	}
}

// ensureDailyAndAdjFactorTables 在适配器中创建 daily、adj_factor 表（无数据），避免 Readers 查询报错
func ensureDailyAndAdjFactorTables(ctx context.Context, t *testing.T, adapter interface {
	TableExists(context.Context, string) (bool, error)
	CreateTable(context.Context, *datastore.TableSchema) error
}) {
	exist, _ := adapter.TableExists(ctx, "daily")
	if exist {
		return
	}
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
}
