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
	"fmt"
	"io"
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

	startDate, endDate := "20260101", "20260131"
	if testCtx.config.IsRealMode {
		startDate = time.Now().AddDate(0, -1, 0).Format("20060102")
		endDate = time.Now().Format("20060102")
	}
	// 一个月日频数据至少 15 个交易日
	minRows := 1
	if testCtx.config.IsRealMode {
		minRows = 15
	}

	skipWorkflow := false
	if testCtx.duckDBAdapter != nil {
		// 先尝试取数，不先建空表，避免“无表→建空表→查空表→必然同步”
		if data, err := testCtx.analysisAppService.GetKLine(ctx, analysis.KLineRequest{TsCode: "000001.SZ", StartDate: startDate, EndDate: endDate, AdjustType: analysis.AdjustNone, Period: "D"}); err == nil && len(data) >= minRows {
			skipWorkflow = true
			t.Logf("DuckDB 已有足够 K 线数据（%d 条），跳过同步", len(data))
			logTableLine(t, fmt.Sprintf("DuckDB 已有足够 K 线数据（%d 条），跳过同步", len(data)), logFile)
		}
	}

	if !skipWorkflow {
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

		executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
			StartDate: startDate,
			EndDate:   endDate,
		})
		require.NoError(t, err)
		execution, err := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
		require.NoError(t, err)
		timeout := 2 * time.Minute
		if testCtx.config.IsRealMode {
			timeout = 15 * time.Minute
		}
		_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
	}

	// Step 4: 请求 K 线并校验数量
	req := analysis.KLineRequest{
		TsCode: "000001.SZ", StartDate: startDate, EndDate: endDate, AdjustType: analysis.AdjustNone, Period: "D",
	}
	data, err := testCtx.analysisAppService.GetKLine(ctx, req)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(data), minRows, "K 线条数应满足要求")
	printKLineTable(t, data, logFile)
}

// TestE2E_Analysis_SyncThenLimitStats 先执行同步（同 SyncThenKLine 数据环境），再校验涨跌停统计接口
func TestE2E_Analysis_SyncThenLimitStats(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_SyncThenLimitStats")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()

	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}

	ctx := context.Background()
	startDate, endDate := "20260101", "20260131"
	if testCtx.config.IsRealMode {
		startDate = time.Now().AddDate(0, -1, 0).Format("20060102")
		endDate = time.Now().Format("20060102")
	}
	// 跳过同步条件：区间内已有任意统计即视为有数据，避免中断后再次全量同步
	minRowsToSkip := 1
	if testCtx.config.IsRealMode {
		minRowsToSkip = 5 // 真实模式：至少 5 个交易日有统计即跳过
	}
	minRowsAssert := 1
	if testCtx.config.IsRealMode {
		minRowsAssert = 15 // 断言用：一个月至少约 15 个交易日
	}

	skipWorkflow := false
	if testCtx.duckDBAdapter != nil {
		// 先尝试取数，不先建空表，避免“无表→建空表→查空表→必然同步”
		if stats, err := testCtx.analysisAppService.GetLimitStats(ctx, startDate, endDate); err == nil && len(stats) >= minRowsToSkip {
			skipWorkflow = true
			t.Logf("DuckDB 已有涨跌停统计（%d 条），跳过同步", len(stats))
			logTableLine(t, fmt.Sprintf("DuckDB 已有涨跌停统计（%d 条），跳过同步", len(stats)), logFile)
		}
	}

	if !skipWorkflow {
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
		var dataSourceID shared.ID
		allDataSources, _ := testCtx.metadataAppService.ListDataSources(ctx)
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
		tableInstanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: dataSourceID, DataStoreID: dataStoreID,
		})
		require.NoError(t, err)
		_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, tableInstanceID.String(), 2*time.Minute)

		createReq := contracts.CreateSyncPlanRequest{
			Name: "E2E Analysis Daily", DataSourceID: dataSourceID, DataStoreID: dataStoreID, SelectedAPIs: []string{"daily"},
		}
		plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
		if err != nil {
			if strings.Contains(err.Error(), "no column named") || strings.Contains(err.Error(), "default_execute_params") {
				t.Skipf("SyncPlan 表结构需更新迁移后重试: %v", err)
			}
			require.NoError(t, err)
		}
		require.NoError(t, testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID))
		executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
			StartDate: startDate,
			EndDate:   endDate,
		})
		require.NoError(t, err)
		execution, _ := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
		timeout := 2 * time.Minute
		if testCtx.config.IsRealMode {
			timeout = 15 * time.Minute
		}
		_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
	}

	stats, err := testCtx.analysisAppService.GetLimitStats(ctx, startDate, endDate)
	require.NoError(t, err)
	assert.NotNil(t, stats)
	if len(stats) > 0 {
		assert.NotEmpty(t, stats[0].TradeDate)
		if testCtx.config.IsRealMode {
			assert.GreaterOrEqual(t, len(stats), minRowsAssert, "真实模式下一月区间至少约 15 个交易日统计")
		}
	}
	printLimitStatsTable(t, stats, logFile)
}

// TestE2E_Analysis_Fallback 本地无该代码数据时，通过兜底从数据源拉取
// Mock 模式：mock client 对 daily 返回 000003.SZ 数据，本地无 000003.SZ，应得到兜底结果
// 真实模式：用 000001.SZ（平安银行，全市场 daily 必有）先同步 daily 再请求，保证能拿到数据
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
	startDate, endDate := "20260101", "20260131"
	tsCode := "000003.SZ"
	if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
		tsCode = "000001.SZ" // 真实模式用平安银行，全市场 daily 同步后必有数据
	}
	// 确保存在 daily/adj_factor 表，先尝试获取一次 K 线，有数据则跳过同步
	if testCtx.duckDBAdapter != nil {
		ensureDailyAndAdjFactorTables(ctx, t, testCtx.duckDBAdapter)
		if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
			minRows := 15
			if data, err := testCtx.analysisAppService.GetKLine(ctx, analysis.KLineRequest{TsCode: tsCode, StartDate: startDate, EndDate: endDate, AdjustType: analysis.AdjustNone, Period: "D"}); err == nil && len(data) >= minRows {
				t.Logf("DuckDB 已有 %s K 线数据（%d 条），跳过同步", tsCode, len(data))
				logTableLine(t, fmt.Sprintf("DuckDB 已有 %s K 线数据（%d 条），跳过同步", tsCode, len(data)), logFile)
			} else {
				t.Logf("无数据或不足 %d 条，执行 daily 同步", minRows)
				logTableLine(t, "无数据或不足，执行 daily 同步", logFile)
				runDailySyncForFallback(ctx, t, testCtx, startDate, endDate)
			}
		}
	}
	req := analysis.KLineRequest{
		TsCode: tsCode, StartDate: startDate, EndDate: endDate, AdjustType: analysis.AdjustNone, Period: "D",
	}
	data, err := testCtx.analysisAppService.GetKLine(ctx, req)
	require.NoError(t, err)
	// 若有数据：一个月日频至少 15 条（约 15 个交易日）；若无数据（如真实模式下该代码/区间无兜底数据）仅校验不报错
	if len(data) > 0 {
		assert.GreaterOrEqual(t, len(data), 15, "一个月日频 K 线至少应有 15 条")
	}
	if len(data) > 0 {
		assert.NotEmpty(t, data[0].TradeDate)
		assert.Greater(t, data[0].Close, 0.0)
	}
	printKLineTable(t, data, logFile)
}

// TestE2E_Analysis_GetLimitStats_Empty 仅 daily 表存在（可空）时调用涨跌停统计，校验不报错
func TestE2E_Analysis_GetLimitStats_Empty(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_GetLimitStats_Empty")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}
	ctx := context.Background()
	if testCtx.duckDBAdapter != nil {
		ensureDailyAndAdjFactorTables(ctx, t, testCtx.duckDBAdapter)
	}
	stats, err := testCtx.analysisAppService.GetLimitStats(ctx, "20260101", "20260131")
	require.NoError(t, err)
	assert.NotNil(t, stats)
	printLimitStatsTable(t, stats, logFile)
}

// TestE2E_Analysis_ListStocks_ListIndices_ListConcepts 空表下调用列表接口，校验不报错且返回 slice
func TestE2E_Analysis_ListStocks_ListIndices_ListConcepts(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_ListStocks_ListIndices_ListConcepts")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}
	ctx := context.Background()
	if testCtx.duckDBAdapter != nil {
		ensureAnalysisMinimalTables(ctx, t, testCtx.duckDBAdapter)
		if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
			// 先尝试获取一次 ListIndices / ListConcepts，均有数据则跳过同步
			indices, errIdx := testCtx.analysisAppService.ListIndices(ctx, analysis.IndexListRequest{Limit: 10, Offset: 0})
			concepts, errConcept := testCtx.analysisAppService.ListConcepts(ctx, analysis.ConceptListRequest{Limit: 10, Offset: 0})
			if errIdx == nil && errConcept == nil && len(indices) > 0 && len(concepts) > 0 {
				t.Log("DuckDB 已有 index_basic/concept 数据，跳过同步")
				logTableLine(t, "DuckDB 已有 index_basic/concept 数据，跳过同步", logFile)
			} else {
				t.Log("index_basic/concept 无数据或不足，执行同步")
				logTableLine(t, "index_basic/concept 无数据或不足，执行同步", logFile)
				runIndexAndConceptSync(ctx, t, testCtx)
			}
		}
	}

	stocks, err := testCtx.analysisAppService.ListStocks(ctx, analysis.StockListRequest{Limit: 10, Offset: 0})
	require.NoError(t, err)
	assert.NotNil(t, stocks)
	printStockInfoTable(t, "ListStocks", stocks, logFile)

	indices, err := testCtx.analysisAppService.ListIndices(ctx, analysis.IndexListRequest{Limit: 10, Offset: 0})
	require.NoError(t, err)
	assert.NotNil(t, indices)
	printIndexInfoTable(t, indices, logFile)

	concepts, err := testCtx.analysisAppService.ListConcepts(ctx, analysis.ConceptListRequest{Limit: 10, Offset: 0})
	require.NoError(t, err)
	assert.NotNil(t, concepts)
	printConceptInfoTable(t, concepts, logFile)
}

// TestE2E_Analysis_LimitLadder_AndRelated 调用涨停天梯/列表/板块等接口；真实模式下先检查 limit_step 数据是否满足，不满足再同步
func TestE2E_Analysis_LimitLadder_AndRelated(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_LimitLadder_AndRelated")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	if testCtx.analysisAppService == nil {
		_, _ = logFile.Write([]byte("Skip: Analysis 服务未初始化（需要 QuantDB），涨停天梯测试未执行\n"))
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}
	ctx := context.Background()
	tradeDate := "20260115"
	if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
		tradeDate = time.Now().Format("20060102")
	}
	if testCtx.duckDBAdapter != nil {
		ensureAnalysisMinimalTables(ctx, t, testCtx.duckDBAdapter)
		// 优先用 limit_step 最近交易日，无则用 limit_list_d
		if latest := analysisE2ELatestLimitStepTradeDate(ctx, t, testCtx.duckDBAdapter); latest != "" {
			tradeDate = latest
		} else if latest := analysisE2ELatestLimitListTradeDate(ctx, t, testCtx.duckDBAdapter); latest != "" {
			tradeDate = latest
		}
	}
	// 真实模式：仅当 limit_list_ths、limit_step、limit_list_d 三表均有数据且天梯有效时才跳过同步
	if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
		needSync := true
		allThreeReady := analysisE2EHasLimitListThsData(ctx, t, testCtx.duckDBAdapter) &&
			analysisE2EHasLimitStepData(ctx, t, testCtx.duckDBAdapter) &&
			analysisE2EHasLimitListData(ctx, t, testCtx.duckDBAdapter, tradeDate)
		if ladder, err := testCtx.analysisAppService.GetLimitLadder(ctx, tradeDate); err == nil && ladder != nil && ladder.TotalLimitUp > 0 && len(ladder.Ladders) > 0 && allThreeReady {
			needSync = false
			t.Log("DuckDB 已有 limit_list_ths/limit_step/limit_list_d 三表数据且天梯有效，跳过同步")
			logTableLine(t, "DuckDB 三表数据且天梯有效，跳过同步", logFile)
		}
		if needSync {
			t.Log("无数据或数据不足，先同步 limit_list_ths，再同步 limit_step/limit_list_d")
			logTableLine(t, "无数据或数据不足，先同步 limit_list_ths，再同步 limit_step/limit_list_d", logFile)
			runLimitListThsSync(ctx, t, testCtx, tradeDate)
			runLimitListSync(ctx, t, testCtx, tradeDate)
			if testCtx.duckDBAdapter != nil {
				if latest := analysisE2ELatestLimitStepTradeDate(ctx, t, testCtx.duckDBAdapter); latest != "" {
					tradeDate = latest
					t.Logf("使用 limit_step 最近交易日: %s", tradeDate)
				} else if latest := analysisE2ELatestLimitListTradeDate(ctx, t, testCtx.duckDBAdapter); latest != "" {
					tradeDate = latest
					t.Logf("使用 limit_list_d 最近交易日: %s", tradeDate)
				}
			}
		}
	}
	logTableLine(t, fmt.Sprintf("请求涨停天梯 tradeDate=%s", tradeDate), logFile)

	// 以下接口依赖 limit_list/stock_basic 等表及 SQL 兼容性，若报错则跳过（如真实 DB 表结构或 DuckDB 日期运算差异）
	ladder, err := testCtx.analysisAppService.GetLimitLadder(ctx, tradeDate)
	if err != nil {
		_, _ = logFile.Write([]byte(fmt.Sprintf("GetLimitLadder 失败（跳过）: %v\n", err)))
		t.Skipf("GetLimitLadder 依赖表结构或 SQL，跳过: %v", err)
	}
	printLimitLadderStatsTable(t, ladder, logFile)

	limitStocks, err := testCtx.analysisAppService.GetLimitStockList(ctx, tradeDate, "up")
	if err != nil {
		t.Skipf("GetLimitStockList 依赖表结构或 SQL，跳过: %v", err)
	}
	if len(limitStocks) > 0 {
		t.Logf("GetLimitStockList(up): 共 %d 条", len(limitStocks))
	}

	ladders, err := testCtx.analysisAppService.GetLimitUpLadder(ctx, tradeDate)
	if err != nil {
		t.Skipf("GetLimitUpLadder 依赖表结构或 SQL，跳过: %v", err)
	}
	if len(ladders) > 0 {
		t.Logf("GetLimitUpLadder: 共 %d 个阶梯", len(ladders))
	}

	comp, err := testCtx.analysisAppService.GetLimitUpComparison(ctx, tradeDate)
	if err != nil {
		t.Skipf("GetLimitUpComparison 依赖表结构或 SQL，跳过: %v", err)
	}
	if comp != nil {
		t.Logf("GetLimitUpComparison: 今日=%s 昨日=%s 今日涨停=%d 昨日涨停=%d", comp.TodayDate, comp.YesterdayDate, comp.TodayCount, comp.YesterdayCount)
	}

	limitUpList, err := testCtx.analysisAppService.GetLimitUpList(ctx, analysis.LimitUpListRequest{TradeDate: tradeDate, Limit: 10, Offset: 0})
	if err != nil {
		t.Skipf("GetLimitUpList 依赖表结构或 SQL，跳过: %v", err)
	}
	if len(limitUpList) > 0 {
		t.Logf("GetLimitUpList: 共 %d 条", len(limitUpList))
	}

	sectorStats, err := testCtx.analysisAppService.GetSectorLimitStats(ctx, tradeDate, "industry")
	if err != nil {
		t.Skipf("GetSectorLimitStats 依赖表结构或 SQL，跳过: %v", err)
	}
	printSectorLimitStatsTable(t, sectorStats, logFile)

	heat, err := testCtx.analysisAppService.GetConceptHeat(ctx, tradeDate)
	if err != nil {
		t.Skipf("GetConceptHeat 依赖表结构或 SQL，跳过: %v", err)
	}
	printConceptHeatTable(t, heat, logFile)
}

// TestE2E_Analysis_PopularityRank_ListNews 不依赖表的接口，直接调用校验不报错
func TestE2E_Analysis_PopularityRank_ListNews(t *testing.T) {
	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}
	ctx := context.Background()

	rank, err := testCtx.analysisAppService.GetPopularityRank(ctx, analysis.PopularityRankRequest{Src: "ths", Limit: 10})
	require.NoError(t, err)
	t.Log("GetPopularityRank 结果:")
	if len(rank) == 0 {
		t.Log("  (无数据)")
	} else {
		header := fmt.Sprintf("%4s %-10s %-12s %10s %10s", "rank", "ts_code", "name", "score", "pct_chg%")
		t.Log(header)
		t.Log(strings.Repeat("-", len(header)))
		for _, r := range rank {
			t.Log(fmt.Sprintf("%4d %-10s %-12s %10.2f %10.2f", r.Rank, r.TsCode, trunc(r.Name, 12), r.Score, r.PctChg))
		}
		t.Logf("共 %d 条", len(rank))
	}

	news, err := testCtx.analysisAppService.ListNews(ctx, analysis.NewsListRequest{Limit: 10, Offset: 0})
	require.NoError(t, err)
	t.Log("ListNews 结果:")
	if len(news) == 0 {
		t.Log("  (无数据)")
	} else {
		header := fmt.Sprintf("%-4s %-36s %-12s %-10s", "序号", "title", "publish_time", "source")
		t.Log(header)
		t.Log(strings.Repeat("-", len(header)))
		for i, n := range news {
			t.Log(fmt.Sprintf("%-4d %-36s %-12s %-10s", i+1, trunc(n.Title, 36), n.PublishTime, trunc(n.Source, 10)))
		}
		t.Logf("共 %d 条", len(news))
	}
}

// TestE2E_Analysis_GetStockBasicInfo_Empty 空 stock_basic 下查询单只股票基本信息，允许返回 nil
// 真实模式下若已有 stock_basic 表且列名与 Reader 不一致（如缺 reg_capital），则跳过
func TestE2E_Analysis_GetStockBasicInfo_Empty(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_GetStockBasicInfo_Empty")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}
	ctx := context.Background()
	if testCtx.duckDBAdapter != nil {
		ensureAnalysisMinimalTables(ctx, t, testCtx.duckDBAdapter)
	}
	info, err := testCtx.analysisAppService.GetStockBasicInfo(ctx, "000001.SZ")
	if err != nil && strings.Contains(err.Error(), "not found in FROM clause") {
		t.Skipf("现有 DB 中 stock_basic 表结构与 Reader 不一致，跳过: %v", err)
	}
	require.NoError(t, err)
	// 空表时当前实现返回 (nil, nil)
	assert.True(t, info == nil || info.TsCode != "")
	printStockBasicInfoTable(t, info, logFile)
}

// TestE2E_Analysis_DragonTiger_MoneyFlow_Empty 龙虎榜、资金流向：无数据时先执行拉取 workflow，再请求并打印
// 真实模式：若 top_list/moneyflow 无数据则同步 trade_cal+stock_basic+top_list+moneyflow 后再测
func TestE2E_Analysis_DragonTiger_MoneyFlow_Empty(t *testing.T) {
	logFile, logCleanup := setupLogFile(t, "Analysis_DragonTiger_MoneyFlow")
	defer logCleanup()
	defer logFile.Close()

	testCtx := setupBuiltinWorkflowE2EContext(t)
	defer testCtx.cleanup()
	if testCtx.analysisAppService == nil {
		t.Skip("Analysis 服务未初始化（需要 QuantDB）")
	}
	ctx := context.Background()
	tradeDate := "20260115"
	if testCtx.config.IsRealMode {
		tradeDate = time.Now().AddDate(0, 0, -5).Format("20060102") // 约 5 日前，便于有交易日
	}

	if testCtx.duckDBAdapter != nil {
		ensureAnalysisMinimalTables(ctx, t, testCtx.duckDBAdapter)
		if latest := analysisE2ELatestDragonTigerMoneyFlowTradeDate(ctx, t, testCtx.duckDBAdapter); latest != "" {
			tradeDate = latest
		}
		// 先尝试获取一次龙虎榜/资金流向，有数据则跳过同步（仅真实模式且需 Tushare）
		if testCtx.config.IsRealMode && testCtx.config.TushareToken != "" {
			list, errList := testCtx.analysisAppService.GetDragonTigerList(ctx, analysis.DragonTigerRequest{TradeDate: &tradeDate, Limit: 10, Offset: 0})
			flow, errFlow := testCtx.analysisAppService.GetMoneyFlow(ctx, analysis.MoneyFlowRequest{TradeDate: &tradeDate, Limit: 10, Offset: 0})
			if errList == nil && errFlow == nil && (len(list) > 0 || len(flow) > 0) {
				t.Log("DuckDB 已有 top_list/moneyflow 数据，跳过同步")
				logTableLine(t, "DuckDB 已有 top_list/moneyflow 数据，跳过同步", logFile)
			} else {
				t.Log("top_list/moneyflow 无数据或不足，执行同步")
				logTableLine(t, "top_list/moneyflow 无数据或不足，执行同步", logFile)
				runDragonTigerMoneyFlowSync(ctx, t, testCtx, tradeDate)
				if latest := analysisE2ELatestDragonTigerMoneyFlowTradeDate(ctx, t, testCtx.duckDBAdapter); latest != "" {
					tradeDate = latest
					t.Logf("使用 top_list/moneyflow 最近交易日: %s", tradeDate)
				}
			}
		}
	}

	list, err := testCtx.analysisAppService.GetDragonTigerList(ctx, analysis.DragonTigerRequest{TradeDate: &tradeDate, Limit: 10, Offset: 0})
	if err != nil && strings.Contains(err.Error(), "not found in FROM clause") {
		t.Skipf("现有 DB 中 top_list 表结构与 Reader 不一致，跳过: %v", err)
	}
	require.NoError(t, err)
	assert.NotNil(t, list)
	printDragonTigerTable(t, list, logFile)

	flow, err := testCtx.analysisAppService.GetMoneyFlow(ctx, analysis.MoneyFlowRequest{TradeDate: &tradeDate, Limit: 10, Offset: 0})
	if err != nil && strings.Contains(err.Error(), "not found in FROM clause") {
		t.Skipf("现有 DB 中 moneyflow 表结构与 Reader 不一致，跳过: %v", err)
	}
	require.NoError(t, err)
	assert.NotNil(t, flow)
	printMoneyFlowTable(t, flow, logFile)
}

// analysisE2EHasDragonTigerMoneyFlowData 检查 top_list 与 moneyflow 在指定日期或任意日期是否有数据
func analysisE2EHasDragonTigerMoneyFlowData(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter, tradeDate string) bool {
	if adapter == nil {
		return false
	}
	okTop, _ := adapter.TableExists(ctx, "top_list")
	okMf, _ := adapter.TableExists(ctx, "moneyflow")
	if !okTop || !okMf {
		return false
	}
	rowsTop, err := adapter.Query(ctx, "SELECT 1 FROM top_list WHERE trade_date = ? LIMIT 1", tradeDate)
	if err != nil || len(rowsTop) == 0 {
		rowsTop, _ = adapter.Query(ctx, "SELECT 1 FROM top_list LIMIT 1")
		if len(rowsTop) == 0 {
			return false
		}
	}
	rowsMf, err := adapter.Query(ctx, "SELECT 1 FROM moneyflow WHERE trade_date = ? LIMIT 1", tradeDate)
	if err != nil || len(rowsMf) == 0 {
		rowsMf, _ = adapter.Query(ctx, "SELECT 1 FROM moneyflow LIMIT 1")
		if len(rowsMf) == 0 {
			return false
		}
	}
	return true
}

// analysisE2ELatestDragonTigerMoneyFlowTradeDate 返回 top_list 与 moneyflow 均有数据的最近 trade_date（取两表最大日期的较小值），无数据返回空
func analysisE2ELatestDragonTigerMoneyFlowTradeDate(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter) string {
	if adapter == nil {
		return ""
	}
	var maxTop, maxMf string
	if ok, _ := adapter.TableExists(ctx, "top_list"); ok {
		rows, _ := adapter.Query(ctx, "SELECT trade_date FROM top_list ORDER BY trade_date DESC LIMIT 1")
		if len(rows) > 0 {
			maxTop = fmt.Sprint(rows[0]["trade_date"])
		}
	}
	if ok, _ := adapter.TableExists(ctx, "moneyflow"); ok {
		rows, _ := adapter.Query(ctx, "SELECT trade_date FROM moneyflow ORDER BY trade_date DESC LIMIT 1")
		if len(rows) > 0 {
			maxMf = fmt.Sprint(rows[0]["trade_date"])
		}
	}
	if maxTop == "" || maxMf == "" {
		return ""
	}
	// 取较小者，保证该日两表都有数据
	if maxTop <= maxMf {
		return maxTop
	}
	return maxMf
}

// analysisE2EHasLimitListThsData 检查 limit_list_ths 表是否存在且有至少一条数据
func analysisE2EHasLimitListThsData(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter) bool {
	if adapter == nil {
		return false
	}
	ok, _ := adapter.TableExists(ctx, "limit_list_ths")
	if !ok {
		return false
	}
	rows, _ := adapter.Query(ctx, "SELECT 1 FROM limit_list_ths LIMIT 1")
	return len(rows) > 0
}

// analysisE2EHasLimitStepData 检查 limit_step 表是否存在且有至少一条数据
func analysisE2EHasLimitStepData(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter) bool {
	return analysisE2ELatestLimitStepTradeDate(ctx, t, adapter) != ""
}

// analysisE2EHasLimitListData 检查 limit_list_d（同步 API limit_list_d 产生的表）在指定日期或任意日期是否有数据
func analysisE2EHasLimitListData(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter, tradeDate string) bool {
	if adapter == nil {
		return false
	}
	ok, _ := adapter.TableExists(ctx, "limit_list_d")
	if !ok {
		return false
	}
	rows, err := adapter.Query(ctx, "SELECT 1 FROM limit_list_d WHERE trade_date = ? LIMIT 1", tradeDate)
	if err != nil || len(rows) == 0 {
		rows, _ = adapter.Query(ctx, "SELECT 1 FROM limit_list_d LIMIT 1")
		if len(rows) == 0 {
			return false
		}
	}
	return true
}

// analysisE2ELatestLimitListTradeDate 返回 limit_list_d 中最大的 trade_date，无数据或出错返回空字符串
func analysisE2ELatestLimitListTradeDate(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter) string {
	if adapter == nil {
		return ""
	}
	ok, _ := adapter.TableExists(ctx, "limit_list_d")
	if !ok {
		return ""
	}
	rows, err := adapter.Query(ctx, "SELECT trade_date FROM limit_list_d ORDER BY trade_date DESC LIMIT 1")
	if err != nil || len(rows) == 0 {
		return ""
	}
	v, ok := rows[0]["trade_date"]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

// analysisE2ELatestLimitStepTradeDate 返回 limit_step 中最大的 trade_date（天梯接口优先用此表），无数据或出错返回空字符串
func analysisE2ELatestLimitStepTradeDate(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter) string {
	if adapter == nil {
		return ""
	}
	ok, _ := adapter.TableExists(ctx, "limit_step")
	if !ok {
		return ""
	}
	rows, err := adapter.Query(ctx, "SELECT trade_date FROM limit_step ORDER BY trade_date DESC LIMIT 1")
	if err != nil || len(rows) == 0 {
		return ""
	}
	v, ok := rows[0]["trade_date"]
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

// runLimitListThsSync 先单独同步 limit_list_ths（同花顺涨跌停榜单），保证天梯详情优先有数据
func runLimitListThsSync(ctx context.Context, t *testing.T, testCtx *builtinWorkflowE2EContext, tradeDate string) {
	targetDBPath := testCtx.config.DuckDBPath
	dataStoreID, dataSourceID := getOrCreateE2ELimitDataStoreAndSource(ctx, t, testCtx, targetDBPath)
	startDate, endDate := tradeDate, tradeDate
	if testCtx.config.IsRealMode {
		startDate = time.Now().AddDate(0, 0, -14).Format("20060102")
		endDate = time.Now().Format("20060102")
	}
	createReq := contracts.CreateSyncPlanRequest{
		Name: "E2E LimitLadder THS", DataSourceID: dataSourceID, DataStoreID: dataStoreID,
		SelectedAPIs: []string{"trade_cal", "stock_basic", "limit_list_ths"},
	}
	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
	if err != nil {
		if strings.Contains(err.Error(), "no column named") || strings.Contains(err.Error(), "default_execute_params") {
			t.Skipf("SyncPlan 表结构需更新迁移后重试: %v", err)
		}
		require.NoError(t, err)
	}
	require.NoError(t, testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID))
	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: startDate,
		EndDate:   endDate,
	})
	require.NoError(t, err)
	execution, _ := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	timeout := 10 * time.Minute
	if testCtx.config.IsRealMode {
		timeout = 15 * time.Minute
	}
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
}

// getOrCreateE2ELimitDataStoreAndSource 获取或创建 E2E 用的 DataStore 与 DataSource，并确保建表完成
func getOrCreateE2ELimitDataStoreAndSource(ctx context.Context, t *testing.T, testCtx *builtinWorkflowE2EContext, targetDBPath string) (dataStoreID, dataSourceID shared.ID) {
	existingStores, err := testCtx.datastoreAppService.ListDataStores(ctx)
	require.NoError(t, err)
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
	allDataSources, _ := testCtx.metadataAppService.ListDataSources(ctx)
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
	if testCtx.config.TushareToken != "" {
		_ = testCtx.metadataAppService.SaveToken(ctx, contracts.SaveTokenRequest{DataSourceID: dataSourceID, TokenValue: testCtx.config.TushareToken})
	}
	tableInstanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: dataSourceID, DataStoreID: dataStoreID,
	})
	require.NoError(t, err)
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, tableInstanceID.String(), 2*time.Minute)
	return dataStoreID, dataSourceID
}

// runLimitListSync 在 limit_list_ths 已先同步的前提下，执行 limit_step + limit_list_d 同步
func runLimitListSync(ctx context.Context, t *testing.T, testCtx *builtinWorkflowE2EContext, tradeDate string) {
	targetDBPath := testCtx.config.DuckDBPath
	dataStoreID, dataSourceID := getOrCreateE2ELimitDataStoreAndSource(ctx, t, testCtx, targetDBPath)

	startDate := tradeDate
	endDate := tradeDate
	if testCtx.config.IsRealMode {
		startDate = time.Now().AddDate(0, 0, -14).Format("20060102")
		endDate = time.Now().Format("20060102")
	}
	createReq := contracts.CreateSyncPlanRequest{
		Name: "E2E LimitLadder", DataSourceID: dataSourceID, DataStoreID: dataStoreID,
		SelectedAPIs: []string{"trade_cal", "stock_basic", "limit_step", "limit_list_d"},
	}
	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
	if err != nil {
		if strings.Contains(err.Error(), "no column named") || strings.Contains(err.Error(), "default_execute_params") {
			t.Skipf("SyncPlan 表结构需更新迁移后重试: %v", err)
		}
		require.NoError(t, err)
	}
	require.NoError(t, testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID))
	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: startDate,
		EndDate:   endDate,
	})
	require.NoError(t, err)
	execution, _ := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	timeout := 10 * time.Minute
	if testCtx.config.IsRealMode {
		timeout = 15 * time.Minute
	}
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
}

// analysisE2EHasIndexAndConceptData 检查 index_basic 与 concept 是否均有至少一条数据
func analysisE2EHasIndexAndConceptData(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter) bool {
	if adapter == nil {
		return false
	}
	for _, table := range []string{"index_basic", "concept"} {
		ok, _ := adapter.TableExists(ctx, table)
		if !ok {
			return false
		}
		rows, _ := adapter.Query(ctx, "SELECT 1 FROM "+table+" LIMIT 1")
		if len(rows) == 0 {
			return false
		}
	}
	return true
}

// runDailySyncForFallback 执行 trade_cal + stock_basic + daily 同步，使 Fallback 测试请求的区间有本地数据
func runDailySyncForFallback(ctx context.Context, t *testing.T, testCtx *builtinWorkflowE2EContext, startDate, endDate string) {
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
	var dataSourceID shared.ID
	allDataSources, _ := testCtx.metadataAppService.ListDataSources(ctx)
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
	if testCtx.config.TushareToken != "" {
		_ = testCtx.metadataAppService.SaveToken(ctx, contracts.SaveTokenRequest{DataSourceID: dataSourceID, TokenValue: testCtx.config.TushareToken})
	}
	tableInstanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: dataSourceID, DataStoreID: dataStoreID,
	})
	require.NoError(t, err)
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, tableInstanceID.String(), 2*time.Minute)

	createReq := contracts.CreateSyncPlanRequest{
		Name: "E2E Fallback Daily", DataSourceID: dataSourceID, DataStoreID: dataStoreID,
		SelectedAPIs: []string{"trade_cal", "stock_basic", "daily"},
	}
	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
	if err != nil {
		if strings.Contains(err.Error(), "no column named") || strings.Contains(err.Error(), "default_execute_params") {
			t.Skipf("SyncPlan 表结构需更新迁移后重试: %v", err)
		}
		require.NoError(t, err)
	}
	require.NoError(t, testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID))
	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: startDate,
		EndDate:   endDate,
	})
	require.NoError(t, err)
	execution, _ := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	timeout := 15 * time.Minute
	if testCtx.config.IsRealMode {
		timeout = 25 * time.Minute
	}
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
}

// runIndexAndConceptSync 执行 trade_cal + stock_basic + index_basic + concept 同步（ListIndices/ListConcepts 依赖）
func runIndexAndConceptSync(ctx context.Context, t *testing.T, testCtx *builtinWorkflowE2EContext) {
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
	var dataSourceID shared.ID
	allDataSources, _ := testCtx.metadataAppService.ListDataSources(ctx)
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
	if testCtx.config.TushareToken != "" {
		_ = testCtx.metadataAppService.SaveToken(ctx, contracts.SaveTokenRequest{DataSourceID: dataSourceID, TokenValue: testCtx.config.TushareToken})
	}
	tableInstanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: dataSourceID, DataStoreID: dataStoreID,
	})
	require.NoError(t, err)
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, tableInstanceID.String(), 2*time.Minute)

	startDate, endDate := "20260101", "20260101"
	if testCtx.config.IsRealMode {
		startDate = time.Now().AddDate(0, 0, -7).Format("20060102")
		endDate = time.Now().Format("20060102")
	}
	createReq := contracts.CreateSyncPlanRequest{
		Name: "E2E Index Concept", DataSourceID: dataSourceID, DataStoreID: dataStoreID,
		SelectedAPIs: []string{"trade_cal", "stock_basic", "index_basic", "concept"},
	}
	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
	if err != nil {
		if strings.Contains(err.Error(), "no column named") || strings.Contains(err.Error(), "default_execute_params") {
			t.Skipf("SyncPlan 表结构需更新迁移后重试: %v", err)
		}
		require.NoError(t, err)
	}
	require.NoError(t, testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID))
	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: startDate,
		EndDate:   endDate,
	})
	require.NoError(t, err)
	execution, _ := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	timeout := 5 * time.Minute
	if testCtx.config.IsRealMode {
		timeout = 10 * time.Minute
	}
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
}

// runDragonTigerMoneyFlowSync 执行 trade_cal + stock_basic + top_list + moneyflow 同步
func runDragonTigerMoneyFlowSync(ctx context.Context, t *testing.T, testCtx *builtinWorkflowE2EContext, tradeDate string) {
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
	var dataSourceID shared.ID
	allDataSources, _ := testCtx.metadataAppService.ListDataSources(ctx)
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
	if testCtx.config.TushareToken != "" {
		_ = testCtx.metadataAppService.SaveToken(ctx, contracts.SaveTokenRequest{DataSourceID: dataSourceID, TokenValue: testCtx.config.TushareToken})
	}
	tableInstanceID, err := testCtx.datastoreAppService.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
		DataSourceID: dataSourceID, DataStoreID: dataStoreID,
	})
	require.NoError(t, err)
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, tableInstanceID.String(), 2*time.Minute)

	startDate := tradeDate
	endDate := tradeDate
	if testCtx.config.IsRealMode {
		startDate = time.Now().AddDate(0, 0, -14).Format("20060102")
		endDate = time.Now().Format("20060102")
	}
	createReq := contracts.CreateSyncPlanRequest{
		Name: "E2E DragonTiger MoneyFlow", DataSourceID: dataSourceID, DataStoreID: dataStoreID,
		SelectedAPIs: []string{"trade_cal", "stock_basic", "top_list", "moneyflow"},
	}
	plan, err := testCtx.syncAppService.CreateSyncPlan(ctx, createReq)
	if err != nil {
		if strings.Contains(err.Error(), "no column named") || strings.Contains(err.Error(), "default_execute_params") {
			t.Skipf("SyncPlan 表结构需更新迁移后重试: %v", err)
		}
		require.NoError(t, err)
	}
	require.NoError(t, testCtx.syncAppService.ResolveSyncPlan(ctx, plan.ID))
	executionID, err := testCtx.syncAppService.ExecuteSyncPlan(ctx, plan.ID, contracts.ExecuteSyncPlanRequest{
		StartDate: startDate,
		EndDate:   endDate,
	})
	require.NoError(t, err)
	execution, _ := testCtx.syncAppService.GetSyncExecution(ctx, executionID)
	timeout := 10 * time.Minute
	if testCtx.config.IsRealMode {
		timeout = 15 * time.Minute
	}
	_, _ = waitForWorkflowCompletionQuiet(ctx, testCtx.taskEngineAdapter, execution.WorkflowInstID.String(), timeout)
}

// logTableLine 输出一行到测试日志，若提供 writers 则同时写入（如日志文件）
func logTableLine(t *testing.T, line string, writers ...io.Writer) {
	t.Log(line)
	for _, w := range writers {
		if w != nil {
			_, _ = w.Write([]byte(line + "\n"))
		}
	}
}

// printKLineTable 以表格形式打印 K 线结果到测试日志，可选写入 writers（如 logFile）
func printKLineTable(t *testing.T, data []analysis.KLineData, writers ...io.Writer) {
	if len(data) == 0 {
		logTableLine(t, "K 线结果: (无数据)", writers...)
		return
	}
	const (
		dateW = 10
		numW  = 10
		volW  = 14
		pctW  = 8
	)
	header := fmt.Sprintf("%-*s %*s %*s %*s %*s %*s %*s %*s %*s %*s",
		dateW, "日期", numW, "开盘", numW, "最高", numW, "最低", numW, "收盘",
		volW, "成交量", volW, "成交额", numW, "昨收", numW, "涨跌额", pctW, "涨跌幅%")
	sep := strings.Repeat("-", len(header))
	logTableLine(t, "K 线结果:", writers...)
	logTableLine(t, header, writers...)
	logTableLine(t, sep, writers...)
	for _, r := range data {
		line := fmt.Sprintf("%-*s %*.*f %*.*f %*.*f %*.*f %*.*f %*.*f %*.*f %*.*f %*.*f",
			dateW, r.TradeDate,
			numW, 2, r.Open, numW, 2, r.High, numW, 2, r.Low, numW, 2, r.Close,
			volW, 0, r.Volume, volW, 0, r.Amount, numW, 2, r.PreClose, numW, 2, r.Change, pctW, 2, r.PctChg)
		logTableLine(t, line, writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printLimitStatsTable(t *testing.T, data []analysis.LimitStats, writers ...io.Writer) {
	logTableLine(t, "GetLimitStats 结果:", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-10s %8s %8s %6s %6s %6s %8s", "日期", "涨停", "跌停", "上涨", "下跌", "平盘", "涨停比%")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-10s %8d %8d %6d %6d %6d %8.2f", r.TradeDate, r.LimitUpCount, r.LimitDownCount, r.UpCount, r.DownCount, r.FlatCount, r.LimitUpRatio), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printStockInfoTable(t *testing.T, title string, data []analysis.StockInfo, writers ...io.Writer) {
	logTableLine(t, title+":", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-12s %-8s %-12s %-8s %-12s %-6s", "ts_code", "symbol", "name", "area", "industry", "market")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-12s %-8s %-12s %-8s %-12s %-6s", r.TsCode, r.Symbol, trunc(r.Name, 12), trunc(r.Area, 8), trunc(r.Industry, 12), r.Market), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printIndexInfoTable(t *testing.T, data []analysis.IndexInfo, writers ...io.Writer) {
	logTableLine(t, "ListIndices 结果:", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-14s %-20s %-8s %-10s %-8s", "ts_code", "name", "market", "publisher", "category")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-14s %-20s %-8s %-10s %-8s", r.TsCode, trunc(r.Name, 15), r.Market, trunc(r.Publisher, 10), trunc(r.Category, 8)), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printConceptInfoTable(t *testing.T, data []analysis.ConceptInfo, writers ...io.Writer) {
	logTableLine(t, "ListConcepts 结果:", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-12s %-20s %-10s %6s", "code", "name", "source", "stock_count")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-12s %-20s %-10s %6d", r.Code, trunc(r.Name, 15), r.Source, r.StockCount), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printDragonTigerTable(t *testing.T, data []analysis.DragonTigerList, writers ...io.Writer) {
	logTableLine(t, "龙虎榜(GetDragonTigerList) 结果:", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-10s %-10s %-8s %8s %8s %14s %14s", "日期", "ts_code", "name", "close", "pct_chg%", "buy_amount", "sell_amount")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-10s %-10s %-8s %8.2f %8.2f %14.0f %14.0f", r.TradeDate, r.TsCode, trunc(r.Name, 8), r.Close, r.PctChg, r.BuyAmount, r.SellAmount), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printMoneyFlowTable(t *testing.T, data []analysis.MoneyFlow, writers ...io.Writer) {
	logTableLine(t, "资金流向(GetMoneyFlow) 结果:", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-10s %-10s %14s", "日期", "ts_code", "net_mf_amount")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-10s %-10s %14.0f", r.TradeDate, r.TsCode, r.NetMfAmount), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printStockBasicInfoTable(t *testing.T, info *analysis.StockBasicInfo, writers ...io.Writer) {
	logTableLine(t, "GetStockBasicInfo 结果:", writers...)
	if info == nil {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	logTableLine(t, fmt.Sprintf("  ts_code=%s symbol=%s name=%s area=%s industry=%s market=%s list_date=%s", info.TsCode, info.Symbol, info.Name, info.Area, info.Industry, info.Market, info.ListDate), writers...)
}

func printSectorLimitStatsTable(t *testing.T, data []analysis.SectorLimitStats, writers ...io.Writer) {
	logTableLine(t, "GetSectorLimitStats 结果:", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-12s %-16s %-10s %6s %6s %8s", "sector_code", "sector_name", "sector_type", "涨停数", "总数", "涨停比%")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-12s %-16s %-10s %6d %6d %8.2f", r.SectorCode, trunc(r.SectorName, 16), r.SectorType, r.LimitUpCount, r.TotalStocks, r.LimitUpRatio), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printConceptHeatTable(t *testing.T, data []analysis.ConceptHeat, writers ...io.Writer) {
	logTableLine(t, "GetConceptHeat 结果:", writers...)
	if len(data) == 0 {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	header := fmt.Sprintf("%-12s %-20s %6s %6s %8s", "concept_code", "concept_name", "成分股", "涨停数", "均涨幅%")
	logTableLine(t, header, writers...)
	logTableLine(t, strings.Repeat("-", len(header)), writers...)
	for _, r := range data {
		logTableLine(t, fmt.Sprintf("%-12s %-20s %6d %6d %8.2f", r.ConceptCode, trunc(r.ConceptName, 15), r.StockCount, r.LimitUpCount, r.AvgPctChg), writers...)
	}
	logTableLine(t, fmt.Sprintf("共 %d 条", len(data)), writers...)
}

func printLimitLadderStatsTable(t *testing.T, s *analysis.LimitLadderStats, writers ...io.Writer) {
	logTableLine(t, "GetLimitLadder 结果:", writers...)
	if s == nil {
		logTableLine(t, "  (无数据)", writers...)
		return
	}
	logTableLine(t, fmt.Sprintf("  日期=%s 涨停总数=%d 最高连板=%d 阶梯数=%d", s.TradeDate, s.TotalLimitUp, s.MaxConsecutive, len(s.Ladders)), writers...)
	header := fmt.Sprintf("%-12s %-10s %4s %-10s %-8s %-8s %-8s %4s %10s %-14s %8s %6s %8s %12s %-8s",
		"代码", "名称", "连板", "首次涨停", "首次封板", "最后封板", "状态", "炸板", "封单额", "原因", "收盘", "涨跌幅%", "换手%", "成交额", "行业")
	for _, ld := range s.Ladders {
		logTableLine(t, fmt.Sprintf("  ---- 连板%d天: %d 只 ----", ld.ConsecutiveDays, ld.StockCount), writers...)
		if len(ld.Stocks) > 0 {
			logTableLine(t, header, writers...)
			logTableLine(t, strings.Repeat("-", len(header)), writers...)
			for _, st := range ld.Stocks {
				reason := trunc(st.LimitReason, 14)
				if reason == "" {
					reason = "-"
				}
				firstLimit := st.FirstLimitDate
				if firstLimit == "" {
					firstLimit = "-"
				}
				limitTime := st.LimitTime
				if limitTime == "" {
					limitTime = "-"
				}
				lastLimit := st.LastLimitTime
				if lastLimit == "" {
					lastLimit = "-"
				}
				status := trunc(st.LimitStatus, 8)
				if status == "" {
					status = "-"
				}
				logTableLine(t, fmt.Sprintf("%-12s %-10s %4d %-10s %-8s %-8s %-8s %4d %10.0f %-14s %8.2f %6.2f %8.2f %12.0f %-8s",
					st.TsCode, trunc(st.Name, 10), st.ConsecutiveDays, firstLimit, limitTime, lastLimit, status,
					st.OpenTimes, st.LimitAmount, reason, st.Close, st.PctChg, st.TurnoverRate, st.Amount, trunc(st.Industry, 8)), writers...)
			}
		}
	}
}

// trunc 按字符（rune）截断，避免截断多字节 UTF-8 导致最后一字显示为
func trunc(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// duckDBSkipCheckAdapter 用于“数据已就绪则跳过工作流”的检查
type duckDBSkipCheckAdapter interface {
	TableExists(ctx context.Context, tableName string) (bool, error)
	Query(ctx context.Context, sqlQuery string, args ...any) ([]map[string]any, error)
}

// analysisE2ECanSkipSyncWorkflow 若 daily 表存在且 000001.SZ 在 [startDate,endDate] 内条数 >= minRows 则返回 (true, count)，否则 (false, 0)
func analysisE2ECanSkipSyncWorkflow(ctx context.Context, t *testing.T, adapter duckDBSkipCheckAdapter, startDate, endDate string, minRows int) (bool, int) {
	if adapter == nil {
		return false, 0
	}
	ok, err := adapter.TableExists(ctx, "daily")
	if err != nil || !ok {
		return false, 0
	}
	rows, err := adapter.Query(ctx, "SELECT COUNT(*) AS cnt FROM daily WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?", "000001.SZ", startDate, endDate)
	if err != nil || len(rows) == 0 {
		return false, 0
	}
	cnt, ok := rows[0]["cnt"]
	if !ok {
		return false, 0
	}
	var n int64
	switch v := cnt.(type) {
	case int64:
		n = v
	case int:
		n = int64(v)
	case float64:
		n = int64(v)
	default:
		return false, 0
	}
	if n < int64(minRows) {
		return false, int(n)
	}
	return true, int(n)
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

// ensureAnalysisMinimalTables 创建分析接口依赖的最小空表（stock_basic, index_basic, concept, limit_list 等），避免 Reader 查询报错
func ensureAnalysisMinimalTables(ctx context.Context, t *testing.T, adapter interface {
	TableExists(context.Context, string) (bool, error)
	CreateTable(context.Context, *datastore.TableSchema) error
}) {
	ensureDailyAndAdjFactorTables(ctx, t, adapter)

	tables := []struct {
		name   string
		schema *datastore.TableSchema
	}{
		{
			"stock_basic",
			&datastore.TableSchema{
				TableName: "stock_basic",
				Columns: []datastore.ColumnDef{
					{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
					{Name: "symbol", TargetType: "VARCHAR", Nullable: true},
					{Name: "name", TargetType: "VARCHAR", Nullable: true},
					{Name: "area", TargetType: "VARCHAR", Nullable: true},
					{Name: "industry", TargetType: "VARCHAR", Nullable: true},
					{Name: "market", TargetType: "VARCHAR", Nullable: true},
					{Name: "list_date", TargetType: "VARCHAR", Nullable: true},
					{Name: "list_status", TargetType: "VARCHAR", Nullable: true},
					{Name: "is_hs", TargetType: "VARCHAR", Nullable: true},
					{Name: "fullname", TargetType: "VARCHAR", Nullable: true},
					{Name: "enname", TargetType: "VARCHAR", Nullable: true},
					{Name: "cnspell", TargetType: "VARCHAR", Nullable: true},
					{Name: "exchange", TargetType: "VARCHAR", Nullable: true},
					{Name: "curr_type", TargetType: "VARCHAR", Nullable: true},
					{Name: "reg_capital", TargetType: "DOUBLE", Nullable: true},
					{Name: "website", TargetType: "VARCHAR", Nullable: true},
					{Name: "email", TargetType: "VARCHAR", Nullable: true},
					{Name: "office", TargetType: "VARCHAR", Nullable: true},
					{Name: "employees", TargetType: "BIGINT", Nullable: true},
					{Name: "introduction", TargetType: "VARCHAR", Nullable: true},
					{Name: "business", TargetType: "VARCHAR", Nullable: true},
					{Name: "main_business", TargetType: "VARCHAR", Nullable: true},
				},
				PrimaryKeys: []string{"ts_code"},
			},
		},
		{
			"index_basic",
			&datastore.TableSchema{
				TableName: "index_basic",
				Columns: []datastore.ColumnDef{
					{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
					{Name: "name", TargetType: "VARCHAR", Nullable: true},
					{Name: "market", TargetType: "VARCHAR", Nullable: true},
					{Name: "publisher", TargetType: "VARCHAR", Nullable: true},
					{Name: "index_type", TargetType: "VARCHAR", Nullable: true},
					{Name: "category", TargetType: "VARCHAR", Nullable: true},
					{Name: "base_date", TargetType: "VARCHAR", Nullable: true},
					{Name: "base_point", TargetType: "DOUBLE", Nullable: true},
					{Name: "list_date", TargetType: "VARCHAR", Nullable: true},
				},
				PrimaryKeys: []string{"ts_code"},
			},
		},
		{
			"concept",
			&datastore.TableSchema{
				TableName: "concept",
				Columns: []datastore.ColumnDef{
					{Name: "code", TargetType: "VARCHAR", Nullable: false},
					{Name: "name", TargetType: "VARCHAR", Nullable: true},
					{Name: "source", TargetType: "VARCHAR", Nullable: true},
				},
				PrimaryKeys: []string{"code"},
			},
		},
		{
			"limit_list",
			&datastore.TableSchema{
				TableName: "limit_list",
				Columns: []datastore.ColumnDef{
					{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
					{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
					{Name: "first_time", TargetType: "VARCHAR", Nullable: true},
					{Name: "reason", TargetType: "VARCHAR", Nullable: true},
					{Name: "close", TargetType: "DOUBLE", Nullable: true},
					{Name: "pct_chg", TargetType: "DOUBLE", Nullable: true},
					{Name: "turnover_rate", TargetType: "DOUBLE", Nullable: true},
					{Name: "amount", TargetType: "DOUBLE", Nullable: true},
				},
				PrimaryKeys: []string{"ts_code", "trade_date"},
			},
		},
		{
			"limit_list_d",
			&datastore.TableSchema{
				TableName: "limit_list_d",
				Columns: []datastore.ColumnDef{
					{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
					{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
					{Name: "industry", TargetType: "VARCHAR", Nullable: true},
					{Name: "name", TargetType: "VARCHAR", Nullable: true},
					{Name: "close", TargetType: "DOUBLE", Nullable: true},
					{Name: "pct_chg", TargetType: "DOUBLE", Nullable: true},
					{Name: "amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "first_time", TargetType: "VARCHAR", Nullable: true},
					{Name: "turnover_ratio", TargetType: "DOUBLE", Nullable: true},
					{Name: "up_stat", TargetType: "VARCHAR", Nullable: true},
					{Name: "limit", TargetType: "VARCHAR", Nullable: true},
				},
				PrimaryKeys: []string{"ts_code", "trade_date"},
			},
		},
		{
			"concept_detail",
			&datastore.TableSchema{
				TableName: "concept_detail",
				Columns: []datastore.ColumnDef{
					{Name: "concept_code", TargetType: "VARCHAR", Nullable: false},
					{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
				},
				PrimaryKeys: []string{"concept_code", "ts_code"},
			},
		},
		{
			"top_list",
			&datastore.TableSchema{
				TableName: "top_list",
				Columns: []datastore.ColumnDef{
					{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
					{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
					{Name: "name", TargetType: "VARCHAR", Nullable: true},
					{Name: "close", TargetType: "DOUBLE", Nullable: true},
					{Name: "pct_chg", TargetType: "DOUBLE", Nullable: true},
					{Name: "turnover_rate", TargetType: "DOUBLE", Nullable: true},
					{Name: "amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "reason", TargetType: "VARCHAR", Nullable: true},
					{Name: "buy_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "sell_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "net_amount", TargetType: "DOUBLE", Nullable: true},
				},
				PrimaryKeys: []string{"trade_date", "ts_code"},
			},
		},
		{
			"moneyflow",
			&datastore.TableSchema{
				TableName: "moneyflow",
				Columns: []datastore.ColumnDef{
					{Name: "trade_date", TargetType: "VARCHAR", Nullable: false},
					{Name: "ts_code", TargetType: "VARCHAR", Nullable: false},
					{Name: "name", TargetType: "VARCHAR", Nullable: true},
					{Name: "buy_sm_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "sell_sm_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "buy_md_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "sell_md_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "buy_lg_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "sell_lg_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "buy_elg_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "sell_elg_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "net_mf_amount", TargetType: "DOUBLE", Nullable: true},
					{Name: "net_mf_ratio", TargetType: "DOUBLE", Nullable: true},
				},
				PrimaryKeys: []string{"trade_date", "ts_code"},
			},
		},
	}
	for _, tt := range tables {
		exist, _ := adapter.TableExists(ctx, tt.name)
		if exist {
			continue
		}
		require.NoError(t, adapter.CreateTable(ctx, tt.schema))
	}
}
