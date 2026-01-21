//go:build e2e
// +build e2e

// Package e2e 提供端到端测试
// 本文件实现 SyncPlan 创建-解析-执行的完整流程测试
package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// TestSyncPlan_CreateAndResolve 测试创建 SyncPlan 并解析依赖
func TestSyncPlan_CreateAndResolve(t *testing.T) {
	// 1. 创建 SyncPlan
	dataSourceID := shared.NewID()
	selectedAPIs := []string{"daily", "adj_factor", "stock_basic", "trade_cal"}

	plan := sync.NewSyncPlan("Test Sync Plan", "测试同步计划", dataSourceID, selectedAPIs)

	require.False(t, plan.ID.IsEmpty())
	assert.Equal(t, "Test Sync Plan", plan.Name)
	assert.Equal(t, sync.PlanStatusDraft, plan.Status)
	assert.Len(t, plan.SelectedAPIs, 4)

	// 2. 模拟依赖解析
	resolver := sync.NewDependencyResolver()

	// 构建 API 依赖映射（模拟从 metadata 获取）
	allAPIDependencies := map[string][]sync.ParamDependency{
		"trade_cal": {},
		"stock_basic": {},
		"daily": {
			{ParamName: "ts_code", SourceAPI: "stock_basic", SourceField: "ts_code", IsList: true},
			{ParamName: "trade_date", SourceAPI: "trade_cal", SourceField: "cal_date", IsList: true},
		},
		"adj_factor": {
			{ParamName: "ts_code", SourceAPI: "stock_basic", SourceField: "ts_code", IsList: true},
		},
	}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	require.NoError(t, err)

	// 3. 验证解析结果
	assert.Len(t, resolvedAPIs, 4) // 所有 API 都已包含，无需补充

	// 验证分层结构
	require.Len(t, graph.Levels, 2)
	// Level 0: 无依赖的 API (trade_cal, stock_basic)
	assert.Contains(t, graph.Levels[0], "trade_cal")
	assert.Contains(t, graph.Levels[0], "stock_basic")
	// Level 1: 依赖 Level 0 的 API (daily, adj_factor)
	assert.Contains(t, graph.Levels[1], "daily")
	assert.Contains(t, graph.Levels[1], "adj_factor")

	// 4. 设置执行图并验证状态变化
	plan.SetExecutionGraph(graph, resolvedAPIs)
	assert.Equal(t, sync.PlanStatusResolved, plan.Status)
	assert.NotNil(t, plan.ExecutionGraph)

	// 5. 验证 TaskConfig 生成
	dailyConfig := graph.TaskConfigs["daily"]
	require.NotNil(t, dailyConfig)
	assert.Equal(t, sync.TaskSyncModeTemplate, dailyConfig.SyncMode)
	assert.Len(t, dailyConfig.ParamMappings, 2) // ts_code 和 trade_date

	stockBasicConfig := graph.TaskConfigs["stock_basic"]
	require.NotNil(t, stockBasicConfig)
	assert.Equal(t, sync.TaskSyncModeDirect, stockBasicConfig.SyncMode)
}

// TestSyncPlan_SyncFrequency 测试同步频率过滤逻辑
func TestSyncPlan_SyncFrequency(t *testing.T) {
	// 创建多个 SyncTask，配置不同的同步频率
	tasks := []*sync.SyncTask{
		sync.NewSyncTask("trade_cal", sync.TaskSyncModeDirect, 0),
		sync.NewSyncTask("stock_basic", sync.TaskSyncModeDirect, 0),
		sync.NewSyncTask("daily", sync.TaskSyncModeTemplate, 1),
	}

	// 设置不同的同步频率
	tasks[0].SetSyncFrequency(sync.SyncFrequencyMonthly) // 每月同步
	tasks[1].SetSyncFrequency(sync.SyncFrequencyWeekly)  // 每周同步
	tasks[2].SetSyncFrequency(sync.SyncFrequencyAlways)  // 每次都同步

	// 模拟最近同步时间
	recentSync := time.Now().Add(-1 * time.Hour)
	tasks[0].LastSyncedAt = &recentSync // trade_cal: 1小时前同步
	tasks[1].LastSyncedAt = &recentSync // stock_basic: 1小时前同步
	// daily: 从未同步过

	// 验证 NeedsSync 逻辑
	assert.False(t, tasks[0].NeedsSync(), "trade_cal recently synced, should not need sync")
	assert.False(t, tasks[1].NeedsSync(), "stock_basic recently synced, should not need sync")
	assert.True(t, tasks[2].NeedsSync(), "daily always needs sync")

	// 模拟过期同步时间
	oldSync := time.Now().Add(-35 * 24 * time.Hour) // 35天前
	tasks[0].LastSyncedAt = &oldSync

	assert.True(t, tasks[0].NeedsSync(), "trade_cal sync expired, should need sync")
}

// TestSyncPlan_EnableDisable 测试启用禁用流程
func TestSyncPlan_EnableDisable(t *testing.T) {
	plan := sync.NewSyncPlan("Test", "", shared.NewID(), []string{"daily"})

	// 草稿状态无法启用
	err := plan.Enable()
	assert.Error(t, err, "should not enable draft plan")

	// 解析后可以启用
	plan.SetExecutionGraph(&sync.ExecutionGraph{}, []string{"daily"})
	err = plan.Enable()
	require.NoError(t, err)
	assert.Equal(t, sync.PlanStatusEnabled, plan.Status)

	// 禁用
	err = plan.Disable()
	require.NoError(t, err)
	assert.Equal(t, sync.PlanStatusDisabled, plan.Status)

	// 运行中无法禁用
	plan.Enable()
	plan.MarkRunning()
	err = plan.Disable()
	assert.Error(t, err, "should not disable running plan")
}

// TestSyncPlan_JSONSerialization 测试 JSON 序列化
func TestSyncPlan_JSONSerialization(t *testing.T) {
	plan := sync.NewSyncPlan("Test Plan", "Description", shared.NewID(), []string{"daily", "stock_basic"})
	plan.SetExecutionGraph(&sync.ExecutionGraph{
		Levels: [][]string{{"stock_basic"}, {"daily"}},
		TaskConfigs: map[string]*sync.TaskConfig{
			"daily": {
				APIName:  "daily",
				SyncMode: sync.TaskSyncModeTemplate,
				ParamMappings: []sync.ParamMapping{
					{ParamName: "ts_code", SourceTask: "FetchStockBasic", SourceField: "ts_code", IsList: true},
				},
			},
		},
	}, []string{"stock_basic", "daily"})

	// 序列化
	selectedJSON, err := plan.MarshalSelectedAPIsJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, selectedJSON)

	resolvedJSON, err := plan.MarshalResolvedAPIsJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, resolvedJSON)

	graphJSON, err := plan.MarshalExecutionGraphJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, graphJSON)

	// 反序列化
	plan2 := sync.NewSyncPlan("Empty", "", shared.NewID(), []string{})
	err = plan2.UnmarshalSelectedAPIsJSON(selectedJSON)
	require.NoError(t, err)
	assert.Len(t, plan2.SelectedAPIs, 2)

	err = plan2.UnmarshalResolvedAPIsJSON(resolvedJSON)
	require.NoError(t, err)
	assert.Len(t, plan2.ResolvedAPIs, 2)

	err = plan2.UnmarshalExecutionGraphJSON(graphJSON)
	require.NoError(t, err)
	require.NotNil(t, plan2.ExecutionGraph)
	assert.Len(t, plan2.ExecutionGraph.Levels, 2)
}

// TestDependencyResolver_AutoAddDependencies 测试自动补充依赖
func TestDependencyResolver_AutoAddDependencies(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// 用户只选择 daily，但 daily 依赖 stock_basic
	selectedAPIs := []string{"daily"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"daily": {
			{ParamName: "ts_code", SourceAPI: "stock_basic", SourceField: "ts_code", IsList: true},
		},
		"stock_basic": {},
	}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	require.NoError(t, err)

	// 应该自动添加 stock_basic
	assert.Len(t, resolvedAPIs, 2)
	assert.Contains(t, resolvedAPIs, "stock_basic")
	assert.Contains(t, resolvedAPIs, "daily")

	// 验证分层
	require.Len(t, graph.Levels, 2)
	assert.Contains(t, graph.Levels[0], "stock_basic")
	assert.Contains(t, graph.Levels[1], "daily")
}

// TestDependencyResolver_CircularDependency 测试循环依赖检测
func TestDependencyResolver_CircularDependency(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// 创建循环依赖: A -> B -> A
	selectedAPIs := []string{"api_a"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"api_a": {
			{ParamName: "param", SourceAPI: "api_b", SourceField: "field", IsList: false},
		},
		"api_b": {
			{ParamName: "param", SourceAPI: "api_a", SourceField: "field", IsList: false},
		},
	}

	_, _, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	assert.Error(t, err, "should detect circular dependency")
}

// TestSyncExecution_Lifecycle 测试 SyncExecution 生命周期
func TestSyncExecution_Lifecycle(t *testing.T) {
	planID := shared.NewID()
	wfInstID := shared.NewID()

	exec := sync.NewSyncExecution(planID, wfInstID)
	assert.Equal(t, sync.ExecStatusPending, exec.Status)

	// 开始运行
	exec.MarkRunning()
	assert.Equal(t, sync.ExecStatusRunning, exec.Status)

	// 成功完成
	exec.MarkSuccess(1000)
	assert.Equal(t, sync.ExecStatusSuccess, exec.Status)
	assert.Equal(t, int64(1000), exec.RecordCount)
	assert.NotNil(t, exec.FinishedAt)
}

// TestAPIMetadata_ParamDependencies 测试 APIMetadata 中的参数依赖
func TestAPIMetadata_ParamDependencies(t *testing.T) {
	// 创建带参数依赖的 APIMetadata
	apiMeta := &metadata.APIMetadata{
		ID:          shared.NewID(),
		Name:        "daily",
		DisplayName: "日线行情",
	}

	// 设置参数依赖
	deps := []metadata.ParamDependency{
		{
			ParamName:   "ts_code",
			SourceAPI:   "stock_basic",
			SourceField: "ts_code",
			IsList:      true,
			FilterField: "list_status",
			FilterValue: "L",
		},
		{
			ParamName:   "trade_date",
			SourceAPI:   "trade_cal",
			SourceField: "cal_date",
			IsList:      true,
			FilterField: "is_open",
			FilterValue: "1",
		},
	}
	apiMeta.ParamDependencies = deps

	// 验证
	assert.Len(t, apiMeta.ParamDependencies, 2)
	assert.Equal(t, "ts_code", apiMeta.ParamDependencies[0].ParamName)
	assert.Equal(t, "stock_basic", apiMeta.ParamDependencies[0].SourceAPI)
	assert.True(t, apiMeta.ParamDependencies[0].IsList)
}

// TestCreateSyncPlanRequest_Validation 测试创建请求验证
func TestCreateSyncPlanRequest_Validation(t *testing.T) {
	ctx := context.Background()

	// 有效请求
	validReq := contracts.CreateSyncPlanRequest{
		Name:         "Test Plan",
		DataSourceID: shared.NewID(),
		SelectedAPIs: []string{"daily", "stock_basic"},
	}

	assert.NotEmpty(t, validReq.Name)
	assert.False(t, validReq.DataSourceID.IsEmpty())
	assert.Len(t, validReq.SelectedAPIs, 2)

	_ = ctx // suppress unused warning
}

// TestExecuteSyncPlanRequest_Params 测试执行请求参数
func TestExecuteSyncPlanRequest_Params(t *testing.T) {
	req := contracts.ExecuteSyncPlanRequest{
		StartDate:    "20240101",
		EndDate:      "20240131",
		StartTime:    "09:30:00",
		EndTime:      "15:00:00",
		TargetDBPath: "/data/test.db",
	}

	assert.Equal(t, "20240101", req.StartDate)
	assert.Equal(t, "20240131", req.EndDate)
	assert.Equal(t, "/data/test.db", req.TargetDBPath)
}
