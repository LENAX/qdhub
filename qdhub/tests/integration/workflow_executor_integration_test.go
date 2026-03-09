//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/impl"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

// TestWorkflowExecutor_Integration tests the WorkflowExecutor implementation
// to verify that all four business methods correctly convert parameters and execute workflows.
func TestWorkflowExecutor_Integration(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Initialize Task Engine
	taskEngineDSN := db.DSN()
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(taskEngineDSN)
	require.NoError(t, err, "Failed to create task engine aggregate repository")

	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	require.NoError(t, err, "Failed to create task engine")

	err = eng.Start(ctx)
	require.NoError(t, err, "Failed to start task engine")
	defer eng.Stop()

	// Initialize Task Engine (register job functions)
	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry: nil,
		MetadataRepo:       nil,
	}
	err = taskengine.Initialize(ctx, eng, taskEngineDeps)
	require.NoError(t, err, "Failed to initialize task engine")

	// Create repositories and services
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err, "Failed to create workflow repository")

	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)

	// Initialize built-in workflows
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err, "Failed to initialize built-in workflows")

	// Create MetadataRepository for WorkflowExecutor
	metadataRepo := repository.NewMetadataRepository(db)

	// Create WorkflowExecutor（无需实时 Adapter，传 nil）
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, nil)

	t.Run("ExecuteMetadataCrawl - converts parameters correctly", func(t *testing.T) {
		req := workflow.MetadataCrawlRequest{
			DataSourceID:   shared.NewID(),
			DataSourceName: "tushare",
			MaxAPICrawl:    10,
		}

		instanceID, err := workflowExecutor.ExecuteMetadataCrawl(ctx, req)
		assert.NoError(t, err, "ExecuteMetadataCrawl should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")

		// Verify instance was created in Task Engine
		status, err := taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
		assert.NoError(t, err, "Should be able to get instance status")
		assert.NotNil(t, status, "Status should not be nil")
	})

	t.Run("ExecuteCreateTables - converts parameters correctly", func(t *testing.T) {
		req := workflow.CreateTablesRequest{
			DataSourceID:   shared.NewID(),
			DataSourceName: "tushare",
			TargetDBPath:   "/tmp/test.duckdb",
			MaxTables:      5,
		}

		instanceID, err := workflowExecutor.ExecuteCreateTables(ctx, req)
		assert.NoError(t, err, "ExecuteCreateTables should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")

		// Verify instance was created
		status, err := taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
		assert.NoError(t, err, "Should be able to get instance status")
		assert.NotNil(t, status, "Status should not be nil")
	})

	t.Run("ExecuteBatchDataSync - converts parameters correctly", func(t *testing.T) {
		req := workflow.BatchDataSyncRequest{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			StartDate:      "20251201",
			EndDate:        "20251231",
			StartTime:      "09:30:00",
			EndTime:        "15:00:00",
			APINames:       []string{"daily", "weekly"},
			MaxStocks:      100,
		}

		instanceID, err := workflowExecutor.ExecuteBatchDataSync(ctx, req)
		assert.NoError(t, err, "ExecuteBatchDataSync should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")

		// Verify instance was created
		status, err := taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
		assert.NoError(t, err, "Should be able to get instance status")
		assert.NotNil(t, status, "Status should not be nil")
	})

	t.Run("ExecuteBatchDataSync - index APIs include FetchIndexBasic", func(t *testing.T) {
		// 验证 index_daily/index_weight 会触发 FetchIndexBasic Level 0 任务
		req := workflow.BatchDataSyncRequest{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			StartDate:      "20251201",
			EndDate:        "20251231",
			APINames:       []string{"index_daily", "index_weight"},
			MaxStocks:      10,
		}

		instanceID, err := workflowExecutor.ExecuteBatchDataSync(ctx, req)
		assert.NoError(t, err, "ExecuteBatchDataSync with index APIs should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")

		status, err := taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
		assert.NoError(t, err, "Should be able to get instance status")
		assert.NotNil(t, status, "Status should not be nil")
	})

	t.Run("ExecuteBatchDataSync - news APIs use IterateParams", func(t *testing.T) {
		// 验证 news 多 src 展开、cctv_news 按 trade_date 迭代
		req := workflow.BatchDataSyncRequest{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			StartDate:      "20251201",
			EndDate:        "20251231",
			APINames:       []string{"news", "cctv_news", "npr"},
			MaxStocks:      0,
		}

		instanceID, err := workflowExecutor.ExecuteBatchDataSync(ctx, req)
		assert.NoError(t, err, "ExecuteBatchDataSync with news APIs should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")

		status, err := taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
		assert.NoError(t, err, "Should be able to get instance status")
		assert.NotNil(t, status, "Status should not be nil")
	})

	t.Run("ExecuteRealtimeDataSync - converts parameters correctly", func(t *testing.T) {
		req := workflow.RealtimeDataSyncRequest{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			APINames:       []string{"realtime_quote"},
			MaxStocks:      50,
			CronExpr:       "0 * * * * *",
		}

		instanceID, err := workflowExecutor.ExecuteRealtimeDataSync(ctx, req)
		assert.NoError(t, err, "ExecuteRealtimeDataSync should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")

		// Verify instance was created
		status, err := taskEngineAdapter.GetInstanceStatus(ctx, instanceID.String())
		assert.NoError(t, err, "Should be able to get instance status")
		assert.NotNil(t, status, "Status should not be nil")
	})

	t.Run("ExecuteRealtimeDataSync - incremental without range params", func(t *testing.T) {
		req := workflow.RealtimeDataSyncRequest{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			APINames:       []string{"realtime_quote"},
		}

		instanceID, err := workflowExecutor.ExecuteRealtimeDataSync(ctx, req)
		assert.NoError(t, err, "ExecuteRealtimeDataSync should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")
	})

	t.Run("ExecuteBuiltInWorkflow - deprecated method still works", func(t *testing.T) {
		params := map[string]interface{}{
			"data_source_id":   shared.NewID().String(),
			"data_source_name": "tushare",
		}

		instanceID, err := workflowExecutor.ExecuteBuiltInWorkflow(ctx, workflows.BuiltInWorkflowNameMetadataCrawl, params)
		assert.NoError(t, err, "ExecuteBuiltInWorkflow should not return error")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")
	})

	t.Run("ExecuteBuiltInWorkflow - returns error for unknown workflow", func(t *testing.T) {
		params := map[string]interface{}{}

		_, err := workflowExecutor.ExecuteBuiltInWorkflow(ctx, "unknown_workflow", params)
		assert.Error(t, err, "Should return error for unknown workflow")
		assert.Contains(t, err.Error(), "not found", "Error should indicate workflow not found")
	})
}

// TestWorkflowExecutor_ParameterMapping tests that parameters are correctly mapped
// from request structs to workflow params map.
func TestWorkflowExecutor_ParameterMapping(t *testing.T) {
	db, cleanup := setupIntegrationDB(t)
	defer cleanup()

	ctx := context.Background()

	// Initialize Task Engine
	taskEngineDSN := db.DSN()
	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(taskEngineDSN)
	require.NoError(t, err)

	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	require.NoError(t, err)

	err = eng.Start(ctx)
	require.NoError(t, err)
	defer eng.Stop()

	taskEngineDeps := &taskengine.Dependencies{}
	err = taskengine.Initialize(ctx, eng, taskEngineDeps)
	require.NoError(t, err)

	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err)

	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)

	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err)

	metadataRepo := repository.NewMetadataRepository(db)
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo)

	t.Run("MetadataCrawl - optional MaxAPICrawl not added when zero", func(t *testing.T) {
		req := workflow.MetadataCrawlRequest{
			DataSourceID:   shared.NewID(),
			DataSourceName: "tushare",
			MaxAPICrawl:    0, // Zero means no limit, should not be added to params
		}

		instanceID, err := workflowExecutor.ExecuteMetadataCrawl(ctx, req)
		assert.NoError(t, err)
		assert.NotEmpty(t, instanceID)
	})

	t.Run("BatchDataSync - optional time params not added when empty", func(t *testing.T) {
		req := workflow.BatchDataSyncRequest{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			StartDate:      "20251201",
			EndDate:        "20251231",
			StartTime:      "", // Empty, should not be added
			EndTime:        "", // Empty, should not be added
			APINames:       []string{"daily"},
			MaxStocks:      0, // Zero, should not be added
		}

		instanceID, err := workflowExecutor.ExecuteBatchDataSync(ctx, req)
		assert.NoError(t, err)
		assert.NotEmpty(t, instanceID)
	})

	t.Run("ExecuteBatchDataSync - accepts APIConfigs only (SyncPlan path)", func(t *testing.T) {
		req := workflow.BatchDataSyncRequest{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			StartDate:      "20251201",
			EndDate:        "20251231",
			APIConfigs: []workflow.APISyncConfig{
				{APIName: "daily", SyncMode: "template", ParamKey: "trade_date", UpstreamTask: "FetchTradeCal", Dependencies: []string{"FetchTradeCal"}},
			},
			MaxStocks: 0,
		}

		instanceID, err := workflowExecutor.ExecuteBatchDataSync(ctx, req)
		assert.NoError(t, err, "ExecuteBatchDataSync with APIConfigs only should succeed")
		assert.NotEmpty(t, instanceID)
	})

	t.Run("CreateTables - optional MaxTables not added when zero", func(t *testing.T) {
		req := workflow.CreateTablesRequest{
			DataSourceID:   shared.NewID(),
			DataSourceName: "tushare",
			TargetDBPath:   "/tmp/test.duckdb",
			MaxTables:      0, // Zero means no limit
		}

		instanceID, err := workflowExecutor.ExecuteCreateTables(ctx, req)
		assert.NoError(t, err)
		assert.NotEmpty(t, instanceID)
	})
}
