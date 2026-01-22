//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/scheduler"
	"qdhub/internal/infrastructure/taskengine"
)

// TestApplicationServices_WorkflowExecutor_Integration tests that application services
// correctly validate preconditions and call WorkflowExecutor methods.
func TestApplicationServices_WorkflowExecutor_Integration(t *testing.T) {
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

	// Create repositories
	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err)

	dataSourceRepo := repository.NewDataSourceRepository(db)
	dataStoreRepo := repository.NewQuantDataStoreRepository(db)
	mappingRuleRepo := repository.NewDataTypeMappingRuleRepository(db)
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)

	// Create adapters
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng)
	workflowFactory := taskengine.GetWorkflowFactory(eng)
	quantDBAdapter := &mockQuantDBAdapter{}
	cronCalculator := scheduler.NewCronSchedulerCalculatorAdapter()

	// Initialize built-in workflows
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err)

	// Create WorkflowExecutor
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo)

	// Create dependency resolver
	dependencyResolver := sync.NewDependencyResolver()

	// Create application services with WorkflowExecutor
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, nil, workflowExecutor)
	dataStoreSvc := impl.NewDataStoreApplicationService(dataStoreRepo, mappingRuleRepo, dataSourceRepo, quantDBAdapter, workflowExecutor)
	syncSvc := impl.NewSyncApplicationService(syncPlanRepo, cronCalculator, nil, dataSourceRepo, workflowExecutor, dependencyResolver)

	// ==================== MetadataApplicationService Tests ====================

	t.Run("MetadataSvc - ParseAndImportMetadata validates data source exists", func(t *testing.T) {
		// Test with non-existent data source
		_, err := metadataSvc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
			DataSourceID: shared.NewID(), // Non-existent
			DocContent:   "<html>test</html>",
			DocType:      metadata.DocumentTypeHTML,
		})
		assert.Error(t, err, "Should return error for non-existent data source")
		assert.Contains(t, err.Error(), "not found", "Error should indicate data source not found")
	})

	t.Run("MetadataSvc - ParseAndImportMetadata triggers workflow for valid data source", func(t *testing.T) {
		// Create a data source
		ds := metadata.NewDataSource("tushare", "Tushare API", "https://api.tushare.pro", "https://tushare.pro/document")
		err := dataSourceRepo.Create(ds)
		require.NoError(t, err)

		// Execute ParseAndImportMetadata
		result, err := metadataSvc.ParseAndImportMetadata(ctx, contracts.ParseMetadataRequest{
			DataSourceID: ds.ID,
			DocContent:   "<html>test</html>",
			DocType:      metadata.DocumentTypeHTML,
		})
		assert.NoError(t, err, "Should not return error for valid data source")
		assert.NotNil(t, result, "Result should not be nil")
		// Result fields are 0 because workflow is async
		assert.Equal(t, 0, result.CategoriesCreated)
		assert.Equal(t, 0, result.APIsCreated)
	})

	// ==================== DataStoreApplicationService Tests ====================

	t.Run("DataStoreSvc - CreateTablesForDatasource validates data source exists", func(t *testing.T) {
		// Create a data store first
		dsStore := datastore.NewQuantDataStore("Test Store", "Test", datastore.DataStoreTypeDuckDB, "", "/tmp/test.duckdb")
		err := dataStoreRepo.Create(dsStore)
		require.NoError(t, err)

		// Test with non-existent data source
		_, err = dataStoreSvc.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: shared.NewID(), // Non-existent
			DataStoreID:  dsStore.ID,
		})
		assert.Error(t, err, "Should return error for non-existent data source")
		assert.Contains(t, err.Error(), "not found", "Error should indicate data source not found")
	})

	t.Run("DataStoreSvc - CreateTablesForDatasource validates data store exists", func(t *testing.T) {
		// Use existing data source from previous test
		sources, _ := dataSourceRepo.List()
		require.NotEmpty(t, sources, "Should have at least one data source")

		// Test with non-existent data store
		_, err = dataStoreSvc.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: sources[0].ID,
			DataStoreID:  shared.NewID(), // Non-existent
		})
		assert.Error(t, err, "Should return error for non-existent data store")
		assert.Contains(t, err.Error(), "not found", "Error should indicate data store not found")
	})

	t.Run("DataStoreSvc - CreateTablesForDatasource triggers workflow for valid inputs", func(t *testing.T) {
		// Get existing data source and data store
		sources, _ := dataSourceRepo.List()
		require.NotEmpty(t, sources)

		stores, _ := dataStoreRepo.List()
		require.NotEmpty(t, stores)

		// Execute CreateTablesForDatasource
		instanceID, err := dataStoreSvc.CreateTablesForDatasource(ctx, contracts.CreateTablesForDatasourceRequest{
			DataSourceID: sources[0].ID,
			DataStoreID:  stores[0].ID,
		})
		assert.NoError(t, err, "Should not return error for valid inputs")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")
	})

	// ==================== SyncApplicationService Tests ====================

	t.Run("SyncSvc - SyncDataSource validates data source exists", func(t *testing.T) {
		_, err := syncSvc.SyncDataSource(ctx, contracts.SyncDataSourceRequest{
			DataSourceID: shared.NewID(), // Non-existent
			TargetDBPath: "/tmp/test.duckdb",
			StartDate:    "20251201",
			EndDate:      "20251231",
			APINames:     []string{"daily"},
		})
		assert.Error(t, err, "Should return error for non-existent data source")
		assert.Contains(t, err.Error(), "not found", "Error should indicate data source not found")
	})

	t.Run("SyncSvc - SyncDataSource validates token exists", func(t *testing.T) {
		// Get existing data source (without token)
		sources, _ := dataSourceRepo.List()
		require.NotEmpty(t, sources)

		_, err := syncSvc.SyncDataSource(ctx, contracts.SyncDataSourceRequest{
			DataSourceID: sources[0].ID,
			TargetDBPath: "/tmp/test.duckdb",
			StartDate:    "20251201",
			EndDate:      "20251231",
			APINames:     []string{"daily"},
		})
		assert.Error(t, err, "Should return error when token not configured")
		assert.Contains(t, err.Error(), "token", "Error should mention token")
	})

	t.Run("SyncSvc - SyncDataSource triggers workflow with valid token", func(t *testing.T) {
		// Get existing data source
		sources, _ := dataSourceRepo.List()
		require.NotEmpty(t, sources)

		// Add token for the data source
		token := metadata.NewToken(sources[0].ID, "test-token-value", nil)
		err := dataSourceRepo.SetToken(token)
		require.NoError(t, err)

		// Execute SyncDataSource
		instanceID, err := syncSvc.SyncDataSource(ctx, contracts.SyncDataSourceRequest{
			DataSourceID: sources[0].ID,
			TargetDBPath: "/tmp/test.duckdb",
			StartDate:    "20251201",
			EndDate:      "20251231",
			APINames:     []string{"daily"},
			MaxStocks:    100,
		})
		assert.NoError(t, err, "Should not return error with valid token")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")
	})

	t.Run("SyncSvc - SyncDataSourceRealtime validates data source exists", func(t *testing.T) {
		_, err := syncSvc.SyncDataSourceRealtime(ctx, contracts.SyncDataSourceRealtimeRequest{
			DataSourceID: shared.NewID(), // Non-existent
			TargetDBPath: "/tmp/test.duckdb",
			APINames:     []string{"realtime_quote"},
		})
		assert.Error(t, err, "Should return error for non-existent data source")
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("SyncSvc - SyncDataSourceRealtime triggers workflow with valid token", func(t *testing.T) {
		// Get existing data source (already has token from previous test)
		sources, _ := dataSourceRepo.List()
		require.NotEmpty(t, sources)

		// Execute SyncDataSourceRealtime
		instanceID, err := syncSvc.SyncDataSourceRealtime(ctx, contracts.SyncDataSourceRealtimeRequest{
			DataSourceID:    sources[0].ID,
			TargetDBPath:    "/tmp/test.duckdb",
			CheckpointTable: "sync_checkpoint",
			APINames:        []string{"realtime_quote"},
			MaxStocks:       50,
		})
		assert.NoError(t, err, "Should not return error with valid token")
		assert.NotEmpty(t, instanceID, "Instance ID should not be empty")
	})

	t.Run("SyncSvc - SyncDataSourceRealtime uses default checkpoint table", func(t *testing.T) {
		sources, _ := dataSourceRepo.List()
		require.NotEmpty(t, sources)

		// Execute with empty checkpoint table
		instanceID, err := syncSvc.SyncDataSourceRealtime(ctx, contracts.SyncDataSourceRealtimeRequest{
			DataSourceID:    sources[0].ID,
			TargetDBPath:    "/tmp/test.duckdb",
			CheckpointTable: "", // Should use default "sync_checkpoint"
			APINames:        []string{"realtime_quote"},
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, instanceID)
	})
}

// mockJobScheduler is a simple mock for testing
type mockJobScheduler struct {
	scheduledJobs map[string]string
}

func (m *mockJobScheduler) Start()                     {}
func (m *mockJobScheduler) Stop() context.Context      { return context.Background() }
func (m *mockJobScheduler) ScheduleJob(jobID string, cronExpr string) error {
	m.scheduledJobs[jobID] = cronExpr
	return nil
}
func (m *mockJobScheduler) UnscheduleJob(jobID string) { delete(m.scheduledJobs, jobID) }
func (m *mockJobScheduler) IsScheduled(jobID string) bool {
	_, ok := m.scheduledJobs[jobID]
	return ok
}
func (m *mockJobScheduler) GetNextRunTime(jobID string) *time.Time { return nil }

// mockQuantDBAdapter is a mock for testing
type mockQuantDBAdapter struct{}

func (m *mockQuantDBAdapter) TestConnection(ctx context.Context, ds *datastore.QuantDataStore) error {
	return nil
}

func (m *mockQuantDBAdapter) ExecuteDDL(ctx context.Context, ds *datastore.QuantDataStore, ddl string) error {
	return nil
}

func (m *mockQuantDBAdapter) TableExists(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (bool, error) {
	return false, nil
}
