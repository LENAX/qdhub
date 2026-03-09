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
	"qdhub/internal/infrastructure/persistence/repository"
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
	syncPlanRepo := repository.NewSyncPlanRepository(db)
	metadataRepo := repository.NewMetadataRepository(db)

	// Create adapters
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	workflowFactory := taskengine.GetWorkflowFactory(eng)

	// Initialize built-in workflows
	builtInInitializer := impl.NewBuiltInWorkflowInitializer(workflowRepo, workflowFactory, taskEngineAdapter)
	err = builtInInitializer.Initialize(ctx)
	require.NoError(t, err)

	// Create WorkflowExecutor（集成测试不覆盖实时 Adapter，传 nil）
	workflowExecutor := taskengine.NewWorkflowExecutor(workflowRepo, taskEngineAdapter, metadataRepo, nil)

	// Create application services with WorkflowExecutor
	metadataSvc := impl.NewMetadataApplicationService(dataSourceRepo, metadataRepo, nil, workflowExecutor, nil)
	dataStoreSvc := impl.NewDataStoreApplicationService(dataStoreRepo, dataSourceRepo, syncPlanRepo, workflowExecutor, nil)

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
	// Note: SyncDataSource and SyncDataSourceRealtime methods have been removed.
	// All sync operations should now go through SyncPlan.
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

func (m *mockQuantDBAdapter) ListTables(ctx context.Context, ds *datastore.QuantDataStore) ([]string, error) {
	return nil, nil
}

func (m *mockQuantDBAdapter) Query(ctx context.Context, ds *datastore.QuantDataStore, sql string, args ...any) ([]map[string]any, error) {
	return nil, nil
}
