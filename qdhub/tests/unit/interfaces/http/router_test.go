package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
	httpapi "qdhub/internal/interfaces/http"
)

// Compile-time interface checks
var _ contracts.MetadataApplicationService = (*FullMockMetadataService)(nil)
var _ contracts.DataStoreApplicationService = (*FullMockDataStoreService)(nil)
var _ contracts.SyncApplicationService = (*FullMockSyncService)(nil)
var _ contracts.WorkflowApplicationService = (*FullMockWorkflowService)(nil)

// FullMockMetadataService implements MetadataApplicationService.
type FullMockMetadataService struct{}

func (m *FullMockMetadataService) CreateDataSource(ctx context.Context, req contracts.CreateDataSourceRequest) (*metadata.DataSource, error) {
	return metadata.NewDataSource(req.Name, req.Description, req.BaseURL, req.DocURL), nil
}
func (m *FullMockMetadataService) GetDataSource(ctx context.Context, id shared.ID) (*metadata.DataSource, error) {
	return nil, shared.NewDomainError(shared.ErrCodeNotFound, "not found", nil)
}
func (m *FullMockMetadataService) ListDataSources(ctx context.Context) ([]*metadata.DataSource, error) {
	return []*metadata.DataSource{}, nil
}
func (m *FullMockMetadataService) ParseAndImportMetadata(ctx context.Context, req contracts.ParseMetadataRequest) (*contracts.ParseMetadataResult, error) {
	return &contracts.ParseMetadataResult{}, nil
}
func (m *FullMockMetadataService) SaveToken(ctx context.Context, req contracts.SaveTokenRequest) error {
	return nil
}
func (m *FullMockMetadataService) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	return nil, nil
}
func (m *FullMockMetadataService) ValidateDataSourceToken(ctx context.Context, dataSourceID shared.ID) (hasToken bool, valid bool, message string, err error) {
	return false, false, "not implemented", nil
}
func (m *FullMockMetadataService) GetDataSourceConfig(ctx context.Context, dataSourceID shared.ID) (apiURL string, token string, err error) {
	return "", "", nil
}
func (m *FullMockMetadataService) UpdateDataSourceCommonDataAPIs(ctx context.Context, dataSourceID shared.ID, req contracts.UpdateDataSourceCommonDataAPIsRequest) error {
	return nil
}
func (m *FullMockMetadataService) CreateAPISyncStrategy(ctx context.Context, req contracts.CreateAPISyncStrategyRequest) (*metadata.APISyncStrategy, error) {
	return nil, nil
}
func (m *FullMockMetadataService) GetAPISyncStrategy(ctx context.Context, req contracts.GetAPISyncStrategyRequest) (*metadata.APISyncStrategy, error) {
	return nil, nil
}
func (m *FullMockMetadataService) UpdateAPISyncStrategy(ctx context.Context, id shared.ID, req contracts.UpdateAPISyncStrategyRequest) error {
	return nil
}
func (m *FullMockMetadataService) DeleteAPISyncStrategy(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockMetadataService) ListAPISyncStrategies(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error) {
	return nil, nil
}
func (m *FullMockMetadataService) ListAPIMetadata(ctx context.Context, dataSourceID shared.ID, req contracts.ListAPIMetadataRequest) (*contracts.ListAPIMetadataResponse, error) {
	return &contracts.ListAPIMetadataResponse{Items: nil, Total: 0}, nil
}
func (m *FullMockMetadataService) ListAPICategories(ctx context.Context, dataSourceID shared.ID, hasAPIsOnly bool) ([]metadata.APICategory, error) {
	return []metadata.APICategory{}, nil
}
func (m *FullMockMetadataService) ListAPINames(ctx context.Context, dataSourceID shared.ID) ([]string, error) {
	return []string{}, nil
}
func (m *FullMockMetadataService) DeleteDataSource(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockMetadataService) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	return nil
}

// FullMockDataStoreService implements DataStoreApplicationService.
type FullMockDataStoreService struct{}

func (m *FullMockDataStoreService) CreateDataStore(ctx context.Context, req contracts.CreateDataStoreRequest) (*datastore.QuantDataStore, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) GetDataStore(ctx context.Context, id shared.ID) (*datastore.QuantDataStore, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) UpdateDataStore(ctx context.Context, id shared.ID, req contracts.UpdateDataStoreRequest) (*datastore.QuantDataStore, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) DeleteDataStore(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockDataStoreService) ValidateDataStore(ctx context.Context, id shared.ID) (*contracts.ValidateDataStoreResult, error) {
	return &contracts.ValidateDataStoreResult{Valid: true}, nil
}
func (m *FullMockDataStoreService) CreateTablesForDatasource(ctx context.Context, req contracts.CreateTablesForDatasourceRequest) (shared.ID, error) {
	return "", nil
}
func (m *FullMockDataStoreService) ListDatastoreTables(ctx context.Context, id shared.ID) ([]string, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) GetDatastoreTableData(ctx context.Context, id shared.ID, tableName string, page, pageSize int, searchQ, searchColumn, orderBy, order string) ([]map[string]any, int64, error) {
	return nil, 0, nil
}

// FullMockSyncService implements SyncApplicationService.
type FullMockSyncService struct{}

func (m *FullMockSyncService) CreateSyncPlan(ctx context.Context, req contracts.CreateSyncPlanRequest) (*sync.SyncPlan, error) {
	return nil, nil
}
func (m *FullMockSyncService) GetSyncPlan(ctx context.Context, id shared.ID) (*sync.SyncPlan, error) {
	return nil, nil
}
func (m *FullMockSyncService) UpdateSyncPlan(ctx context.Context, id shared.ID, req contracts.UpdateSyncPlanRequest) error {
	return nil
}
func (m *FullMockSyncService) DeleteSyncPlan(ctx context.Context, id shared.ID) error { return nil }
func (m *FullMockSyncService) ListSyncPlans(ctx context.Context) ([]*sync.SyncPlan, error) {
	return nil, nil
}
func (m *FullMockSyncService) ResolveSyncPlan(ctx context.Context, planID shared.ID) error {
	return nil
}
func (m *FullMockSyncService) ExecuteSyncPlan(ctx context.Context, planID shared.ID, req contracts.ExecuteSyncPlanRequest) (shared.ID, error) {
	return "", nil
}
func (m *FullMockSyncService) GetSyncExecution(ctx context.Context, id shared.ID) (*sync.SyncExecution, error) {
	return nil, nil
}
func (m *FullMockSyncService) ListPlanExecutions(ctx context.Context, planID shared.ID) ([]*sync.SyncExecution, error) {
	return nil, nil
}
func (m *FullMockSyncService) GetPlanSummary(ctx context.Context, planID shared.ID) (*contracts.PlanSummary, error) {
	return nil, nil
}
func (m *FullMockSyncService) ListPlanExecutionHistory(ctx context.Context, planID shared.ID, limit, offset int) ([]*sync.SyncExecution, int, error) {
	return nil, 0, nil
}
func (m *FullMockSyncService) CancelExecution(ctx context.Context, executionID shared.ID) error {
	return nil
}
func (m *FullMockSyncService) PauseExecution(ctx context.Context, executionID shared.ID) error {
	return nil
}
func (m *FullMockSyncService) ResumeExecution(ctx context.Context, executionID shared.ID) error {
	return nil
}
func (m *FullMockSyncService) EnablePlan(ctx context.Context, planID shared.ID) error  { return nil }
func (m *FullMockSyncService) DisablePlan(ctx context.Context, planID shared.ID) error { return nil }
func (m *FullMockSyncService) UpdatePlanSchedule(ctx context.Context, planID shared.ID, cronExpression string) error {
	return nil
}
func (m *FullMockSyncService) HandleExecutionCallback(ctx context.Context, req contracts.ExecutionCallbackRequest) error {
	return nil
}
func (m *FullMockSyncService) HandleExecutionCallbackByWorkflowInstance(ctx context.Context, workflowInstID string, success bool, recordCount int64, errMsg *string) error {
	return nil
}
func (m *FullMockSyncService) ReconcileRunningWindow(ctx context.Context) error { return nil }

func (m *FullMockSyncService) GetExecutionProgress(ctx context.Context, executionID shared.ID) (*contracts.SyncExecutionProgress, error) {
	return &contracts.SyncExecutionProgress{}, nil
}

func (m *FullMockSyncService) GetPlanProgress(ctx context.Context, planID shared.ID) (*contracts.SyncExecutionProgress, error) {
	return &contracts.SyncExecutionProgress{}, nil
}

func (m *FullMockSyncService) RecordTaskResult(ctx context.Context, workflowInstID, apiName, taskID string, recordCount int64, success bool, errorMessage string) error {
	return nil
}
func (m *FullMockSyncService) GetExecutionDetail(ctx context.Context, executionID shared.ID) (*contracts.ExecutionDetail, error) {
	return &contracts.ExecutionDetail{}, nil
}

func (m *FullMockSyncService) ExecuteNewsRealtimeOnce(ctx context.Context) error {
	return nil
}

// FullMockWorkflowService implements WorkflowApplicationService.
type FullMockWorkflowService struct{}

func (m *FullMockWorkflowService) GetWorkflowInstance(ctx context.Context, id shared.ID) (*workflow.WorkflowInstance, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) ListWorkflowInstances(ctx context.Context, workflowDefID shared.ID, status *workflow.WfInstStatus) ([]*workflow.WorkflowInstance, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) GetWorkflowStatus(ctx context.Context, instanceID shared.ID) (*workflow.WorkflowStatus, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) CancelWorkflow(ctx context.Context, instanceID shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error) {
	return nil, nil
}

func TestNewServer(t *testing.T) {
	config := httpapi.DefaultServerConfig()
	config.Mode = "test"

	server := httpapi.NewServer(
		config,
		nil, &FullMockMetadataService{},
		&FullMockDataStoreService{},
		nil,
		&FullMockSyncService{},
		&FullMockWorkflowService{},
		nil, nil, nil, nil, nil, nil, nil, nil, "",
	)

	assert.NotNil(t, server)
	assert.NotNil(t, server.Engine())
}

func TestHealthCheck(t *testing.T) {
	config := httpapi.DefaultServerConfig()
	config.Mode = "test"

	server := httpapi.NewServer(
		config,
		nil, &FullMockMetadataService{},
		&FullMockDataStoreService{},
		nil,
		&FullMockSyncService{},
		&FullMockWorkflowService{},
		nil, nil, nil, nil, nil, nil, nil, nil, "",
	)

	req, _ := http.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	server.Engine().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "healthy")
}

func TestServerShutdown(t *testing.T) {
	config := httpapi.DefaultServerConfig()
	config.Mode = "test"
	config.Port = 18080 // Use different port for test

	server := httpapi.NewServer(
		config,
		nil, &FullMockMetadataService{},
		&FullMockDataStoreService{},
		nil,
		&FullMockSyncService{},
		&FullMockWorkflowService{},
		nil, nil, nil, nil, nil, nil, nil, nil, "",
	)

	// Test shutdown without starting
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := server.Shutdown(ctx)
	assert.NoError(t, err)
}

func TestDefaultServerConfig(t *testing.T) {
	config := httpapi.DefaultServerConfig()

	assert.Equal(t, "0.0.0.0", config.Host)
	assert.Equal(t, 8080, config.Port)
	assert.Equal(t, 30*time.Second, config.ReadTimeout)
	assert.Equal(t, time.Duration(0), config.WriteTimeout, "WriteTimeout 0 for SSE long-lived streams")
}

func TestAPIRoutes(t *testing.T) {
	config := httpapi.DefaultServerConfig()
	config.Mode = "test"

	server := httpapi.NewServer(
		config,
		nil, &FullMockMetadataService{},
		&FullMockDataStoreService{},
		nil,
		&FullMockSyncService{},
		&FullMockWorkflowService{},
		nil, nil, nil, nil, nil, nil, nil, nil, "",
	)

	// Test that all routes are registered
	routes := server.Engine().Routes()
	assert.True(t, len(routes) > 0)

	// Check some specific routes exist
	routePaths := make(map[string]bool)
	for _, r := range routes {
		routePaths[r.Method+" "+r.Path] = true
	}

	assert.True(t, routePaths["GET /health"])
	assert.True(t, routePaths["GET /api/v1/datasources"])
	assert.True(t, routePaths["GET /api/v1/datastores"])
	assert.True(t, routePaths["GET /api/v1/sync-plans"])
	assert.True(t, routePaths["GET /api/v1/instances"])
}
