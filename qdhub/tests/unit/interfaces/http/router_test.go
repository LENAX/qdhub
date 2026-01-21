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
func (m *FullMockMetadataService) UpdateDataSource(ctx context.Context, id shared.ID, req contracts.UpdateDataSourceRequest) error {
	return nil
}
func (m *FullMockMetadataService) DeleteDataSource(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockMetadataService) ListDataSources(ctx context.Context) ([]*metadata.DataSource, error) {
	return []*metadata.DataSource{}, nil
}
func (m *FullMockMetadataService) ParseAndImportMetadata(ctx context.Context, req contracts.ParseMetadataRequest) (*contracts.ParseMetadataResult, error) {
	return &contracts.ParseMetadataResult{}, nil
}
func (m *FullMockMetadataService) CreateAPIMetadata(ctx context.Context, req contracts.CreateAPIMetadataRequest) (*metadata.APIMetadata, error) {
	return nil, nil
}
func (m *FullMockMetadataService) GetAPIMetadata(ctx context.Context, id shared.ID) (*metadata.APIMetadata, error) {
	return nil, nil
}
func (m *FullMockMetadataService) UpdateAPIMetadata(ctx context.Context, id shared.ID, req contracts.UpdateAPIMetadataRequest) error {
	return nil
}
func (m *FullMockMetadataService) DeleteAPIMetadata(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockMetadataService) ListAPIMetadataByDataSource(ctx context.Context, dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	return nil, nil
}
func (m *FullMockMetadataService) SaveToken(ctx context.Context, req contracts.SaveTokenRequest) error {
	return nil
}
func (m *FullMockMetadataService) GetToken(ctx context.Context, dataSourceID shared.ID) (*metadata.Token, error) {
	return nil, nil
}
func (m *FullMockMetadataService) DeleteToken(ctx context.Context, dataSourceID shared.ID) error {
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
func (m *FullMockDataStoreService) UpdateDataStore(ctx context.Context, id shared.ID, req contracts.UpdateDataStoreRequest) error {
	return nil
}
func (m *FullMockDataStoreService) DeleteDataStore(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockDataStoreService) ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) TestConnection(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockDataStoreService) GenerateTableSchema(ctx context.Context, req contracts.GenerateSchemaRequest) (*datastore.TableSchema, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) CreateTable(ctx context.Context, schemaID shared.ID) error {
	return nil
}
func (m *FullMockDataStoreService) DropTable(ctx context.Context, schemaID shared.ID) error {
	return nil
}
func (m *FullMockDataStoreService) GetTableSchema(ctx context.Context, id shared.ID) (*datastore.TableSchema, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) GetTableSchemaByAPI(ctx context.Context, apiMetadataID shared.ID) (*datastore.TableSchema, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) ListTableSchemas(ctx context.Context, dataStoreID shared.ID) ([]*datastore.TableSchema, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) UpdateTableSchema(ctx context.Context, id shared.ID, req contracts.UpdateSchemaRequest) error {
	return nil
}
func (m *FullMockDataStoreService) SyncSchemaStatus(ctx context.Context, dataStoreID shared.ID) error {
	return nil
}
func (m *FullMockDataStoreService) CreateMappingRule(ctx context.Context, req contracts.CreateMappingRuleRequest) (*datastore.DataTypeMappingRule, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) GetMappingRules(ctx context.Context, dataSourceType, targetDBType string) ([]*datastore.DataTypeMappingRule, error) {
	return nil, nil
}
func (m *FullMockDataStoreService) CreateTablesForDatasource(ctx context.Context, req contracts.CreateTablesForDatasourceRequest) (shared.ID, error) {
	return "", nil
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
func (m *FullMockSyncService) CancelExecution(ctx context.Context, executionID shared.ID) error {
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
func (m *FullMockSyncService) SyncDataSource(ctx context.Context, req contracts.SyncDataSourceRequest) (shared.ID, error) {
	return "", nil
}
func (m *FullMockSyncService) SyncDataSourceRealtime(ctx context.Context, req contracts.SyncDataSourceRealtimeRequest) (shared.ID, error) {
	return "", nil
}

// FullMockWorkflowService implements WorkflowApplicationService.
type FullMockWorkflowService struct{}

func (m *FullMockWorkflowService) CreateWorkflowDefinition(ctx context.Context, req contracts.CreateWorkflowDefinitionRequest) (*workflow.WorkflowDefinition, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) GetWorkflowDefinition(ctx context.Context, id shared.ID) (*workflow.WorkflowDefinition, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) UpdateWorkflowDefinition(ctx context.Context, id shared.ID, req contracts.UpdateWorkflowDefinitionRequest) error {
	return nil
}
func (m *FullMockWorkflowService) DeleteWorkflowDefinition(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) ListWorkflowDefinitions(ctx context.Context, category *workflow.WfCategory) ([]*workflow.WorkflowDefinition, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) EnableWorkflow(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) DisableWorkflow(ctx context.Context, id shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) ExecuteWorkflow(ctx context.Context, req contracts.ExecuteWorkflowRequest) (shared.ID, error) {
	return "", nil
}
func (m *FullMockWorkflowService) GetWorkflowInstance(ctx context.Context, id shared.ID) (*workflow.WorkflowInstance, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) ListWorkflowInstances(ctx context.Context, workflowDefID shared.ID, status *workflow.WfInstStatus) ([]*workflow.WorkflowInstance, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) GetWorkflowStatus(ctx context.Context, instanceID shared.ID) (*workflow.WorkflowStatus, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) PauseWorkflow(ctx context.Context, instanceID shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) ResumeWorkflow(ctx context.Context, instanceID shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) CancelWorkflow(ctx context.Context, instanceID shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) SyncWithEngine(ctx context.Context, instanceID shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) SyncAllInstances(ctx context.Context) error { return nil }
func (m *FullMockWorkflowService) GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error) {
	return nil, nil
}
func (m *FullMockWorkflowService) RetryTask(ctx context.Context, taskInstanceID shared.ID) error {
	return nil
}
func (m *FullMockWorkflowService) ExecuteBuiltInWorkflowByName(ctx context.Context, name string, req contracts.ExecuteWorkflowRequest) (shared.ID, error) {
	return "", nil
}

func TestNewServer(t *testing.T) {
	config := httpapi.DefaultServerConfig()
	config.Mode = "test"

	server := httpapi.NewServer(
		config,
		&FullMockMetadataService{},
		&FullMockDataStoreService{},
		&FullMockSyncService{},
		&FullMockWorkflowService{},
	)

	assert.NotNil(t, server)
	assert.NotNil(t, server.Engine())
}

func TestHealthCheck(t *testing.T) {
	config := httpapi.DefaultServerConfig()
	config.Mode = "test"

	server := httpapi.NewServer(
		config,
		&FullMockMetadataService{},
		&FullMockDataStoreService{},
		&FullMockSyncService{},
		&FullMockWorkflowService{},
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
		&FullMockMetadataService{},
		&FullMockDataStoreService{},
		&FullMockSyncService{},
		&FullMockWorkflowService{},
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
	assert.Equal(t, 30*time.Second, config.WriteTimeout)
}

func TestAPIRoutes(t *testing.T) {
	config := httpapi.DefaultServerConfig()
	config.Mode = "test"

	server := httpapi.NewServer(
		config,
		&FullMockMetadataService{},
		&FullMockDataStoreService{},
		&FullMockSyncService{},
		&FullMockWorkflowService{},
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
	assert.True(t, routePaths["GET /api/v1/workflows"])
}
