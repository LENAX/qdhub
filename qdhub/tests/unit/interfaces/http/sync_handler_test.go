package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	httpapi "qdhub/internal/interfaces/http"
)

// MockSyncService is a mock implementation of SyncApplicationService.
type MockSyncService struct {
	mock.Mock
}

func (m *MockSyncService) CreateSyncJob(ctx context.Context, req contracts.CreateSyncJobRequest) (*sync.SyncJob, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sync.SyncJob), args.Error(1)
}

func (m *MockSyncService) GetSyncJob(ctx context.Context, id shared.ID) (*sync.SyncJob, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sync.SyncJob), args.Error(1)
}

func (m *MockSyncService) UpdateSyncJob(ctx context.Context, id shared.ID, req contracts.UpdateSyncJobRequest) error {
	args := m.Called(ctx, id, req)
	return args.Error(0)
}

func (m *MockSyncService) DeleteSyncJob(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockSyncService) ListSyncJobs(ctx context.Context) ([]*sync.SyncJob, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*sync.SyncJob), args.Error(1)
}

func (m *MockSyncService) ExecuteSyncJob(ctx context.Context, jobID shared.ID) (shared.ID, error) {
	args := m.Called(ctx, jobID)
	return args.Get(0).(shared.ID), args.Error(1)
}

func (m *MockSyncService) GetSyncExecution(ctx context.Context, id shared.ID) (*sync.SyncExecution, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sync.SyncExecution), args.Error(1)
}

func (m *MockSyncService) ListSyncExecutions(ctx context.Context, jobID shared.ID) ([]*sync.SyncExecution, error) {
	args := m.Called(ctx, jobID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*sync.SyncExecution), args.Error(1)
}

func (m *MockSyncService) CancelExecution(ctx context.Context, executionID shared.ID) error {
	args := m.Called(ctx, executionID)
	return args.Error(0)
}

func (m *MockSyncService) EnableJob(ctx context.Context, jobID shared.ID) error {
	args := m.Called(ctx, jobID)
	return args.Error(0)
}

func (m *MockSyncService) DisableJob(ctx context.Context, jobID shared.ID) error {
	args := m.Called(ctx, jobID)
	return args.Error(0)
}

func (m *MockSyncService) UpdateSchedule(ctx context.Context, jobID shared.ID, cronExpression string) error {
	args := m.Called(ctx, jobID, cronExpression)
	return args.Error(0)
}

func (m *MockSyncService) HandleExecutionCallback(ctx context.Context, req contracts.ExecutionCallbackRequest) error {
	args := m.Called(ctx, req)
	return args.Error(0)
}

func (m *MockSyncService) SyncDataSource(ctx context.Context, req contracts.SyncDataSourceRequest) (shared.ID, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(shared.ID), args.Error(1)
}

func (m *MockSyncService) SyncDataSourceRealtime(ctx context.Context, req contracts.SyncDataSourceRealtimeRequest) (shared.ID, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(shared.ID), args.Error(1)
}

func setupSyncRouter(mockSvc *MockSyncService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := httpapi.NewSyncHandler(mockSvc)
	v1 := router.Group("/api/v1")
	handler.RegisterRoutes(v1)
	return router
}

func TestListSyncJobs(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	jobs := []*sync.SyncJob{
		sync.NewSyncJob("test-job", "Test Job", shared.ID("api-1"), shared.ID("ds-1"), shared.ID("wf-1"), sync.SyncModeBatch),
	}
	mockSvc.On("ListSyncJobs", mock.Anything).Return(jobs, nil)

	req, _ := http.NewRequest("GET", "/api/v1/sync-jobs", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateSyncJob(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	job := sync.NewSyncJob("test-job", "Test Job", shared.ID("api-1"), shared.ID("ds-1"), shared.ID("wf-1"), sync.SyncModeBatch)
	mockSvc.On("CreateSyncJob", mock.Anything, mock.MatchedBy(func(req contracts.CreateSyncJobRequest) bool {
		return req.Name == "test-job"
	})).Return(job, nil)

	body := map[string]interface{}{
		"name":            "test-job",
		"description":     "Test Job",
		"api_metadata_id": "api-1",
		"data_store_id":   "ds-1",
		"workflow_def_id": "wf-1",
		"mode":            "batch",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetSyncJob(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	job := sync.NewSyncJob("test-job", "Test Job", shared.ID("api-1"), shared.ID("ds-1"), shared.ID("wf-1"), sync.SyncModeBatch)
	mockSvc.On("GetSyncJob", mock.Anything, shared.ID("job-1")).Return(job, nil)

	req, _ := http.NewRequest("GET", "/api/v1/sync-jobs/job-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDeleteSyncJob(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("DeleteSyncJob", mock.Anything, shared.ID("job-1")).Return(nil)

	req, _ := http.NewRequest("DELETE", "/api/v1/sync-jobs/job-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestTriggerSyncJob(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("ExecuteSyncJob", mock.Anything, shared.ID("job-1")).Return(shared.ID("exec-1"), nil)

	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs/job-1/trigger", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp httpapi.Response
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, 0, resp.Code)

	mockSvc.AssertExpectations(t)
}

func TestEnableJob(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("EnableJob", mock.Anything, shared.ID("job-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs/job-1/enable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDisableJob(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("DisableJob", mock.Anything, shared.ID("job-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs/job-1/disable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListExecutions(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	executions := []*sync.SyncExecution{
		sync.NewSyncExecution(shared.ID("job-1"), shared.ID("wf-inst-1")),
	}
	mockSvc.On("ListSyncExecutions", mock.Anything, shared.ID("job-1")).Return(executions, nil)

	req, _ := http.NewRequest("GET", "/api/v1/sync-jobs/job-1/executions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCancelExecution(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("CancelExecution", mock.Anything, shared.ID("exec-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/executions/exec-1/cancel", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetExecution(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	exec := sync.NewSyncExecution(shared.ID("job-1"), shared.ID("wf-inst-1"))
	mockSvc.On("GetSyncExecution", mock.Anything, shared.ID("exec-1")).Return(exec, nil)

	req, _ := http.NewRequest("GET", "/api/v1/executions/exec-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestUpdateSyncJob(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("UpdateSyncJob", mock.Anything, shared.ID("job-1"), mock.Anything).Return(nil)

	body := map[string]interface{}{
		"name":        "updated-job",
		"description": "Updated Job",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/sync-jobs/job-1", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestHandleCallback(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("HandleExecutionCallback", mock.Anything, mock.MatchedBy(func(req contracts.ExecutionCallbackRequest) bool {
		return req.ExecutionID == "exec-1" && req.Success == true
	})).Return(nil)

	body := map[string]interface{}{
		"execution_id": "exec-1",
		"success":      true,
		"record_count": 100,
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/sync/callback", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

// ==================== Error Scenario Tests ====================

func TestCreateSyncJobInvalidBody(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	// Missing required fields
	body := map[string]interface{}{
		"description": "Test Job",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateSyncJobServiceError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("CreateSyncJob", mock.Anything, mock.Anything).Return(nil, shared.NewDomainError(shared.ErrCodeConflict, "job already exists", nil))

	body := map[string]interface{}{
		"name":            "test-job",
		"description":     "Test Job",
		"api_metadata_id": "api-1",
		"data_store_id":   "ds-1",
		"workflow_def_id": "wf-1",
		"mode":            "batch",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateSyncJobWithParamRules(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	job := sync.NewSyncJob("test-job", "Test Job", shared.ID("api-1"), shared.ID("ds-1"), shared.ID("wf-1"), sync.SyncModeBatch)
	mockSvc.On("CreateSyncJob", mock.Anything, mock.Anything).Return(job, nil)

	body := map[string]interface{}{
		"name":            "test-job",
		"description":     "Test Job",
		"api_metadata_id": "api-1",
		"data_store_id":   "ds-1",
		"workflow_def_id": "wf-1",
		"mode":            "batch",
		"param_rules": []map[string]interface{}{
			{"param_name": "start_date", "rule_type": "date_range", "rule_config": map[string]interface{}{"days": 30}},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListSyncJobsError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("ListSyncJobs", mock.Anything).Return(nil, errors.New("database error"))

	req, _ := http.NewRequest("GET", "/api/v1/sync-jobs", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetSyncJobError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("GetSyncJob", mock.Anything, shared.ID("job-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/sync-jobs/job-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestUpdateSyncJobInvalidBody(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	req, _ := http.NewRequest("PUT", "/api/v1/sync-jobs/job-1", bytes.NewBuffer([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUpdateSyncJobServiceError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("UpdateSyncJob", mock.Anything, shared.ID("job-1"), mock.Anything).Return(shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil))

	body := map[string]interface{}{
		"name": "updated-job",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/sync-jobs/job-1", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestUpdateSyncJobWithModeAndParamRules(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("UpdateSyncJob", mock.Anything, shared.ID("job-1"), mock.Anything).Return(nil)

	mode := "incremental"
	body := map[string]interface{}{
		"name": "updated-job",
		"mode": mode,
		"param_rules": []map[string]interface{}{
			{"param_name": "start_date", "rule_type": "date_range", "rule_config": map[string]interface{}{"days": 30}},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/sync-jobs/job-1", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDeleteSyncJobError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("DeleteSyncJob", mock.Anything, shared.ID("job-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil))

	req, _ := http.NewRequest("DELETE", "/api/v1/sync-jobs/job-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestTriggerSyncJobError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("ExecuteSyncJob", mock.Anything, shared.ID("job-1")).Return(shared.ID(""), shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs/job-1/trigger", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestEnableJobError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("EnableJob", mock.Anything, shared.ID("job-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs/job-1/enable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDisableJobError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("DisableJob", mock.Anything, shared.ID("job-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/sync-jobs/job-1/disable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListExecutionsError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("ListSyncExecutions", mock.Anything, shared.ID("job-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/sync-jobs/job-1/executions", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetExecutionError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("GetSyncExecution", mock.Anything, shared.ID("exec-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "execution not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/executions/exec-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCancelExecutionError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("CancelExecution", mock.Anything, shared.ID("exec-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "execution not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/executions/exec-1/cancel", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestHandleCallbackInvalidBody(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	// Missing required field
	body := map[string]interface{}{
		"success": true,
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/sync/callback", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCallbackError(t *testing.T) {
	mockSvc := new(MockSyncService)
	router := setupSyncRouter(mockSvc)

	mockSvc.On("HandleExecutionCallback", mock.Anything, mock.Anything).Return(shared.NewDomainError(shared.ErrCodeNotFound, "execution not found", nil))

	body := map[string]interface{}{
		"execution_id": "exec-1",
		"success":      true,
		"record_count": 100,
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/sync/callback", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}
