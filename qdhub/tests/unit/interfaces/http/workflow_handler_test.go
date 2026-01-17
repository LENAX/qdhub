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
	"qdhub/internal/domain/workflow"
	httpapi "qdhub/internal/interfaces/http"
)

// MockWorkflowService is a mock implementation of WorkflowApplicationService.
type MockWorkflowService struct {
	mock.Mock
}

func (m *MockWorkflowService) CreateWorkflowDefinition(ctx context.Context, req contracts.CreateWorkflowDefinitionRequest) (*workflow.WorkflowDefinition, error) {
	args := m.Called(ctx, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*workflow.WorkflowDefinition), args.Error(1)
}

func (m *MockWorkflowService) GetWorkflowDefinition(ctx context.Context, id shared.ID) (*workflow.WorkflowDefinition, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*workflow.WorkflowDefinition), args.Error(1)
}

func (m *MockWorkflowService) UpdateWorkflowDefinition(ctx context.Context, id shared.ID, req contracts.UpdateWorkflowDefinitionRequest) error {
	args := m.Called(ctx, id, req)
	return args.Error(0)
}

func (m *MockWorkflowService) DeleteWorkflowDefinition(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockWorkflowService) ListWorkflowDefinitions(ctx context.Context, category *workflow.WfCategory) ([]*workflow.WorkflowDefinition, error) {
	args := m.Called(ctx, category)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*workflow.WorkflowDefinition), args.Error(1)
}

func (m *MockWorkflowService) EnableWorkflow(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockWorkflowService) DisableWorkflow(ctx context.Context, id shared.ID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockWorkflowService) ExecuteWorkflow(ctx context.Context, req contracts.ExecuteWorkflowRequest) (shared.ID, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(shared.ID), args.Error(1)
}

func (m *MockWorkflowService) GetWorkflowInstance(ctx context.Context, id shared.ID) (*workflow.WorkflowInstance, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*workflow.WorkflowInstance), args.Error(1)
}

func (m *MockWorkflowService) ListWorkflowInstances(ctx context.Context, workflowDefID shared.ID, status *workflow.WfInstStatus) ([]*workflow.WorkflowInstance, error) {
	args := m.Called(ctx, workflowDefID, status)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*workflow.WorkflowInstance), args.Error(1)
}

func (m *MockWorkflowService) GetWorkflowStatus(ctx context.Context, instanceID shared.ID) (*workflow.WorkflowStatus, error) {
	args := m.Called(ctx, instanceID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*workflow.WorkflowStatus), args.Error(1)
}

func (m *MockWorkflowService) PauseWorkflow(ctx context.Context, instanceID shared.ID) error {
	args := m.Called(ctx, instanceID)
	return args.Error(0)
}

func (m *MockWorkflowService) ResumeWorkflow(ctx context.Context, instanceID shared.ID) error {
	args := m.Called(ctx, instanceID)
	return args.Error(0)
}

func (m *MockWorkflowService) CancelWorkflow(ctx context.Context, instanceID shared.ID) error {
	args := m.Called(ctx, instanceID)
	return args.Error(0)
}

func (m *MockWorkflowService) SyncWithEngine(ctx context.Context, instanceID shared.ID) error {
	args := m.Called(ctx, instanceID)
	return args.Error(0)
}

func (m *MockWorkflowService) SyncAllInstances(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockWorkflowService) GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error) {
	args := m.Called(ctx, workflowInstID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*workflow.TaskInstance), args.Error(1)
}

func (m *MockWorkflowService) RetryTask(ctx context.Context, taskInstanceID shared.ID) error {
	args := m.Called(ctx, taskInstanceID)
	return args.Error(0)
}

func (m *MockWorkflowService) ExecuteBuiltInWorkflowByName(ctx context.Context, name string, req contracts.ExecuteWorkflowRequest) (shared.ID, error) {
	args := m.Called(ctx, name, req)
	return args.Get(0).(shared.ID), args.Error(1)
}

func setupWorkflowRouter(mockSvc *MockWorkflowService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := httpapi.NewWorkflowHandler(mockSvc)
	v1 := router.Group("/api/v1")
	handler.RegisterRoutes(v1)
	return router
}

func TestListWorkflows(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	defs := []*workflow.WorkflowDefinition{
		workflow.NewWorkflowDefinition("test-wf", "Test Workflow", workflow.WfCategorySync, "yaml: content", false),
	}
	mockSvc.On("ListWorkflowDefinitions", mock.Anything, (*workflow.WfCategory)(nil)).Return(defs, nil)

	req, _ := http.NewRequest("GET", "/api/v1/workflows", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListWorkflowsWithCategory(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	cat := workflow.WfCategorySync
	defs := []*workflow.WorkflowDefinition{}
	mockSvc.On("ListWorkflowDefinitions", mock.Anything, &cat).Return(defs, nil)

	req, _ := http.NewRequest("GET", "/api/v1/workflows?category=sync", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCreateWorkflow(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	def := workflow.NewWorkflowDefinition("test-wf", "Test Workflow", workflow.WfCategorySync, "yaml: content", false)
	mockSvc.On("CreateWorkflowDefinition", mock.Anything, mock.MatchedBy(func(req contracts.CreateWorkflowDefinitionRequest) bool {
		return req.Name == "test-wf"
	})).Return(def, nil)

	body := map[string]interface{}{
		"name":            "test-wf",
		"description":     "Test Workflow",
		"category":        "sync",
		"definition_yaml": "yaml: content",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/workflows", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetWorkflow(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	def := workflow.NewWorkflowDefinition("test-wf", "Test Workflow", workflow.WfCategorySync, "yaml: content", false)
	mockSvc.On("GetWorkflowDefinition", mock.Anything, shared.ID("wf-1")).Return(def, nil)

	req, _ := http.NewRequest("GET", "/api/v1/workflows/wf-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDeleteWorkflow(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("DeleteWorkflowDefinition", mock.Anything, shared.ID("wf-1")).Return(nil)

	req, _ := http.NewRequest("DELETE", "/api/v1/workflows/wf-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNoContent, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestExecuteWorkflow(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("ExecuteWorkflow", mock.Anything, mock.MatchedBy(func(req contracts.ExecuteWorkflowRequest) bool {
		return req.WorkflowDefID == "wf-1"
	})).Return(shared.ID("inst-1"), nil)

	body := map[string]interface{}{
		"trigger_type": "manual",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/workflows/wf-1/execute", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestEnableWorkflow(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("EnableWorkflow", mock.Anything, shared.ID("wf-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/workflows/wf-1/enable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDisableWorkflow(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("DisableWorkflow", mock.Anything, shared.ID("wf-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/workflows/wf-1/disable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListInstances(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	instances := []*workflow.WorkflowInstance{}
	mockSvc.On("ListWorkflowInstances", mock.Anything, shared.ID("wf-1"), (*workflow.WfInstStatus)(nil)).Return(instances, nil)

	req, _ := http.NewRequest("GET", "/api/v1/instances?workflow_id=wf-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListInstancesMissingWorkflowID(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	req, _ := http.NewRequest("GET", "/api/v1/instances", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPauseInstance(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("PauseWorkflow", mock.Anything, shared.ID("inst-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/pause", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestResumeInstance(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("ResumeWorkflow", mock.Anything, shared.ID("inst-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/resume", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCancelInstance(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("CancelWorkflow", mock.Anything, shared.ID("inst-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/cancel", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestSyncAllInstances(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("SyncAllInstances", mock.Anything).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/workflows/sync-all", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestUpdateWorkflow(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("UpdateWorkflowDefinition", mock.Anything, shared.ID("wf-1"), mock.Anything).Return(nil)

	body := map[string]interface{}{
		"name":        "updated-wf",
		"description": "Updated Workflow",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/workflows/wf-1", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetInstance(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	inst := &workflow.WorkflowInstance{
		ID:     "inst-1",
		Status: "Running",
	}
	mockSvc.On("GetWorkflowInstance", mock.Anything, shared.ID("inst-1")).Return(inst, nil)

	req, _ := http.NewRequest("GET", "/api/v1/instances/inst-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetTaskInstances(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	tasks := []*workflow.TaskInstance{}
	mockSvc.On("GetTaskInstances", mock.Anything, shared.ID("inst-1")).Return(tasks, nil)

	req, _ := http.NewRequest("GET", "/api/v1/instances/inst-1/tasks", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetInstanceProgress(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	status := &workflow.WorkflowStatus{
		Status:   "Running",
		Progress: 50.0,
	}
	mockSvc.On("GetWorkflowStatus", mock.Anything, shared.ID("inst-1")).Return(status, nil)

	req, _ := http.NewRequest("GET", "/api/v1/instances/inst-1/progress", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestRetryTask(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("RetryTask", mock.Anything, shared.ID("task-1")).Return(nil)

	body := map[string]interface{}{
		"task_instance_id": "task-1",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/retry", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestSyncInstance(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("SyncWithEngine", mock.Anything, shared.ID("inst-1")).Return(nil)

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/sync", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

// ==================== Error Scenario Tests ====================

func TestCreateWorkflowInvalidBody(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	// Missing required fields
	body := map[string]interface{}{
		"description": "Test Workflow",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/workflows", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCreateWorkflowServiceError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("CreateWorkflowDefinition", mock.Anything, mock.Anything).Return(nil, shared.NewDomainError(shared.ErrCodeConflict, "workflow already exists", nil))

	body := map[string]interface{}{
		"name":            "test-wf",
		"description":     "Test Workflow",
		"category":        "sync",
		"definition_yaml": "yaml: content",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/workflows", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListWorkflowsError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("ListWorkflowDefinitions", mock.Anything, (*workflow.WfCategory)(nil)).Return(nil, errors.New("database error"))

	req, _ := http.NewRequest("GET", "/api/v1/workflows", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetWorkflowNotFound(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("GetWorkflowDefinition", mock.Anything, shared.ID("not-found")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "workflow not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/workflows/not-found", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestUpdateWorkflowInvalidBody(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	// Invalid JSON
	req, _ := http.NewRequest("PUT", "/api/v1/workflows/wf-1", bytes.NewBuffer([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestUpdateWorkflowServiceError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("UpdateWorkflowDefinition", mock.Anything, shared.ID("wf-1"), mock.Anything).Return(shared.NewDomainError(shared.ErrCodeNotFound, "workflow not found", nil))

	body := map[string]interface{}{
		"name": "updated-wf",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("PUT", "/api/v1/workflows/wf-1", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDeleteWorkflowError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("DeleteWorkflowDefinition", mock.Anything, shared.ID("wf-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "workflow not found", nil))

	req, _ := http.NewRequest("DELETE", "/api/v1/workflows/wf-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestExecuteWorkflowError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("ExecuteWorkflow", mock.Anything, mock.Anything).Return(shared.ID(""), shared.NewDomainError(shared.ErrCodeValidation, "invalid workflow", nil))

	body := map[string]interface{}{
		"trigger_type": "manual",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/workflows/wf-1/execute", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestExecuteWorkflowEmptyBody(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("ExecuteWorkflow", mock.Anything, mock.MatchedBy(func(req contracts.ExecuteWorkflowRequest) bool {
		return req.TriggerType == "manual"
	})).Return(shared.ID("inst-1"), nil)

	// Empty body - should use default trigger_type
	req, _ := http.NewRequest("POST", "/api/v1/workflows/wf-1/execute", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestEnableWorkflowError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("EnableWorkflow", mock.Anything, shared.ID("wf-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "workflow not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/workflows/wf-1/enable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestDisableWorkflowError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("DisableWorkflow", mock.Anything, shared.ID("wf-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "workflow not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/workflows/wf-1/disable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListInstancesWithStatus(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	status := workflow.WfInstStatus("Running")
	instances := []*workflow.WorkflowInstance{}
	mockSvc.On("ListWorkflowInstances", mock.Anything, shared.ID("wf-1"), &status).Return(instances, nil)

	req, _ := http.NewRequest("GET", "/api/v1/instances?workflow_id=wf-1&status=Running", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestListInstancesError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("ListWorkflowInstances", mock.Anything, shared.ID("wf-1"), (*workflow.WfInstStatus)(nil)).Return(nil, errors.New("database error"))

	req, _ := http.NewRequest("GET", "/api/v1/instances?workflow_id=wf-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetInstanceError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("GetWorkflowInstance", mock.Anything, shared.ID("inst-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "instance not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/instances/inst-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetTaskInstancesError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("GetTaskInstances", mock.Anything, shared.ID("inst-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "instance not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/instances/inst-1/tasks", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestGetInstanceProgressError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("GetWorkflowStatus", mock.Anything, shared.ID("inst-1")).Return(nil, shared.NewDomainError(shared.ErrCodeNotFound, "instance not found", nil))

	req, _ := http.NewRequest("GET", "/api/v1/instances/inst-1/progress", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestPauseInstanceError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("PauseWorkflow", mock.Anything, shared.ID("inst-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "instance not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/pause", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestResumeInstanceError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("ResumeWorkflow", mock.Anything, shared.ID("inst-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "instance not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/resume", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestCancelInstanceError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("CancelWorkflow", mock.Anything, shared.ID("inst-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "instance not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/cancel", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestRetryTaskInvalidBody(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	// Missing required field
	body := map[string]interface{}{}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/retry", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestRetryTaskError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("RetryTask", mock.Anything, shared.ID("task-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "task not found", nil))

	body := map[string]interface{}{
		"task_instance_id": "task-1",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/retry", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestSyncInstanceError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("SyncWithEngine", mock.Anything, shared.ID("inst-1")).Return(shared.NewDomainError(shared.ErrCodeNotFound, "instance not found", nil))

	req, _ := http.NewRequest("POST", "/api/v1/instances/inst-1/sync", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockSvc.AssertExpectations(t)
}

func TestSyncAllInstancesError(t *testing.T) {
	mockSvc := new(MockWorkflowService)
	router := setupWorkflowRouter(mockSvc)

	mockSvc.On("SyncAllInstances", mock.Anything).Return(errors.New("sync failed"))

	req, _ := http.NewRequest("POST", "/api/v1/workflows/sync-all", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	mockSvc.AssertExpectations(t)
}
