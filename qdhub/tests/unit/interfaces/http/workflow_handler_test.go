package http_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	httpapi "qdhub/internal/interfaces/http"
)

// MockWorkflowService is a mock implementation of WorkflowApplicationService.
type MockWorkflowService struct {
	mock.Mock
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

func (m *MockWorkflowService) CancelWorkflow(ctx context.Context, instanceID shared.ID) error {
	args := m.Called(ctx, instanceID)
	return args.Error(0)
}

func (m *MockWorkflowService) GetTaskInstances(ctx context.Context, workflowInstID shared.ID) ([]*workflow.TaskInstance, error) {
	args := m.Called(ctx, workflowInstID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*workflow.TaskInstance), args.Error(1)
}

func setupWorkflowRouter(mockSvc *MockWorkflowService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := httpapi.NewWorkflowHandler(mockSvc)
	v1 := router.Group("/api/v1")
	handler.RegisterRoutes(v1)
	return router
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

// ==================== Error Scenario Tests ====================

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
