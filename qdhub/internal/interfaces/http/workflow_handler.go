package http

import (
	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// WorkflowHandler handles workflow-related HTTP requests.
type WorkflowHandler struct {
	workflowSvc contracts.WorkflowApplicationService
}

// NewWorkflowHandler creates a new WorkflowHandler.
func NewWorkflowHandler(workflowSvc contracts.WorkflowApplicationService) *WorkflowHandler {
	return &WorkflowHandler{
		workflowSvc: workflowSvc,
	}
}

// RegisterRoutes registers workflow routes to the router group.
func (h *WorkflowHandler) RegisterRoutes(rg *gin.RouterGroup) {
	// Workflow Instance routes
	rg.GET("/instances", h.ListInstances)
	rg.GET("/instances/:id", h.GetInstance)
	rg.GET("/instances/:id/tasks", h.GetTaskInstances)
	rg.GET("/instances/:id/progress", h.GetInstanceProgress)

	// Instance control
	rg.POST("/instances/:id/cancel", h.CancelInstance)
}

// ==================== Workflow Instance Endpoints ====================

// ListInstances handles GET /api/v1/instances
// @Summary      List workflow instances
// @Description  Get a list of workflow instances, optionally filtered by status
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        workflow_id  query     string  true  "Workflow definition ID"
// @Param        status       query     string  false "Filter by instance status"
// @Success      200          {object}  Response
// @Failure      400          {object}  Response
// @Failure      500          {object}  Response
// @Router       /instances [get]
func (h *WorkflowHandler) ListInstances(c *gin.Context) {
	workflowDefID := c.Query("workflow_id")
	statusStr := c.Query("status")

	if workflowDefID == "" {
		BadRequest(c, "workflow_id is required")
		return
	}

	var status *workflow.WfInstStatus
	if statusStr != "" {
		s := workflow.WfInstStatus(statusStr)
		status = &s
	}

	instances, err := h.workflowSvc.ListWorkflowInstances(c.Request.Context(), shared.ID(workflowDefID), status)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, instances)
}

// GetInstance handles GET /api/v1/instances/:id
// @Summary      Get workflow instance
// @Description  Get details of a specific workflow instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow instance ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /instances/{id} [get]
func (h *WorkflowHandler) GetInstance(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	inst, err := h.workflowSvc.GetWorkflowInstance(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, inst)
}

// GetTaskInstances handles GET /api/v1/instances/:id/tasks
// @Summary      Get task instances
// @Description  Get all task instances for a workflow instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow instance ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /instances/{id}/tasks [get]
func (h *WorkflowHandler) GetTaskInstances(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	tasks, err := h.workflowSvc.GetTaskInstances(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, tasks)
}

// GetInstanceProgress handles GET /api/v1/instances/:id/progress
// @Summary      Get instance progress
// @Description  Get the progress status of a workflow instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow instance ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /instances/{id}/progress [get]
func (h *WorkflowHandler) GetInstanceProgress(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	status, err := h.workflowSvc.GetWorkflowStatus(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, status)
}

// ==================== Instance Control Endpoints ====================

// CancelInstance handles POST /api/v1/instances/:id/cancel
// @Summary      Cancel workflow instance
// @Description  Cancel a running or paused workflow instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow instance ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /instances/{id}/cancel [post]
func (h *WorkflowHandler) CancelInstance(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.CancelWorkflow(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "cancelled"})
}
