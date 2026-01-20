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
	// Workflow Definition routes
	rg.POST("/workflows", h.CreateWorkflow)
	rg.GET("/workflows", h.ListWorkflows)
	rg.GET("/workflows/:id", h.GetWorkflow)
	rg.PUT("/workflows/:id", h.UpdateWorkflow)
	rg.DELETE("/workflows/:id", h.DeleteWorkflow)

	// Workflow control
	rg.POST("/workflows/:id/execute", h.ExecuteWorkflow)
	rg.POST("/workflows/:id/enable", h.EnableWorkflow)
	rg.POST("/workflows/:id/disable", h.DisableWorkflow)

	// Built-in workflow shortcuts
	rg.POST("/workflows/built-in/:name/execute", h.ExecuteBuiltInWorkflowByName)

	// Workflow Instance routes
	rg.GET("/instances", h.ListInstances)
	rg.GET("/instances/:id", h.GetInstance)
	rg.GET("/instances/:id/tasks", h.GetTaskInstances)
	rg.GET("/instances/:id/progress", h.GetInstanceProgress)

	// Instance control
	rg.POST("/instances/:id/pause", h.PauseInstance)
	rg.POST("/instances/:id/resume", h.ResumeInstance)
	rg.POST("/instances/:id/cancel", h.CancelInstance)
	rg.POST("/instances/:id/retry", h.RetryTask)

	// Sync
	rg.POST("/instances/:id/sync", h.SyncInstance)
	rg.POST("/workflows/sync-all", h.SyncAllInstances)
}

// ==================== Workflow Definition Endpoints ====================

// CreateWorkflow handles POST /api/v1/workflows
// @Summary      Create a new workflow
// @Description  Create a new workflow definition
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        request  body      CreateWorkflowReq  true  "Workflow definition details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /workflows [post]
func (h *WorkflowHandler) CreateWorkflow(c *gin.Context) {
	var req CreateWorkflowReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	def, err := h.workflowSvc.CreateWorkflowDefinition(c.Request.Context(), contracts.CreateWorkflowDefinitionRequest{
		Name:           req.Name,
		Description:    req.Description,
		Category:       workflow.WfCategory(req.Category),
		DefinitionYAML: req.DefinitionYAML,
		IsSystem:       req.IsSystem,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, def)
}

// ListWorkflows handles GET /api/v1/workflows
// @Summary      List all workflows
// @Description  Get a list of all workflow definitions, optionally filtered by category
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        category  query     string  false  "Filter by category"
// @Success      200       {object}  Response
// @Failure      500       {object}  Response
// @Router       /workflows [get]
func (h *WorkflowHandler) ListWorkflows(c *gin.Context) {
	categoryStr := c.Query("category")

	var category *workflow.WfCategory
	if categoryStr != "" {
		cat := workflow.WfCategory(categoryStr)
		category = &cat
	}

	defs, err := h.workflowSvc.ListWorkflowDefinitions(c.Request.Context(), category)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, defs)
}

// GetWorkflow handles GET /api/v1/workflows/:id
// @Summary      Get a workflow
// @Description  Get details of a specific workflow definition by ID
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow definition ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /workflows/{id} [get]
func (h *WorkflowHandler) GetWorkflow(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	def, err := h.workflowSvc.GetWorkflowDefinition(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, def)
}

// UpdateWorkflow handles PUT /api/v1/workflows/:id
// @Summary      Update a workflow
// @Description  Update details of a specific workflow definition
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Workflow definition ID"
// @Param        request  body      UpdateWorkflowReq true  "Updated workflow details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /workflows/{id} [put]
func (h *WorkflowHandler) UpdateWorkflow(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateWorkflowReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.workflowSvc.UpdateWorkflowDefinition(c.Request.Context(), id, contracts.UpdateWorkflowDefinitionRequest{
		Name:           req.Name,
		Description:    req.Description,
		DefinitionYAML: req.DefinitionYAML,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// DeleteWorkflow handles DELETE /api/v1/workflows/:id
// @Summary      Delete a workflow
// @Description  Delete a specific workflow definition
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow definition ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /workflows/{id} [delete]
func (h *WorkflowHandler) DeleteWorkflow(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.DeleteWorkflowDefinition(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// ==================== Workflow Control Endpoints ====================

// ExecuteWorkflow handles POST /api/v1/workflows/:id/execute
// @Summary      Execute a workflow
// @Description  Execute a workflow definition and create a new instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Workflow definition ID"
// @Param        request  body      ExecuteWorkflowReq false "Execution parameters (optional)"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /workflows/{id}/execute [post]
func (h *WorkflowHandler) ExecuteWorkflow(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req ExecuteWorkflowReq
	if err := c.ShouldBindJSON(&req); err != nil {
		// Allow empty body for simple execution
		req = ExecuteWorkflowReq{
			TriggerType: "manual",
		}
	}

	instanceID, err := h.workflowSvc.ExecuteWorkflow(c.Request.Context(), contracts.ExecuteWorkflowRequest{
		WorkflowDefID: id,
		TriggerType:   workflow.TriggerType(req.TriggerType),
		TriggerParams: req.TriggerParams,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{
		"instance_id": instanceID,
		"status":      "started",
	})
}

// EnableWorkflow handles POST /api/v1/workflows/:id/enable
// @Summary      Enable a workflow
// @Description  Enable a workflow definition
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow definition ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /workflows/{id}/enable [post]
func (h *WorkflowHandler) EnableWorkflow(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.EnableWorkflow(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "enabled"})
}

// DisableWorkflow handles POST /api/v1/workflows/:id/disable
// @Summary      Disable a workflow
// @Description  Disable a workflow definition
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow definition ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /workflows/{id}/disable [post]
func (h *WorkflowHandler) DisableWorkflow(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.DisableWorkflow(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "disabled"})
}

// ExecuteBuiltInWorkflowByName handles POST /api/v1/workflows/built-in/:name/execute
// @Summary      Execute built-in workflow by name
// @Description  Execute a built-in workflow by its API name (shortcut)
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        name       path      string            true  "Built-in workflow name (e.g., metadata_crawl)"
// @Param        request    body      ExecuteWorkflowReq false "Execution parameters"
// @Success      200        {object}  Response
// @Failure      400        {object}  Response
// @Failure      404        {object}  Response
// @Failure      500        {object}  Response
// @Router       /workflows/built-in/{name}/execute [post]
func (h *WorkflowHandler) ExecuteBuiltInWorkflowByName(c *gin.Context) {
	name := c.Param("name")

	var req ExecuteWorkflowReq
	if err := c.ShouldBindJSON(&req); err != nil {
		// Allow empty body for simple execution
		req = ExecuteWorkflowReq{
			TriggerType: "manual",
		}
	}

	instanceID, err := h.workflowSvc.ExecuteBuiltInWorkflowByName(c.Request.Context(), name, contracts.ExecuteWorkflowRequest{
		TriggerType:   workflow.TriggerType(req.TriggerType),
		TriggerParams: req.TriggerParams,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{
		"instance_id": instanceID,
		"status":      "started",
	})
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

// PauseInstance handles POST /api/v1/instances/:id/pause
// @Summary      Pause workflow instance
// @Description  Pause a running workflow instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow instance ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /instances/{id}/pause [post]
func (h *WorkflowHandler) PauseInstance(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.PauseWorkflow(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "paused"})
}

// ResumeInstance handles POST /api/v1/instances/:id/resume
// @Summary      Resume workflow instance
// @Description  Resume a paused workflow instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow instance ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /instances/{id}/resume [post]
func (h *WorkflowHandler) ResumeInstance(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.ResumeWorkflow(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "resumed"})
}

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

// RetryTask handles POST /api/v1/instances/:id/retry
// @Summary      Retry task
// @Description  Retry a failed task instance
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id       path      string       true  "Workflow instance ID"
// @Param        request  body      RetryTaskReq true  "Task retry details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /instances/{id}/retry [post]
func (h *WorkflowHandler) RetryTask(c *gin.Context) {
	var req RetryTaskReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.workflowSvc.RetryTask(c.Request.Context(), shared.ID(req.TaskInstanceID))
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "retrying"})
}

// ==================== Sync Endpoints ====================

// SyncInstance handles POST /api/v1/instances/:id/sync
// @Summary      Sync workflow instance
// @Description  Synchronize a workflow instance with the workflow engine
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Workflow instance ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /instances/{id}/sync [post]
func (h *WorkflowHandler) SyncInstance(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.SyncWithEngine(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "synced"})
}

// SyncAllInstances handles POST /api/v1/workflows/sync-all
// @Summary      Sync all instances
// @Description  Synchronize all workflow instances with the workflow engine
// @Tags         Workflows
// @Accept       json
// @Produce      json
// @Success      200  {object}  Response
// @Failure      500  {object}  Response
// @Router       /workflows/sync-all [post]
func (h *WorkflowHandler) SyncAllInstances(c *gin.Context) {
	err := h.workflowSvc.SyncAllInstances(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "synced"})
}

// ==================== Request DTOs ====================

// CreateWorkflowReq represents the request body for creating a workflow.
type CreateWorkflowReq struct {
	Name           string `json:"name" binding:"required"`
	Description    string `json:"description"`
	Category       string `json:"category" binding:"required"` // metadata, sync, datastore
	DefinitionYAML string `json:"definition_yaml" binding:"required"`
	IsSystem       bool   `json:"is_system"`
}

// UpdateWorkflowReq represents the request body for updating a workflow.
type UpdateWorkflowReq struct {
	Name           *string `json:"name"`
	Description    *string `json:"description"`
	DefinitionYAML *string `json:"definition_yaml"`
}

// ExecuteWorkflowReq represents the request body for executing a workflow.
type ExecuteWorkflowReq struct {
	TriggerType   string                 `json:"trigger_type"` // manual, scheduled, event
	TriggerParams map[string]interface{} `json:"trigger_params"`
}

// RetryTaskReq represents the request body for retrying a task.
type RetryTaskReq struct {
	TaskInstanceID string `json:"task_instance_id" binding:"required"`
}
