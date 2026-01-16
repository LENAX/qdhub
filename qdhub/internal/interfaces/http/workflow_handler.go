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
func (h *WorkflowHandler) DisableWorkflow(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.workflowSvc.DisableWorkflow(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "disabled"})
}

// ==================== Workflow Instance Endpoints ====================

// ListInstances handles GET /api/v1/instances
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
