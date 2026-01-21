package http

import (
	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
)

// SyncHandler handles sync-related HTTP requests.
type SyncHandler struct {
	syncSvc contracts.SyncApplicationService
}

// NewSyncHandler creates a new SyncHandler.
func NewSyncHandler(syncSvc contracts.SyncApplicationService) *SyncHandler {
	return &SyncHandler{
		syncSvc: syncSvc,
	}
}

// RegisterRoutes registers sync routes to the router group.
func (h *SyncHandler) RegisterRoutes(rg *gin.RouterGroup) {
	// Sync Plan routes
	rg.POST("/sync-plans", h.CreateSyncPlan)
	rg.GET("/sync-plans", h.ListSyncPlans)
	rg.GET("/sync-plans/:id", h.GetSyncPlan)
	rg.PUT("/sync-plans/:id", h.UpdateSyncPlan)
	rg.DELETE("/sync-plans/:id", h.DeleteSyncPlan)

	// Plan control
	rg.POST("/sync-plans/:id/resolve", h.ResolveSyncPlan)
	rg.POST("/sync-plans/:id/trigger", h.TriggerSyncPlan)
	rg.POST("/sync-plans/:id/enable", h.EnablePlan)
	rg.POST("/sync-plans/:id/disable", h.DisablePlan)

	// Execution management
	rg.GET("/sync-plans/:id/executions", h.ListExecutions)
	rg.GET("/executions/:id", h.GetExecution)
	rg.POST("/executions/:id/cancel", h.CancelExecution)

	// Callback (for internal use by workflow engine)
	rg.POST("/sync/callback", h.HandleCallback)
}

// ==================== Sync Plan Endpoints ====================

// CreateSyncPlan handles POST /api/v1/sync-plans
// @Summary      Create a new sync plan
// @Description  Create a sync plan to synchronize data from APIs to a data store
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        request  body      CreateSyncPlanReq  true  "Sync plan details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /sync-plans [post]
func (h *SyncHandler) CreateSyncPlan(c *gin.Context) {
	var req CreateSyncPlanReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	plan, err := h.syncSvc.CreateSyncPlan(c.Request.Context(), contracts.CreateSyncPlanRequest{
		Name:           req.Name,
		Description:    req.Description,
		DataSourceID:   shared.ID(req.DataSourceID),
		DataStoreID:    shared.ID(req.DataStoreID),
		SelectedAPIs:   req.SelectedAPIs,
		CronExpression: req.CronExpression,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, plan)
}

// ListSyncPlans handles GET /api/v1/sync-plans
// @Summary      List all sync plans
// @Description  Get a list of all sync plans
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Success      200  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans [get]
func (h *SyncHandler) ListSyncPlans(c *gin.Context) {
	plans, err := h.syncSvc.ListSyncPlans(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, plans)
}

// GetSyncPlan handles GET /api/v1/sync-plans/:id
// @Summary      Get a sync plan
// @Description  Get details of a specific sync plan by ID
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans/{id} [get]
func (h *SyncHandler) GetSyncPlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	plan, err := h.syncSvc.GetSyncPlan(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, plan)
}

// UpdateSyncPlan handles PUT /api/v1/sync-plans/:id
// @Summary      Update a sync plan
// @Description  Update details of a specific sync plan
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Sync plan ID"
// @Param        request  body      UpdateSyncPlanReq  true  "Updated sync plan details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /sync-plans/{id} [put]
func (h *SyncHandler) UpdateSyncPlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateSyncPlanReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	var dataStoreID *shared.ID
	if req.DataStoreID != nil {
		id := shared.ID(*req.DataStoreID)
		dataStoreID = &id
	}

	err := h.syncSvc.UpdateSyncPlan(c.Request.Context(), id, contracts.UpdateSyncPlanRequest{
		Name:           req.Name,
		Description:    req.Description,
		DataStoreID:    dataStoreID,
		SelectedAPIs:   req.SelectedAPIs,
		CronExpression: req.CronExpression,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// DeleteSyncPlan handles DELETE /api/v1/sync-plans/:id
// @Summary      Delete a sync plan
// @Description  Delete a specific sync plan
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans/{id} [delete]
func (h *SyncHandler) DeleteSyncPlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.DeleteSyncPlan(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// ==================== Plan Control Endpoints ====================

// ResolveSyncPlan handles POST /api/v1/sync-plans/:id/resolve
// @Summary      Resolve sync plan dependencies
// @Description  Resolve API dependencies for a sync plan
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans/{id}/resolve [post]
func (h *SyncHandler) ResolveSyncPlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.ResolveSyncPlan(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "resolved"})
}

// TriggerSyncPlan handles POST /api/v1/sync-plans/:id/trigger
// @Summary      Trigger a sync plan
// @Description  Manually trigger execution of a sync plan
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Sync plan ID"
// @Param        request  body      TriggerSyncPlanReq  true  "Execution parameters"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans/{id}/trigger [post]
func (h *SyncHandler) TriggerSyncPlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req TriggerSyncPlanReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	executionID, err := h.syncSvc.ExecuteSyncPlan(c.Request.Context(), id, contracts.ExecuteSyncPlanRequest{
		TargetDBPath: req.TargetDBPath,
		StartDate:    req.StartDate,
		EndDate:      req.EndDate,
		StartTime:    req.StartTime,
		EndTime:      req.EndTime,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{
		"execution_id": executionID,
		"status":       "triggered",
	})
}

// EnablePlan handles POST /api/v1/sync-plans/:id/enable
// @Summary      Enable a sync plan
// @Description  Enable a sync plan for scheduled execution
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans/{id}/enable [post]
func (h *SyncHandler) EnablePlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.EnablePlan(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "enabled"})
}

// DisablePlan handles POST /api/v1/sync-plans/:id/disable
// @Summary      Disable a sync plan
// @Description  Disable a sync plan to stop scheduled execution
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans/{id}/disable [post]
func (h *SyncHandler) DisablePlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.DisablePlan(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "disabled"})
}

// ==================== Execution Endpoints ====================

// ListExecutions handles GET /api/v1/sync-plans/:id/executions
// @Summary      List sync executions
// @Description  Get a list of all executions for a sync plan
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-plans/{id}/executions [get]
func (h *SyncHandler) ListExecutions(c *gin.Context) {
	planID := shared.ID(c.Param("id"))

	executions, err := h.syncSvc.ListPlanExecutions(c.Request.Context(), planID)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, executions)
}

// GetExecution handles GET /api/v1/executions/:id
// @Summary      Get sync execution
// @Description  Get details of a specific sync execution
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Execution ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /executions/{id} [get]
func (h *SyncHandler) GetExecution(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	exec, err := h.syncSvc.GetSyncExecution(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, exec)
}

// CancelExecution handles POST /api/v1/executions/:id/cancel
// @Summary      Cancel sync execution
// @Description  Cancel a running sync execution
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Execution ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /executions/{id}/cancel [post]
func (h *SyncHandler) CancelExecution(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.CancelExecution(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "cancelled"})
}

// ==================== Callback Endpoint ====================

// HandleCallback handles POST /api/v1/sync/callback
// @Summary      Handle execution callback
// @Description  Handle callback from workflow engine for sync execution (internal use)
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        request  body      ExecutionCallbackReq true  "Execution callback details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /sync/callback [post]
func (h *SyncHandler) HandleCallback(c *gin.Context) {
	var req ExecutionCallbackReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.syncSvc.HandleExecutionCallback(c.Request.Context(), contracts.ExecutionCallbackRequest{
		ExecutionID:  shared.ID(req.ExecutionID),
		Success:      req.Success,
		RecordCount:  req.RecordCount,
		ErrorMessage: req.ErrorMessage,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// ==================== Request DTOs ====================

// CreateSyncPlanReq represents the request body for creating a sync plan.
type CreateSyncPlanReq struct {
	Name           string   `json:"name" binding:"required"`
	Description    string   `json:"description"`
	DataSourceID   string   `json:"data_source_id" binding:"required"`
	DataStoreID    string   `json:"data_store_id"`
	SelectedAPIs   []string `json:"selected_apis" binding:"required"`
	CronExpression *string  `json:"cron_expression"`
}

// UpdateSyncPlanReq represents the request body for updating a sync plan.
type UpdateSyncPlanReq struct {
	Name           *string   `json:"name"`
	Description    *string   `json:"description"`
	DataStoreID    *string   `json:"data_store_id"`
	SelectedAPIs   *[]string `json:"selected_apis"`
	CronExpression *string   `json:"cron_expression"`
}

// TriggerSyncPlanReq represents the request body for triggering a sync plan.
type TriggerSyncPlanReq struct {
	TargetDBPath string `json:"target_db_path" binding:"required"`
	StartDate    string `json:"start_date" binding:"required"`
	EndDate      string `json:"end_date" binding:"required"`
	StartTime    string `json:"start_time"`
	EndTime      string `json:"end_time"`
}

// ExecutionCallbackReq represents the request body for execution callback.
type ExecutionCallbackReq struct {
	ExecutionID  string  `json:"execution_id" binding:"required"`
	Success      bool    `json:"success"`
	RecordCount  int64   `json:"record_count"`
	ErrorMessage *string `json:"error_message"`
}
