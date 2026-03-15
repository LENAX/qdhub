package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
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
	rg.POST("/sync-plans/:id/execute", h.ExecuteSyncPlan)
	rg.POST("/sync-plans/:id/enable", h.EnablePlan)
	rg.POST("/sync-plans/:id/disable", h.DisablePlan)

	// Plan progress
	rg.GET("/sync-plans/:id/progress", h.GetPlanProgress)
	rg.GET("/sync-plans/:id/progress-stream", h.StreamPlanProgress)

	// Plan summary and history
	rg.GET("/sync-plans/:id/summary", h.GetPlanSummary)
	rg.GET("/sync-plans/:id/history", h.GetPlanHistory)

	// Execution management
	rg.GET("/sync-plans/:id/executions", h.ListExecutions)
	rg.GET("/executions/:id", h.GetExecution)
	rg.GET("/executions/:id/detail", h.GetExecutionDetail)
	rg.POST("/executions/:id/cancel", h.CancelExecution)
	rg.POST("/executions/:id/pause", h.PauseExecution)
	rg.POST("/executions/:id/resume", h.ResumeExecution)

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
// @Security     BearerAuth
// @Router       /sync-plans [post]
func (h *SyncHandler) CreateSyncPlan(c *gin.Context) {
	var req CreateSyncPlanReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	mode := sync.PlanMode(req.PlanMode)
	if mode == "" {
		mode = sync.PlanModeBatch
	}

	plan, err := h.syncSvc.CreateSyncPlan(c.Request.Context(), contracts.CreateSyncPlanRequest{
		Name:                       req.Name,
		Description:                req.Description,
		DataSourceID:               shared.ID(req.DataSourceID),
		DataStoreID:                shared.ID(req.DataStoreID),
		SelectedAPIs:               req.SelectedAPIs,
		CronExpression:             req.CronExpression,
		DefaultExecuteParams:       req.DefaultExecuteParams,
		IncrementalMode:            req.IncrementalMode,
		IncrementalStartDateAPI:    req.IncrementalStartDateAPI,
		IncrementalStartDateColumn: req.IncrementalStartDateColumn,
		PlanMode:                   mode,
		ScheduleStartCron:          req.ScheduleStartCron,
		ScheduleEndCron:            req.ScheduleEndCron,
		SchedulePauseStartCron:     req.SchedulePauseStartCron,
		SchedulePauseEndCron:       req.SchedulePauseEndCron,
		PullIntervalSeconds:        req.PullIntervalSeconds,
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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

	var incrAPI, incrCol *string
	if req.IncrementalStartDateAPI != nil {
		s := *req.IncrementalStartDateAPI
		incrAPI = &s
	}
	if req.IncrementalStartDateColumn != nil {
		s := *req.IncrementalStartDateColumn
		incrCol = &s
	}

	var planMode *sync.PlanMode
	if req.PlanMode != nil {
		m := sync.PlanMode(*req.PlanMode)
		planMode = &m
	}
	err := h.syncSvc.UpdateSyncPlan(c.Request.Context(), id, contracts.UpdateSyncPlanRequest{
		Name:                       req.Name,
		Description:                req.Description,
		DataStoreID:                dataStoreID,
		SelectedAPIs:               req.SelectedAPIs,
		CronExpression:             req.CronExpression,
		DefaultExecuteParams:       req.DefaultExecuteParams,
		IncrementalMode:            req.IncrementalMode,
		IncrementalStartDateAPI:    incrAPI,
		IncrementalStartDateColumn: incrCol,
		PlanMode:                   planMode,
		ScheduleStartCron:          req.ScheduleStartCron,
		ScheduleEndCron:            req.ScheduleEndCron,
		SchedulePauseStartCron:     req.SchedulePauseStartCron,
		SchedulePauseEndCron:       req.SchedulePauseEndCron,
		PullIntervalSeconds:        req.PullIntervalSeconds,
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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

// ExecuteSyncPlan handles POST /api/v1/sync-plans/:id/execute
// @Summary      Execute a sync plan
// @Description  Manually trigger execution of a sync plan. Target DB path is resolved from the sync plan's associated data store. Request body may be empty; start_dt/end_dt are optional.
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Sync plan ID"
// @Param        request  body      ExecuteSyncPlanReq  true  "Execution parameters"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /sync-plans/{id}/execute [post]
func (h *SyncHandler) ExecuteSyncPlan(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req ExecuteSyncPlanReq
	_ = c.ShouldBindJSON(&req) // 允许空 body，未传则使用计划默认参数

	startDate, endDate := parseOptionalDatetimeToDate(req.StartDt, req.EndDt)
	executionID, err := h.syncSvc.ExecuteSyncPlan(c.Request.Context(), id, contracts.ExecuteSyncPlanRequest{
		StartDate: startDate,
		EndDate:   endDate,
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

// parseOptionalDatetimeToDate parses optional start_dt/end_dt (RFC3339 or 2006-01-02) to "20060102".
// Returns empty strings if input is empty.
func parseOptionalDatetimeToDate(startDt, endDt string) (startDate, endDate string) {
	const dateOnly = "20060102"
	for _, s := range []struct {
		in  *string
		out *string
	}{
		{&startDt, &startDate},
		{&endDt, &endDate},
	} {
		if *s.in == "" {
			continue
		}
		var t time.Time
		var err error
		if t, err = time.Parse(time.RFC3339, *s.in); err != nil {
			t, err = time.Parse("2006-01-02", *s.in)
		}
		if err == nil {
			*s.out = t.Format(dateOnly)
		}
	}
	return startDate, endDate
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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

// GetPlanSummary handles GET /api/v1/sync-plans/:id/summary
// @Summary      Get sync plan execution summary
// @Description  Returns the latest execution summary for the plan (status, record_count, synced_apis, skipped_apis). Null when the plan has never been executed.
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /sync-plans/{id}/summary [get]
func (h *SyncHandler) GetPlanSummary(c *gin.Context) {
	planID := shared.ID(c.Param("id"))

	summary, err := h.syncSvc.GetPlanSummary(c.Request.Context(), planID)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, summary)
}

// GetPlanHistory handles GET /api/v1/sync-plans/:id/history
// @Summary      Get sync plan execution history
// @Description  Returns paginated execution history for the plan. Query: limit (default 20), offset (default 0).
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id      path      string  true   "Sync plan ID"
// @Param        limit   query     int     false  "Page size"   default(20)
// @Param        offset  query     int     false  "Offset"
// @Success      200     {object}  Response
// @Failure      404     {object}  Response
// @Failure      500     {object}  Response
// @Security     BearerAuth
// @Router       /sync-plans/{id}/history [get]
func (h *SyncHandler) GetPlanHistory(c *gin.Context) {
	planID := shared.ID(c.Param("id"))

	limit := 20
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	offset := 0
	if o := c.Query("offset"); o != "" {
		if n, err := strconv.Atoi(o); err == nil && n >= 0 {
			offset = n
		}
	}

	items, total, err := h.syncSvc.ListPlanExecutionHistory(c.Request.Context(), planID, limit, offset)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"items": items, "total": total})
}

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
// @Security     BearerAuth
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
// @Security     BearerAuth
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

// GetExecutionDetail handles GET /api/v1/executions/:id/detail
// @Summary      Get sync execution detail
// @Description  Get per-API stats and task-level error details for a sync execution
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Execution ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /executions/{id}/detail [get]
func (h *SyncHandler) GetExecutionDetail(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	detail, err := h.syncSvc.GetExecutionDetail(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, detail)
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
// @Security     BearerAuth
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

// PauseExecution handles POST /api/v1/executions/:id/pause
// @Summary      Pause sync execution
// @Description  Pause a running sync execution (workflow instance)
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Execution ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /executions/{id}/pause [post]
func (h *SyncHandler) PauseExecution(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.PauseExecution(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "paused"})
}

// ResumeExecution handles POST /api/v1/executions/:id/resume
// @Summary      Resume sync execution
// @Description  Resume a paused sync execution
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Execution ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /executions/{id}/resume [post]
func (h *SyncHandler) ResumeExecution(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.ResumeExecution(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "resumed"})
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
// @Security     BearerAuth
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
	Name                       string              `json:"name" binding:"required"`
	Description                string              `json:"description"`
	DataSourceID               string              `json:"data_source_id" binding:"required"`
	DataStoreID                string              `json:"data_store_id"`
	SelectedAPIs               []string            `json:"selected_apis" binding:"required"`
	CronExpression             *string             `json:"cron_expression"`
	DefaultExecuteParams       *sync.ExecuteParams `json:"default_execute_params"`
	IncrementalMode            bool                `json:"incremental_mode"`
	IncrementalStartDateAPI    string              `json:"incremental_start_date_api"`
	IncrementalStartDateColumn string              `json:"incremental_start_date_column"`
	// PlanMode: "batch"（默认）或 "realtime"
	PlanMode               string `json:"plan_mode"`
	ScheduleStartCron      string `json:"schedule_start_cron"`       // 运行时段：启动 cron（仅 realtime）
	ScheduleEndCron        string `json:"schedule_end_cron"`         // 运行时段：停止 cron
	SchedulePauseStartCron string `json:"schedule_pause_start_cron"` // 午休暂停开始 cron，如 11:30
	SchedulePauseEndCron   string `json:"schedule_pause_end_cron"`   // 午休暂停结束 cron，如 13:00
	PullIntervalSeconds    int    `json:"pull_interval_seconds"`     // Pull 间隔（秒），0=默认 60
}

// UpdateSyncPlanReq represents the request body for updating a sync plan.
type UpdateSyncPlanReq struct {
	Name                       *string             `json:"name"`
	Description                *string             `json:"description"`
	DataStoreID                *string             `json:"data_store_id"`
	SelectedAPIs               *[]string           `json:"selected_apis"`
	CronExpression             *string             `json:"cron_expression"`
	DefaultExecuteParams       *sync.ExecuteParams `json:"default_execute_params"`
	IncrementalMode            *bool               `json:"incremental_mode"`
	IncrementalStartDateAPI    *string             `json:"incremental_start_date_api"`
	IncrementalStartDateColumn *string             `json:"incremental_start_date_column"`
	// PlanMode: "batch" 或 "realtime"
	PlanMode               *string `json:"plan_mode"`
	ScheduleStartCron      *string `json:"schedule_start_cron"`
	ScheduleEndCron        *string `json:"schedule_end_cron"`
	SchedulePauseStartCron *string `json:"schedule_pause_start_cron"`
	SchedulePauseEndCron   *string `json:"schedule_pause_end_cron"`
	PullIntervalSeconds    *int    `json:"pull_interval_seconds"`
}

// ExecuteSyncPlanReq represents the request body for triggering a sync plan.
// Only start_dt and end_dt are accepted (datetime, e.g. RFC3339 or YYYY-MM-DD).
// Target DB path is resolved from the plan's associated data store.
type ExecuteSyncPlanReq struct {
	StartDt string `json:"start_dt"` // optional, datetime (RFC3339 or 2006-01-02)
	EndDt   string `json:"end_dt"`   // optional, datetime
}

// ExecutionCallbackReq represents the request body for execution callback.
type ExecutionCallbackReq struct {
	ExecutionID  string  `json:"execution_id" binding:"required"`
	Success      bool    `json:"success"`
	RecordCount  int64   `json:"record_count"`
	ErrorMessage *string `json:"error_message"`
}

// SyncPlanProgressResponse represents aggregated progress information for a sync plan.
// It is the HTTP-level DTO returned by /sync-plans/:id/progress and progress-stream.
type SyncPlanProgressResponse struct {
	PlanID             string `json:"plan_id"`
	ExecutionID        string `json:"execution_id,omitempty"`
	WorkflowInstanceID string `json:"workflow_instance_id,omitempty"`
	Status             string `json:"status"`
	// Schedule window config copied from SyncPlan (mainly for realtime plans).
	ScheduleStartCron      *string    `json:"schedule_start_cron,omitempty"`
	ScheduleEndCron        *string    `json:"schedule_end_cron,omitempty"`
	SchedulePauseStartCron *string    `json:"schedule_pause_start_cron,omitempty"`
	SchedulePauseEndCron   *string    `json:"schedule_pause_end_cron,omitempty"`
	Progress               float64    `json:"progress"`
	TaskCount              int        `json:"task_count"`
	CompletedTask          int        `json:"completed_task"`
	FailedTask             int        `json:"failed_task"`
	RunningCount           int        `json:"running_count"`              // 正在运行的任务数（0 时也返回，与内部一致）
	PendingCount           int        `json:"pending_count"`              // 挂起的任务数（0 时也返回）
	RunningTaskIDs         []string   `json:"running_task_ids,omitempty"` // 正在运行的任务 ID（存储可能滞后）
	PendingTaskIDs         []string   `json:"pending_task_ids,omitempty"` // 挂起的任务 ID（存储可能滞后）
	RecordCount            int64      `json:"record_count"`
	ErrorMessage           *string    `json:"error_message,omitempty"`
	StartedAt              *time.Time `json:"started_at,omitempty"`
	FinishedAt             *time.Time `json:"finished_at,omitempty"`
}

// toSyncPlanProgressResponse converts application-level SyncExecutionProgress to HTTP response DTO.
func toSyncPlanProgressResponse(p *contracts.SyncExecutionProgress) *SyncPlanProgressResponse {
	if p == nil {
		return nil
	}

	resp := &SyncPlanProgressResponse{
		PlanID:                 p.PlanID.String(),
		ExecutionID:            p.ExecutionID.String(),
		WorkflowInstanceID:     p.WorkflowInstanceID.String(),
		Status:                 p.Status.String(),
		ScheduleStartCron:      p.ScheduleStartCron,
		ScheduleEndCron:        p.ScheduleEndCron,
		SchedulePauseStartCron: p.SchedulePauseStartCron,
		SchedulePauseEndCron:   p.SchedulePauseEndCron,
		Progress:               p.Progress,
		TaskCount:              p.TaskCount,
		CompletedTask:          p.CompletedTask,
		FailedTask:             p.FailedTask,
		RunningCount:           p.RunningCount,
		PendingCount:           p.PendingCount,
		RunningTaskIDs:         p.RunningTaskIDs,
		PendingTaskIDs:         p.PendingTaskIDs,
		RecordCount:            p.RecordCount,
		ErrorMessage:           p.ErrorMessage,
	}

	// Convert timestamps to *time.Time (omit zero values)
	if !p.StartedAt.IsZero() {
		tm := p.StartedAt.ToTime()
		resp.StartedAt = &tm
	}
	if p.FinishedAt != nil && !p.FinishedAt.IsZero() {
		tm := p.FinishedAt.ToTime()
		resp.FinishedAt = &tm
	}

	return resp
}

// GetPlanProgress handles GET /api/v1/sync-plans/:id/progress
// @Summary      Get sync plan progress
// @Description  Get aggregated progress of the latest execution for a sync plan
// @Tags         SyncPlans
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /sync-plans/{id}/progress [get]
func (h *SyncHandler) GetPlanProgress(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	progress, err := h.syncSvc.GetPlanProgress(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}

	resp := toSyncPlanProgressResponse(progress)
	Success(c, resp)
}

// StreamPlanProgress handles GET /api/v1/sync-plans/:id/progress-stream
// @Summary      Stream sync plan progress
// @Description  Stream aggregated progress of the latest execution for a sync plan via SSE
// @Tags         SyncPlans
// @Accept       json
// @Produce      text/event-stream
// @Param        id   path      string  true  "Sync plan ID"
// @Success      200  {string}  string  "SSE stream of progress events"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /sync-plans/{id}/progress-stream [get]
func (h *SyncHandler) StreamPlanProgress(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	// Set SSE headers
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		BadRequest(c, "streaming not supported")
		return
	}

	ctx := c.Request.Context()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 先发 keepalive 注释，避免代理/负载均衡因长时间无输出断开连接
			c.Writer.Write([]byte(": keepalive\n\n"))
			flusher.Flush()

			progress, err := h.syncSvc.GetPlanProgress(ctx, id)
			if err != nil {
				c.Writer.Write([]byte("event: error\n"))
				c.Writer.Write([]byte("data: {\"error\":\"" + err.Error() + "\"}\n\n"))
				flusher.Flush()
				return
			}

			resp := toSyncPlanProgressResponse(progress)
			data, err := json.Marshal(resp)
			if err != nil {
				c.Writer.Write([]byte("event: error\n"))
				c.Writer.Write([]byte("data: {\"error\":\"failed to marshal progress\"}\n\n"))
				flusher.Flush()
				return
			}

			c.Writer.Write([]byte("data: "))
			c.Writer.Write(data)
			c.Writer.Write([]byte("\n\n"))
			flusher.Flush()

			// Stop streaming when execution reaches terminal state
			status := resp.Status
			if status == sync.ExecStatusSuccess.String() ||
				status == sync.ExecStatusFailed.String() ||
				status == sync.ExecStatusCancelled.String() {
				return
			}

			time.Sleep(1 * time.Second)
		}
	}
}
