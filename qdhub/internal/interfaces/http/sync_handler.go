package http

import (
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
	// Sync Job routes
	rg.POST("/sync-jobs", h.CreateSyncJob)
	rg.GET("/sync-jobs", h.ListSyncJobs)
	rg.GET("/sync-jobs/:id", h.GetSyncJob)
	rg.PUT("/sync-jobs/:id", h.UpdateSyncJob)
	rg.DELETE("/sync-jobs/:id", h.DeleteSyncJob)

	// Job control
	rg.POST("/sync-jobs/:id/trigger", h.TriggerSyncJob)
	rg.POST("/sync-jobs/:id/enable", h.EnableJob)
	rg.POST("/sync-jobs/:id/disable", h.DisableJob)

	// Execution management
	rg.GET("/sync-jobs/:id/executions", h.ListExecutions)
	rg.GET("/executions/:id", h.GetExecution)
	rg.POST("/executions/:id/cancel", h.CancelExecution)

	// Callback (for internal use by workflow engine)
	rg.POST("/sync/callback", h.HandleCallback)
}

// ==================== Sync Job Endpoints ====================

// CreateSyncJob handles POST /api/v1/sync-jobs
// @Summary      Create a new sync job
// @Description  Create a sync job to synchronize data from an API to a data store
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        request  body      CreateSyncJobReq  true  "Sync job details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /sync-jobs [post]
func (h *SyncHandler) CreateSyncJob(c *gin.Context) {
	var req CreateSyncJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	job, err := h.syncSvc.CreateSyncJob(c.Request.Context(), contracts.CreateSyncJobRequest{
		Name:           req.Name,
		Description:    req.Description,
		APIMetadataID:  shared.ID(req.APIMetadataID),
		DataStoreID:    shared.ID(req.DataStoreID),
		WorkflowDefID:  shared.ID(req.WorkflowDefID),
		Mode:           sync.SyncMode(req.Mode),
		CronExpression: req.CronExpression,
		Params:         req.Params,
		ParamRules:     convertParamRules(req.ParamRules),
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, job)
}

// ListSyncJobs handles GET /api/v1/sync-jobs
// @Summary      List all sync jobs
// @Description  Get a list of all sync jobs
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Success      200  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-jobs [get]
func (h *SyncHandler) ListSyncJobs(c *gin.Context) {
	jobs, err := h.syncSvc.ListSyncJobs(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, jobs)
}

// GetSyncJob handles GET /api/v1/sync-jobs/:id
// @Summary      Get a sync job
// @Description  Get details of a specific sync job by ID
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync job ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-jobs/{id} [get]
func (h *SyncHandler) GetSyncJob(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	job, err := h.syncSvc.GetSyncJob(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, job)
}

// UpdateSyncJob handles PUT /api/v1/sync-jobs/:id
// @Summary      Update a sync job
// @Description  Update details of a specific sync job
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Sync job ID"
// @Param        request  body      UpdateSyncJobReq  true  "Updated sync job details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /sync-jobs/{id} [put]
func (h *SyncHandler) UpdateSyncJob(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateSyncJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	var mode *sync.SyncMode
	if req.Mode != nil {
		m := sync.SyncMode(*req.Mode)
		mode = &m
	}

	var paramRules *[]sync.ParamRule
	if req.ParamRules != nil {
		rules := convertParamRules(*req.ParamRules)
		paramRules = &rules
	}

	err := h.syncSvc.UpdateSyncJob(c.Request.Context(), id, contracts.UpdateSyncJobRequest{
		Name:           req.Name,
		Description:    req.Description,
		Mode:           mode,
		CronExpression: req.CronExpression,
		Params:         req.Params,
		ParamRules:     paramRules,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// DeleteSyncJob handles DELETE /api/v1/sync-jobs/:id
// @Summary      Delete a sync job
// @Description  Delete a specific sync job
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync job ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-jobs/{id} [delete]
func (h *SyncHandler) DeleteSyncJob(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.DeleteSyncJob(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// ==================== Job Control Endpoints ====================

// TriggerSyncJob handles POST /api/v1/sync-jobs/:id/trigger
// @Summary      Trigger a sync job
// @Description  Manually trigger execution of a sync job
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync job ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-jobs/{id}/trigger [post]
func (h *SyncHandler) TriggerSyncJob(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	executionID, err := h.syncSvc.ExecuteSyncJob(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{
		"execution_id": executionID,
		"status":       "triggered",
	})
}

// EnableJob handles POST /api/v1/sync-jobs/:id/enable
// @Summary      Enable a sync job
// @Description  Enable a sync job for scheduled execution
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync job ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-jobs/{id}/enable [post]
func (h *SyncHandler) EnableJob(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.EnableJob(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "enabled"})
}

// DisableJob handles POST /api/v1/sync-jobs/:id/disable
// @Summary      Disable a sync job
// @Description  Disable a sync job to stop scheduled execution
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync job ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-jobs/{id}/disable [post]
func (h *SyncHandler) DisableJob(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.syncSvc.DisableJob(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "disabled"})
}

// ==================== Execution Endpoints ====================

// ListExecutions handles GET /api/v1/sync-jobs/:id/executions
// @Summary      List sync executions
// @Description  Get a list of all executions for a sync job
// @Tags         SyncJobs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Sync job ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /sync-jobs/{id}/executions [get]
func (h *SyncHandler) ListExecutions(c *gin.Context) {
	jobID := shared.ID(c.Param("id"))

	executions, err := h.syncSvc.ListSyncExecutions(c.Request.Context(), jobID)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, executions)
}

// GetExecution handles GET /api/v1/executions/:id
// @Summary      Get sync execution
// @Description  Get details of a specific sync execution
// @Tags         SyncJobs
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
// @Tags         SyncJobs
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
// @Tags         SyncJobs
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

// CreateSyncJobReq represents the request body for creating a sync job.
type CreateSyncJobReq struct {
	Name           string                 `json:"name" binding:"required"`
	Description    string                 `json:"description"`
	APIMetadataID  string                 `json:"api_metadata_id" binding:"required"`
	DataStoreID    string                 `json:"data_store_id" binding:"required"`
	WorkflowDefID  string                 `json:"workflow_def_id" binding:"required"`
	Mode           string                 `json:"mode" binding:"required"` // full, incremental
	CronExpression *string                `json:"cron_expression"`
	Params         map[string]interface{} `json:"params"`
	ParamRules     []ParamRuleReq         `json:"param_rules"`
}

// UpdateSyncJobReq represents the request body for updating a sync job.
type UpdateSyncJobReq struct {
	Name           *string                 `json:"name"`
	Description    *string                 `json:"description"`
	Mode           *string                 `json:"mode"`
	CronExpression *string                 `json:"cron_expression"`
	Params         *map[string]interface{} `json:"params"`
	ParamRules     *[]ParamRuleReq         `json:"param_rules"`
}

// ParamRuleReq represents a parameter generation rule in request.
type ParamRuleReq struct {
	ParamName  string      `json:"param_name"`
	RuleType   string      `json:"rule_type"` // date_range, list, fixed
	RuleConfig interface{} `json:"rule_config"`
}

// ExecutionCallbackReq represents the request body for execution callback.
type ExecutionCallbackReq struct {
	ExecutionID  string  `json:"execution_id" binding:"required"`
	Success      bool    `json:"success"`
	RecordCount  int64   `json:"record_count"`
	ErrorMessage *string `json:"error_message"`
}

// convertParamRules converts request param rules to domain param rules.
func convertParamRules(reqRules []ParamRuleReq) []sync.ParamRule {
	rules := make([]sync.ParamRule, len(reqRules))
	for i, r := range reqRules {
		rules[i] = sync.ParamRule{
			ParamName:  r.ParamName,
			RuleType:   r.RuleType,
			RuleConfig: r.RuleConfig,
		}
	}
	return rules
}
