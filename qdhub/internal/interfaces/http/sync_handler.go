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
func (h *SyncHandler) ListSyncJobs(c *gin.Context) {
	jobs, err := h.syncSvc.ListSyncJobs(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, jobs)
}

// GetSyncJob handles GET /api/v1/sync-jobs/:id
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
