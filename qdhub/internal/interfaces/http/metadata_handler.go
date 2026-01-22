package http

import (
	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// MetadataHandler handles metadata-related HTTP requests.
type MetadataHandler struct {
	metadataSvc contracts.MetadataApplicationService
}

// NewMetadataHandler creates a new MetadataHandler.
func NewMetadataHandler(metadataSvc contracts.MetadataApplicationService) *MetadataHandler {
	return &MetadataHandler{
		metadataSvc: metadataSvc,
	}
}

// RegisterRoutes registers metadata routes to the router group.
func (h *MetadataHandler) RegisterRoutes(rg *gin.RouterGroup) {
	// Data Source routes
	rg.GET("/datasources", h.ListDataSources)
	rg.POST("/datasources", h.CreateDataSource)
	rg.GET("/datasources/:id", h.GetDataSource)

	// Metadata refresh
	rg.POST("/datasources/:id/refresh", h.RefreshMetadata)

	// Token management
	rg.POST("/datasources/:id/token", h.SetToken)
	rg.GET("/datasources/:id/token", h.GetToken)

	// API Sync Strategy management
	rg.POST("/datasources/:id/api-sync-strategies", h.CreateAPISyncStrategy)
	rg.GET("/datasources/:id/api-sync-strategies", h.ListAPISyncStrategies)
	rg.GET("/api-sync-strategies/:id", h.GetAPISyncStrategy)
	rg.PUT("/api-sync-strategies/:id", h.UpdateAPISyncStrategy)
	rg.DELETE("/api-sync-strategies/:id", h.DeleteAPISyncStrategy)
}

// ==================== Data Source Endpoints ====================

// ListDataSources handles GET /api/v1/datasources
// @Summary      List all data sources
// @Description  Get a list of all registered data sources
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Success      200  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources [get]
func (h *MetadataHandler) ListDataSources(c *gin.Context) {
	sources, err := h.metadataSvc.ListDataSources(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, sources)
}

// CreateDataSource handles POST /api/v1/datasources
// @Summary      Create a new data source
// @Description  Register a new data source with its connection details
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        request  body      CreateDataSourceReq  true  "Data source details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datasources [post]
func (h *MetadataHandler) CreateDataSource(c *gin.Context) {
	var req CreateDataSourceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	ds, err := h.metadataSvc.CreateDataSource(c.Request.Context(), contracts.CreateDataSourceRequest{
		Name:        req.Name,
		Description: req.Description,
		BaseURL:     req.BaseURL,
		DocURL:      req.DocURL,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, ds)
}

// GetDataSource handles GET /api/v1/datasources/:id
// @Summary      Get a data source
// @Description  Get details of a specific data source by ID
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources/{id} [get]
func (h *MetadataHandler) GetDataSource(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	ds, err := h.metadataSvc.GetDataSource(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, ds)
}

// ==================== Metadata Refresh ====================

// RefreshMetadata handles POST /api/v1/datasources/:id/refresh
// @Summary      Refresh metadata from data source
// @Description  Trigger metadata crawl workflow for the data source. The workflow will fetch documentation from the data source's DocURL and parse it.
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Data source ID"
// @Param        request  body      RefreshMetadataReq false "Request body (optional, workflow will fetch from DocURL)"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datasources/{id}/refresh [post]
func (h *MetadataHandler) RefreshMetadata(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	// Request body is optional - workflow will fetch documentation from data source's DocURL
	var req RefreshMetadataReq
	_ = c.ShouldBindJSON(&req) // Ignore binding errors, as body is optional

	// Note: DocContent and DocType are no longer used - workflow fetches from DocURL
	// They are kept in the request for backward compatibility but will be ignored
	result, err := h.metadataSvc.ParseAndImportMetadata(c.Request.Context(), contracts.ParseMetadataRequest{
		DataSourceID: id,
		DocContent:   req.DocContent,                     // Ignored - workflow fetches from DocURL
		DocType:      metadata.DocumentType(req.DocType), // Ignored - workflow detects type
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, result)
}

// ==================== Token Endpoints ====================

// SetToken handles POST /api/v1/datasources/:id/token
// @Summary      Set data source token
// @Description  Set or update the authentication token for a data source
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id       path      string      true  "Data source ID"
// @Param        request  body      SetTokenReq true  "Token details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datasources/{id}/token [post]
func (h *MetadataHandler) SetToken(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req SetTokenReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.metadataSvc.SaveToken(c.Request.Context(), contracts.SaveTokenRequest{
		DataSourceID: id,
		TokenValue:   req.Token,
		ExpiresAt:    req.ExpiresAt,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// GetToken handles GET /api/v1/datasources/:id/token
// @Summary      Get data source token
// @Description  Get token information for a data source (token value is not returned for security)
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources/{id}/token [get]
func (h *MetadataHandler) GetToken(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	token, err := h.metadataSvc.GetToken(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}

	// Return token info without the actual value for security
	Success(c, gin.H{
		"id":             token.ID,
		"data_source_id": token.DataSourceID,
		"expires_at":     token.ExpiresAt,
		"created_at":     token.CreatedAt,
	})
}

// ==================== API Sync Strategy Endpoints ====================

// CreateAPISyncStrategy handles POST /api/v1/datasources/:id/api-sync-strategies
// @Summary      Create API sync strategy
// @Description  Create a new API sync strategy for a data source
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id       path      string                    true  "Data source ID"
// @Param        request  body      CreateAPISyncStrategyReq  true  "API sync strategy details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datasources/{id}/api-sync-strategies [post]
func (h *MetadataHandler) CreateAPISyncStrategy(c *gin.Context) {
	dataSourceID := shared.ID(c.Param("id"))

	var req CreateAPISyncStrategyReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	strategy, err := h.metadataSvc.CreateAPISyncStrategy(c.Request.Context(), contracts.CreateAPISyncStrategyRequest{
		DataSourceID:     dataSourceID,
		APIName:          req.APIName,
		PreferredParam:   metadata.SyncParamType(req.PreferredParam),
		SupportDateRange: req.SupportDateRange,
		RequiredParams:   req.RequiredParams,
		Dependencies:     req.Dependencies,
		Description:      req.Description,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, strategy)
}

// ListAPISyncStrategies handles GET /api/v1/datasources/:id/api-sync-strategies
// @Summary      List API sync strategies
// @Description  Get all API sync strategies for a data source
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources/{id}/api-sync-strategies [get]
func (h *MetadataHandler) ListAPISyncStrategies(c *gin.Context) {
	dataSourceID := shared.ID(c.Param("id"))

	strategies, err := h.metadataSvc.ListAPISyncStrategies(c.Request.Context(), dataSourceID)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, strategies)
}

// GetAPISyncStrategy handles GET /api/v1/api-sync-strategies/:id
// @Summary      Get API sync strategy
// @Description  Get details of a specific API sync strategy by ID
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "API sync strategy ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /api-sync-strategies/{id} [get]
func (h *MetadataHandler) GetAPISyncStrategy(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	strategy, err := h.metadataSvc.GetAPISyncStrategy(c.Request.Context(), contracts.GetAPISyncStrategyRequest{
		ID: &id,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, strategy)
}

// UpdateAPISyncStrategy handles PUT /api/v1/api-sync-strategies/:id
// @Summary      Update API sync strategy
// @Description  Update details of a specific API sync strategy
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id       path      string                    true  "API sync strategy ID"
// @Param        request  body      UpdateAPISyncStrategyReq  true  "Updated API sync strategy details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /api-sync-strategies/{id} [put]
func (h *MetadataHandler) UpdateAPISyncStrategy(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateAPISyncStrategyReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	var preferredParam *metadata.SyncParamType
	if req.PreferredParam != nil {
		pp := metadata.SyncParamType(*req.PreferredParam)
		preferredParam = &pp
	}

	err := h.metadataSvc.UpdateAPISyncStrategy(c.Request.Context(), id, contracts.UpdateAPISyncStrategyRequest{
		PreferredParam:   preferredParam,
		SupportDateRange: req.SupportDateRange,
		RequiredParams:   req.RequiredParams,
		Dependencies:     req.Dependencies,
		Description:      req.Description,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// DeleteAPISyncStrategy handles DELETE /api/v1/api-sync-strategies/:id
// @Summary      Delete API sync strategy
// @Description  Delete a specific API sync strategy
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "API sync strategy ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /api-sync-strategies/{id} [delete]
func (h *MetadataHandler) DeleteAPISyncStrategy(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.metadataSvc.DeleteAPISyncStrategy(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// ==================== Request DTOs ====================

// CreateDataSourceReq represents the request body for creating a data source.
type CreateDataSourceReq struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	BaseURL     string `json:"base_url"`
	DocURL      string `json:"doc_url"`
}

// RefreshMetadataReq represents the request body for refreshing metadata.
// Note: These fields are optional and will be ignored - the workflow fetches documentation from the data source's DocURL.
type RefreshMetadataReq struct {
	DocContent string `json:"doc_content"` // Optional - workflow fetches from DocURL
	DocType    string `json:"doc_type"`    // Optional - workflow detects type automatically
}

// SetTokenReq represents the request body for setting a token.
type SetTokenReq struct {
	Token     string  `json:"token" binding:"required"`
	ExpiresAt *string `json:"expires_at"`
}

// CreateAPISyncStrategyReq represents the request body for creating an API sync strategy.
type CreateAPISyncStrategyReq struct {
	APIName          string   `json:"api_name" binding:"required"`
	PreferredParam   string   `json:"preferred_param" binding:"required"` // none/trade_date/ts_code
	SupportDateRange bool     `json:"support_date_range"`
	RequiredParams   []string `json:"required_params"`
	Dependencies     []string `json:"dependencies"`
	Description       string   `json:"description"`
}

// UpdateAPISyncStrategyReq represents the request body for updating an API sync strategy.
type UpdateAPISyncStrategyReq struct {
	PreferredParam   *string   `json:"preferred_param"`
	SupportDateRange *bool      `json:"support_date_range"`
	RequiredParams   *[]string `json:"required_params"`
	Dependencies     *[]string  `json:"dependencies"`
	Description       *string    `json:"description"`
}
