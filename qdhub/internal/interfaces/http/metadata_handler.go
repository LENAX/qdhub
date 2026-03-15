package http

import (
	"strconv"

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
	rg.DELETE("/datasources/:id", h.DeleteDataSource)
	rg.PUT("/datasources/:id/common-data-apis", h.UpdateDataSourceCommonDataAPIs)

	// Metadata refresh
	rg.POST("/datasources/:id/refresh", h.RefreshMetadata)

	// Token management
	rg.POST("/datasources/:id/token", h.SetToken)
	rg.GET("/datasources/:id/token", h.GetToken)
	rg.GET("/datasources/:id/token/validate", h.ValidateDataSourceToken)
	rg.GET("/datasources/:id/config", h.GetDataSourceConfig)

	// API Metadata (paginated list + delete)
	rg.GET("/datasources/:id/api-metadata", h.ListAPIMetadata)
	rg.GET("/datasources/:id/api-names", h.ListAPINames)
	rg.GET("/datasources/:id/api-categories", h.ListAPICategories)
	rg.DELETE("/datasources/:id/api-metadata/:meta_id", h.DeleteAPIMetadata)

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
// @Failure      401  {object}  Response  "Unauthorized"
// @Failure      500  {object}  Response
// @Security     BearerAuth
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
// @Failure      401      {object}  Response  "Unauthorized"
// @Failure      500      {object}  Response
// @Security     BearerAuth
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
// @Security     BearerAuth
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

// DeleteDataSource handles DELETE /api/v1/datasources/:id
// Cascades: api_sync_strategies, token, api_metadata, api_categories. Admin only (Casbin: datasources delete).
// @Summary      Delete a data source
// @Description  Delete a data source and cascade to api_metadata, api_sync_strategies, api_categories, token. Admin only.
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      204  "No Content"
// @Failure      403  {object}  Response  "Forbidden (non-admin)"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id} [delete]
func (h *MetadataHandler) DeleteDataSource(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	if err := h.metadataSvc.DeleteDataSource(c.Request.Context(), id); err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// UpdateDataSourceCommonDataAPIs handles PUT /api/v1/datasources/:id/common-data-apis
// @Summary      Update common data APIs for a data source
// @Description  Set the list of API names treated as common data (e.g. trade_cal, stock_basic for tushare). Used for cache/DataStore reuse across workflows.
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Param        request  body  UpdateDataSourceCommonDataAPIsReq  true  "common_data_apis array"
// @Success      200  {object}  Response
// @Failure      400  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id}/common-data-apis [put]
func (h *MetadataHandler) UpdateDataSourceCommonDataAPIs(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	var req UpdateDataSourceCommonDataAPIsReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}
	if err := h.metadataSvc.UpdateDataSourceCommonDataAPIs(c.Request.Context(), id, contracts.UpdateDataSourceCommonDataAPIsRequest{
		CommonDataAPIs: req.CommonDataAPIs,
	}); err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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

// ValidateDataSourceToken handles GET /api/v1/datasources/:id/token/validate
// If no token: returns has_token=false, message="未认证". If has token: uses data source adapter to send a test request; success -> valid=true, failure -> valid=false and message with concrete error.
// @Summary      Validate data source token
// @Description  Check if data source has a token and validate it via adapter test request. No token returns unauthenticated; with token runs a test request and returns success or error detail.
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  Response  "has_token, valid, message"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id}/token/validate [get]
func (h *MetadataHandler) ValidateDataSourceToken(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	hasToken, valid, message, err := h.metadataSvc.ValidateDataSourceToken(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}

	Success(c, gin.H{
		"has_token": hasToken,
		"valid":     valid,
		"message":   message,
	})
}

// GetDataSourceConfig handles GET /api/v1/datasources/:id/config
// Returns api_url and token for the config form (token only if present).
// @Summary      Get data source config for edit
// @Description  Returns api_url and token for pre-filling the configure modal. Token is only included when present.
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  Response  "api_url, token (optional)"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id}/config [get]
func (h *MetadataHandler) GetDataSourceConfig(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	apiURL, token, err := h.metadataSvc.GetDataSourceConfig(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}

	out := gin.H{"api_url": apiURL}
	if token != "" {
		out["token"] = token
	}
	Success(c, out)
}

// ==================== API Metadata Endpoints ====================

// ListAPIMetadata handles GET /api/v1/datasources/:id/api-metadata
// @Summary      List API metadata for a data source
// @Description  Get a paginated list of API metadata. Each item includes category_id. Optional filters: api_metadata_id, name, category_id.
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id         path      string  true   "Data source ID"
// @Param        page       query     int     false  "Page number (1-based)"     default(1)
// @Param        page_size  query     int     false  "Page size (max 100)"       default(20)
// @Param        api_metadata_id  query     string  false  "Filter by API metadata ID (exact)"
// @Param        name             query     string  false  "Filter by name (contains)"
// @Param        category_id      query     string  false  "Filter by API category ID (exact)"
// @Success      200        {object}  PagedResponse  "data: API metadata list with category_id; total, page, size"
// @Failure      404        {object}  Response  "Data source not found"
// @Failure      500        {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id}/api-metadata [get]
func (h *MetadataHandler) ListAPIMetadata(c *gin.Context) {
	dataSourceID := shared.ID(c.Param("id"))
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	idStr := c.Query("api_metadata_id")
	name := c.Query("name")
	categoryIDStr := c.Query("category_id")

	req := contracts.ListAPIMetadataRequest{
		Page:     page,
		PageSize: pageSize,
		Name:     name,
	}
	if idStr != "" {
		id := shared.ID(idStr)
		req.ID = &id
	}
	if categoryIDStr != "" {
		cid := shared.ID(categoryIDStr)
		req.CategoryID = &cid
	}

	resp, err := h.metadataSvc.ListAPIMetadata(c.Request.Context(), dataSourceID, req)
	if err != nil {
		HandleError(c, err)
		return
	}
	Paged(c, resp.Items, resp.Total, page, pageSize)
}

// ListAPINames handles GET /api/v1/datasources/:id/api-names
// 返回该数据源下所有 API 名称，用于「公共数据 API」表单勾选项。
// @Summary      List API names for a data source
// @Description  Get all API names for the data source (e.g. for common-data-apis checkbox form).
// @Tags         DataSources
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  ListAPINamesResp
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id}/api-names [get]
func (h *MetadataHandler) ListAPINames(c *gin.Context) {
	dataSourceID := shared.ID(c.Param("id"))
	names, err := h.metadataSvc.ListAPINames(c.Request.Context(), dataSourceID)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, ListAPINamesResp{APINames: names})
}

// ListAPINamesResp 数据源 API 名称列表（用于公共数据 API 勾选表单）
type ListAPINamesResp struct {
	APINames []string `json:"api_names"`
}

// ListAPICategories handles GET /api/v1/datasources/:id/api-categories
// @Summary      List API categories for a data source
// @Description  Get API categories. When has_apis_only=true, only categories that have at least one api_metadata are returned (for catalog dropdown).
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id              path      string  true   "Data source ID"
// @Param        has_apis_only   query     bool    false  "If true, only return categories that have at least one API"
// @Success      200  {object}  Response  "data: []APICategory"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id}/api-categories [get]
func (h *MetadataHandler) ListAPICategories(c *gin.Context) {
	dataSourceID := shared.ID(c.Param("id"))
	hasAPIsOnly := c.Query("has_apis_only") == "true" || c.Query("has_apis_only") == "1"
	list, err := h.metadataSvc.ListAPICategories(c.Request.Context(), dataSourceID, hasAPIsOnly)
	if err != nil {
		HandleError(c, err)
		return
	}
	if list == nil {
		list = []metadata.APICategory{}
	}
	Success(c, list)
}

// DeleteAPIMetadata handles DELETE /api/v1/datasources/:id/api-metadata/:meta_id
// Admin only (Casbin: datasources delete).
// @Summary      Delete API metadata
// @Description  Delete a single API metadata by ID. Admin only.
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id       path      string  true  "Data source ID"
// @Param        meta_id  path      string  true  "API metadata ID"
// @Success      204  "No Content"
// @Failure      403  {object}  Response  "Forbidden (non-admin)"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datasources/{id}/api-metadata/{meta_id} [delete]
func (h *MetadataHandler) DeleteAPIMetadata(c *gin.Context) {
	metaID := shared.ID(c.Param("meta_id"))
	if err := h.metadataSvc.DeleteAPIMetadata(c.Request.Context(), metaID); err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
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
// @Security     BearerAuth
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
		FixedParams:      req.FixedParams,
		FixedParamKeys:   req.FixedParamKeys,
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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
		FixedParams:      req.FixedParams,
		FixedParamKeys:   req.FixedParamKeys,
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
// @Security     BearerAuth
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

// UpdateDataSourceCommonDataAPIsReq represents the request body for PUT /datasources/:id/common-data-apis.
type UpdateDataSourceCommonDataAPIsReq struct {
	CommonDataAPIs []string `json:"common_data_apis"`
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
	APIName          string                 `json:"api_name" binding:"required"`
	PreferredParam   string                 `json:"preferred_param" binding:"required"` // none/trade_date/ts_code
	SupportDateRange bool                   `json:"support_date_range"`
	RequiredParams   []string               `json:"required_params"`
	Dependencies     []string               `json:"dependencies"`
	FixedParams      map[string]interface{} `json:"fixed_params"`     // JSON object
	FixedParamKeys   []string               `json:"fixed_param_keys"` // keys that cannot be overridden
	Description      string                 `json:"description"`
}

// UpdateAPISyncStrategyReq represents the request body for updating an API sync strategy.
type UpdateAPISyncStrategyReq struct {
	PreferredParam   *string                 `json:"preferred_param"`
	SupportDateRange *bool                   `json:"support_date_range"`
	RequiredParams   *[]string               `json:"required_params"`
	Dependencies     *[]string               `json:"dependencies"`
	FixedParams      *map[string]interface{} `json:"fixed_params"`
	FixedParamKeys   *[]string               `json:"fixed_param_keys"`
	Description      *string                 `json:"description"`
}
