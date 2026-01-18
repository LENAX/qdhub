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
	rg.PUT("/datasources/:id", h.UpdateDataSource)
	rg.DELETE("/datasources/:id", h.DeleteDataSource)

	// Metadata refresh
	rg.POST("/datasources/:id/refresh", h.RefreshMetadata)

	// API categories and metadata
	rg.GET("/datasources/:id/categories", h.GetCategories)
	rg.GET("/datasources/:id/apis", h.ListAPIsByDataSource)
	rg.GET("/apis/:id", h.GetAPIMetadata)
	rg.POST("/apis", h.CreateAPIMetadata)
	rg.PUT("/apis/:id", h.UpdateAPIMetadata)
	rg.DELETE("/apis/:id", h.DeleteAPIMetadata)

	// Token management
	rg.POST("/datasources/:id/token", h.SetToken)
	rg.GET("/datasources/:id/token", h.GetToken)
	rg.DELETE("/datasources/:id/token", h.DeleteToken)
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

// UpdateDataSource handles PUT /api/v1/datasources/:id
// @Summary      Update a data source
// @Description  Update details of a specific data source
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id       path      string              true  "Data source ID"
// @Param        request  body      UpdateDataSourceReq true  "Updated data source details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datasources/{id} [put]
func (h *MetadataHandler) UpdateDataSource(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateDataSourceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.metadataSvc.UpdateDataSource(c.Request.Context(), id, contracts.UpdateDataSourceRequest{
		Name:        req.Name,
		Description: req.Description,
		BaseURL:     req.BaseURL,
		DocURL:      req.DocURL,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// DeleteDataSource handles DELETE /api/v1/datasources/:id
// @Summary      Delete a data source
// @Description  Delete a specific data source and its associated metadata
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources/{id} [delete]
func (h *MetadataHandler) DeleteDataSource(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.metadataSvc.DeleteDataSource(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
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

// ==================== API Category Endpoints ====================

// GetCategories handles GET /api/v1/datasources/:id/categories
// @Summary      Get API categories
// @Description  Get all API categories for a data source
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources/{id}/categories [get]
func (h *MetadataHandler) GetCategories(c *gin.Context) {
	// Categories are retrieved via ListAPIMetadataByDataSource
	// and then grouped by category. For simplicity, we return
	// all APIs grouped by their CategoryID.
	id := shared.ID(c.Param("id"))

	apis, err := h.metadataSvc.ListAPIMetadataByDataSource(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}

	// Group by category
	categories := make(map[string][]interface{})
	for _, api := range apis {
		catID := "uncategorized"
		if api.CategoryID != nil {
			catID = api.CategoryID.String()
		}
		categories[catID] = append(categories[catID], api)
	}

	Success(c, categories)
}

// ==================== API Metadata Endpoints ====================

// ListAPIsByDataSource handles GET /api/v1/datasources/:id/apis
// @Summary      List APIs by data source
// @Description  Get all API metadata for a specific data source
// @Tags         APIs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources/{id}/apis [get]
func (h *MetadataHandler) ListAPIsByDataSource(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	apis, err := h.metadataSvc.ListAPIMetadataByDataSource(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, apis)
}

// GetAPIMetadata handles GET /api/v1/apis/:id
// @Summary      Get API metadata
// @Description  Get details of a specific API by ID
// @Tags         APIs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "API metadata ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /apis/{id} [get]
func (h *MetadataHandler) GetAPIMetadata(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	api, err := h.metadataSvc.GetAPIMetadata(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, api)
}

// CreateAPIMetadata handles POST /api/v1/apis
// @Summary      Create API metadata
// @Description  Create a new API metadata entry
// @Tags         APIs
// @Accept       json
// @Produce      json
// @Param        request  body      CreateAPIMetadataReq  true  "API metadata details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /apis [post]
func (h *MetadataHandler) CreateAPIMetadata(c *gin.Context) {
	var req CreateAPIMetadataReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	var categoryID *shared.ID
	if req.CategoryID != "" {
		id := shared.ID(req.CategoryID)
		categoryID = &id
	}

	api, err := h.metadataSvc.CreateAPIMetadata(c.Request.Context(), contracts.CreateAPIMetadataRequest{
		DataSourceID:   shared.ID(req.DataSourceID),
		CategoryID:     categoryID,
		Name:           req.Name,
		DisplayName:    req.DisplayName,
		Description:    req.Description,
		Endpoint:       req.Endpoint,
		RequestParams:  req.RequestParams,
		ResponseFields: req.ResponseFields,
		RateLimit:      req.RateLimit,
		Permission:     req.Permission,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, api)
}

// UpdateAPIMetadata handles PUT /api/v1/apis/:id
// @Summary      Update API metadata
// @Description  Update details of a specific API metadata entry
// @Tags         APIs
// @Accept       json
// @Produce      json
// @Param        id       path      string              true  "API metadata ID"
// @Param        request  body      UpdateAPIMetadataReq true  "Updated API metadata details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /apis/{id} [put]
func (h *MetadataHandler) UpdateAPIMetadata(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateAPIMetadataReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.metadataSvc.UpdateAPIMetadata(c.Request.Context(), id, contracts.UpdateAPIMetadataRequest{
		DisplayName:    req.DisplayName,
		Description:    req.Description,
		Endpoint:       req.Endpoint,
		RequestParams:  req.RequestParams,
		ResponseFields: req.ResponseFields,
		RateLimit:      req.RateLimit,
		Permission:     req.Permission,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// DeleteAPIMetadata handles DELETE /api/v1/apis/:id
// @Summary      Delete API metadata
// @Description  Delete a specific API metadata entry
// @Tags         APIs
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "API metadata ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /apis/{id} [delete]
func (h *MetadataHandler) DeleteAPIMetadata(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.metadataSvc.DeleteAPIMetadata(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
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

// DeleteToken handles DELETE /api/v1/datasources/:id/token
// @Summary      Delete data source token
// @Description  Delete the authentication token for a data source
// @Tags         DataSources
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data source ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datasources/{id}/token [delete]
func (h *MetadataHandler) DeleteToken(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.metadataSvc.DeleteToken(c.Request.Context(), id)
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

// UpdateDataSourceReq represents the request body for updating a data source.
type UpdateDataSourceReq struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	BaseURL     *string `json:"base_url"`
	DocURL      *string `json:"doc_url"`
}

// RefreshMetadataReq represents the request body for refreshing metadata.
// Note: These fields are optional and will be ignored - the workflow fetches documentation from the data source's DocURL.
type RefreshMetadataReq struct {
	DocContent string `json:"doc_content"` // Optional - workflow fetches from DocURL
	DocType    string `json:"doc_type"`    // Optional - workflow detects type automatically
}

// CreateAPIMetadataReq represents the request body for creating API metadata.
type CreateAPIMetadataReq struct {
	DataSourceID   string               `json:"data_source_id" binding:"required"`
	CategoryID     string               `json:"category_id"`
	Name           string               `json:"name" binding:"required"`
	DisplayName    string               `json:"display_name"`
	Description    string               `json:"description"`
	Endpoint       string               `json:"endpoint"`
	RequestParams  []metadata.ParamMeta `json:"request_params"`
	ResponseFields []metadata.FieldMeta `json:"response_fields"`
	RateLimit      *metadata.RateLimit  `json:"rate_limit"`
	Permission     string               `json:"permission"`
}

// UpdateAPIMetadataReq represents the request body for updating API metadata.
type UpdateAPIMetadataReq struct {
	DisplayName    *string               `json:"display_name"`
	Description    *string               `json:"description"`
	Endpoint       *string               `json:"endpoint"`
	RequestParams  *[]metadata.ParamMeta `json:"request_params"`
	ResponseFields *[]metadata.FieldMeta `json:"response_fields"`
	RateLimit      *metadata.RateLimit   `json:"rate_limit"`
	Permission     *string               `json:"permission"`
}

// SetTokenReq represents the request body for setting a token.
type SetTokenReq struct {
	Token     string  `json:"token" binding:"required"`
	ExpiresAt *string `json:"expires_at"`
}
