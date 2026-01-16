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
func (h *MetadataHandler) ListDataSources(c *gin.Context) {
	sources, err := h.metadataSvc.ListDataSources(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, sources)
}

// CreateDataSource handles POST /api/v1/datasources
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
func (h *MetadataHandler) RefreshMetadata(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req RefreshMetadataReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	result, err := h.metadataSvc.ParseAndImportMetadata(c.Request.Context(), contracts.ParseMetadataRequest{
		DataSourceID: id,
		DocContent:   req.DocContent,
		DocType:      metadata.DocumentType(req.DocType),
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, result)
}

// ==================== API Category Endpoints ====================

// GetCategories handles GET /api/v1/datasources/:id/categories
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
func (h *MetadataHandler) GetToken(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	token, err := h.metadataSvc.GetToken(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}

	// Return token info without the actual value for security
	Success(c, gin.H{
		"id":            token.ID,
		"data_source_id": token.DataSourceID,
		"expires_at":    token.ExpiresAt,
		"created_at":    token.CreatedAt,
	})
}

// DeleteToken handles DELETE /api/v1/datasources/:id/token
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
type RefreshMetadataReq struct {
	DocContent string `json:"doc_content" binding:"required"`
	DocType    string `json:"doc_type" binding:"required"`
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
