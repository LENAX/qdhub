package http

import (
	"github.com/gin-gonic/gin"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// DataStoreHandler handles data store-related HTTP requests.
type DataStoreHandler struct {
	dataStoreSvc contracts.DataStoreApplicationService
}

// NewDataStoreHandler creates a new DataStoreHandler.
func NewDataStoreHandler(dataStoreSvc contracts.DataStoreApplicationService) *DataStoreHandler {
	return &DataStoreHandler{
		dataStoreSvc: dataStoreSvc,
	}
}

// RegisterRoutes registers data store routes to the router group.
func (h *DataStoreHandler) RegisterRoutes(rg *gin.RouterGroup) {
	// Data Store routes
	rg.POST("/datastores", h.CreateDataStore)
	rg.GET("/datastores", h.ListDataStores)
	rg.GET("/datastores/:id", h.GetDataStore)
	rg.PUT("/datastores/:id", h.UpdateDataStore)
	rg.DELETE("/datastores/:id", h.DeleteDataStore)

	// Connection test
	rg.POST("/datastores/:id/test", h.TestConnection)

	// Schema management
	rg.POST("/datastores/:id/schemas/generate", h.GenerateSchema)
	rg.GET("/datastores/:id/schemas", h.ListSchemas)
	rg.GET("/datastores/:id/schemas/:schemaId", h.GetSchema)
	rg.PUT("/datastores/:id/schemas/:schemaId", h.UpdateSchema)
	rg.POST("/datastores/:id/schemas/:schemaId/create", h.CreateTable)
	rg.DELETE("/datastores/:id/schemas/:schemaId", h.DropTable)

	// Schema sync
	rg.POST("/datastores/:id/sync-status", h.SyncSchemaStatus)

	// Mapping rules
	rg.POST("/mapping-rules", h.CreateMappingRule)
	rg.GET("/mapping-rules", h.GetMappingRules)
}

// ==================== Data Store Endpoints ====================

// CreateDataStore handles POST /api/v1/datastores
// @Summary      Create a new data store
// @Description  Create a new quantitative data store configuration
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        request  body      CreateDataStoreReq  true  "Data store details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datastores [post]
func (h *DataStoreHandler) CreateDataStore(c *gin.Context) {
	var req CreateDataStoreReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	ds, err := h.dataStoreSvc.CreateDataStore(c.Request.Context(), contracts.CreateDataStoreRequest{
		Name:        req.Name,
		Description: req.Description,
		Type:        datastore.DataStoreType(req.Type),
		DSN:         req.DSN,
		StoragePath: req.StoragePath,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, ds)
}

// ListDataStores handles GET /api/v1/datastores
// @Summary      List all data stores
// @Description  Get a list of all configured data stores
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Success      200  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datastores [get]
func (h *DataStoreHandler) ListDataStores(c *gin.Context) {
	stores, err := h.dataStoreSvc.ListDataStores(c.Request.Context())
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, stores)
}

// GetDataStore handles GET /api/v1/datastores/:id
// @Summary      Get a data store
// @Description  Get details of a specific data store by ID
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data store ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datastores/{id} [get]
func (h *DataStoreHandler) GetDataStore(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	ds, err := h.dataStoreSvc.GetDataStore(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, ds)
}

// UpdateDataStore handles PUT /api/v1/datastores/:id
// @Summary      Update a data store
// @Description  Update details of a specific data store
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id       path      string              true  "Data store ID"
// @Param        request  body      UpdateDataStoreReq  true  "Updated data store details"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datastores/{id} [put]
func (h *DataStoreHandler) UpdateDataStore(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateDataStoreReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.dataStoreSvc.UpdateDataStore(c.Request.Context(), id, contracts.UpdateDataStoreRequest{
		Name:        req.Name,
		Description: req.Description,
		DSN:         req.DSN,
		StoragePath: req.StoragePath,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// DeleteDataStore handles DELETE /api/v1/datastores/:id
// @Summary      Delete a data store
// @Description  Delete a specific data store configuration
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data store ID"
// @Success      204  {object}  nil
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datastores/{id} [delete]
func (h *DataStoreHandler) DeleteDataStore(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.dataStoreSvc.DeleteDataStore(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// TestConnection handles POST /api/v1/datastores/:id/test
// @Summary      Test data store connection
// @Description  Test the connection to a data store
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data store ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datastores/{id}/test [post]
func (h *DataStoreHandler) TestConnection(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.dataStoreSvc.TestConnection(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "connected"})
}

// ==================== Schema Endpoints ====================

// GenerateSchema handles POST /api/v1/datastores/:id/schemas/generate
// @Summary      Generate table schema
// @Description  Generate a table schema from API metadata
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id       path      string            true  "Data store ID"
// @Param        request  body      GenerateSchemaReq true  "Schema generation details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Router       /datastores/{id}/schemas/generate [post]
func (h *DataStoreHandler) GenerateSchema(c *gin.Context) {
	dataStoreID := shared.ID(c.Param("id"))

	var req GenerateSchemaReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	schema, err := h.dataStoreSvc.GenerateTableSchema(c.Request.Context(), contracts.GenerateSchemaRequest{
		APIMetadataID: shared.ID(req.APIMetadataID),
		DataStoreID:   dataStoreID,
		TableName:     req.TableName,
		AutoCreate:    req.AutoCreate,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, schema)
}

// ListSchemas handles GET /api/v1/datastores/:id/schemas
// @Summary      List table schemas
// @Description  Get all table schemas for a data store
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data store ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datastores/{id}/schemas [get]
func (h *DataStoreHandler) ListSchemas(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	schemas, err := h.dataStoreSvc.ListTableSchemas(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, schemas)
}

// GetSchema handles GET /api/v1/datastores/:id/schemas/:schemaId
// @Summary      Get table schema
// @Description  Get details of a specific table schema
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id        path      string  true  "Data store ID"
// @Param        schemaId  path      string  true  "Schema ID"
// @Success      200       {object}  Response
// @Failure      404       {object}  Response
// @Failure      500       {object}  Response
// @Router       /datastores/{id}/schemas/{schemaId} [get]
func (h *DataStoreHandler) GetSchema(c *gin.Context) {
	schemaID := shared.ID(c.Param("schemaId"))

	schema, err := h.dataStoreSvc.GetTableSchema(c.Request.Context(), schemaID)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, schema)
}

// UpdateSchema handles PUT /api/v1/datastores/:id/schemas/:schemaId
// @Summary      Update table schema
// @Description  Update a table schema configuration
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id        path      string          true  "Data store ID"
// @Param        schemaId  path      string          true  "Schema ID"
// @Param        request   body      UpdateSchemaReq true  "Updated schema details"
// @Success      200       {object}  Response
// @Failure      400       {object}  Response
// @Failure      404       {object}  Response
// @Failure      500       {object}  Response
// @Router       /datastores/{id}/schemas/{schemaId} [put]
func (h *DataStoreHandler) UpdateSchema(c *gin.Context) {
	schemaID := shared.ID(c.Param("schemaId"))

	var req UpdateSchemaReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	err := h.dataStoreSvc.UpdateTableSchema(c.Request.Context(), schemaID, contracts.UpdateSchemaRequest{
		Columns:     req.Columns,
		PrimaryKeys: req.PrimaryKeys,
		Indexes:     req.Indexes,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, nil)
}

// CreateTable handles POST /api/v1/datastores/:id/schemas/:schemaId/create
// @Summary      Create table
// @Description  Create a table in the data store based on the schema
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id        path      string  true  "Data store ID"
// @Param        schemaId  path      string  true  "Schema ID"
// @Success      200       {object}  Response
// @Failure      404       {object}  Response
// @Failure      500       {object}  Response
// @Router       /datastores/{id}/schemas/{schemaId}/create [post]
func (h *DataStoreHandler) CreateTable(c *gin.Context) {
	schemaID := shared.ID(c.Param("schemaId"))

	err := h.dataStoreSvc.CreateTable(c.Request.Context(), schemaID)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "created"})
}

// DropTable handles DELETE /api/v1/datastores/:id/schemas/:schemaId
// @Summary      Drop table
// @Description  Drop a table from the data store
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id        path      string  true  "Data store ID"
// @Param        schemaId  path      string  true  "Schema ID"
// @Success      204       {object}  nil
// @Failure      404       {object}  Response
// @Failure      500       {object}  Response
// @Router       /datastores/{id}/schemas/{schemaId} [delete]
func (h *DataStoreHandler) DropTable(c *gin.Context) {
	schemaID := shared.ID(c.Param("schemaId"))

	err := h.dataStoreSvc.DropTable(c.Request.Context(), schemaID)
	if err != nil {
		HandleError(c, err)
		return
	}
	NoContent(c)
}

// SyncSchemaStatus handles POST /api/v1/datastores/:id/sync-status
// @Summary      Sync schema status
// @Description  Synchronize schema status with the data store
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id   path      string  true  "Data store ID"
// @Success      200  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Router       /datastores/{id}/sync-status [post]
func (h *DataStoreHandler) SyncSchemaStatus(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	err := h.dataStoreSvc.SyncSchemaStatus(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"status": "synced"})
}

// ==================== Mapping Rule Endpoints ====================

// CreateMappingRule handles POST /api/v1/mapping-rules
// @Summary      Create mapping rule
// @Description  Create a data type mapping rule
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        request  body      CreateMappingRuleReq true  "Mapping rule details"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      500      {object}  Response
// @Router       /mapping-rules [post]
func (h *DataStoreHandler) CreateMappingRule(c *gin.Context) {
	var req CreateMappingRuleReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	rule, err := h.dataStoreSvc.CreateMappingRule(c.Request.Context(), contracts.CreateMappingRuleRequest{
		DataSourceType: req.DataSourceType,
		SourceType:     req.SourceType,
		TargetDBType:   req.TargetDBType,
		TargetType:     req.TargetType,
		FieldPattern:   req.FieldPattern,
		Priority:       req.Priority,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, rule)
}

// GetMappingRules handles GET /api/v1/mapping-rules
// @Summary      Get mapping rules
// @Description  Get data type mapping rules
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        data_source_type  query     string  true  "Data source type"
// @Param        target_db_type    query     string  true  "Target database type"
// @Success      200               {object}  Response
// @Failure      400               {object}  Response
// @Failure      500               {object}  Response
// @Router       /mapping-rules [get]
func (h *DataStoreHandler) GetMappingRules(c *gin.Context) {
	dataSourceType := c.Query("data_source_type")
	targetDBType := c.Query("target_db_type")

	if dataSourceType == "" || targetDBType == "" {
		BadRequest(c, "data_source_type and target_db_type are required")
		return
	}

	rules, err := h.dataStoreSvc.GetMappingRules(c.Request.Context(), dataSourceType, targetDBType)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, rules)
}

// ==================== Request DTOs ====================

// CreateDataStoreReq represents the request body for creating a data store.
type CreateDataStoreReq struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Type        string `json:"type" binding:"required"` // duckdb, clickhouse, postgresql
	DSN         string `json:"dsn"`
	StoragePath string `json:"storage_path"`
}

// UpdateDataStoreReq represents the request body for updating a data store.
type UpdateDataStoreReq struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	DSN         *string `json:"dsn"`
	StoragePath *string `json:"storage_path"`
}

// GenerateSchemaReq represents the request body for generating table schema.
type GenerateSchemaReq struct {
	APIMetadataID string `json:"api_metadata_id" binding:"required"`
	TableName     string `json:"table_name" binding:"required"`
	AutoCreate    bool   `json:"auto_create"`
}

// UpdateSchemaReq represents the request body for updating table schema.
type UpdateSchemaReq struct {
	Columns     *[]datastore.ColumnDef `json:"columns"`
	PrimaryKeys *[]string              `json:"primary_keys"`
	Indexes     *[]datastore.IndexDef  `json:"indexes"`
}

// CreateMappingRuleReq represents the request body for creating a mapping rule.
type CreateMappingRuleReq struct {
	DataSourceType string  `json:"data_source_type" binding:"required"`
	SourceType     string  `json:"source_type" binding:"required"`
	TargetDBType   string  `json:"target_db_type" binding:"required"`
	TargetType     string  `json:"target_type" binding:"required"`
	FieldPattern   *string `json:"field_pattern"`
	Priority       int     `json:"priority"`
}
