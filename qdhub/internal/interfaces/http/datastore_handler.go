package http

import (
	"strconv"

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
	rg.POST("/datastores", h.CreateDataStore)
	rg.GET("/datastores", h.ListDataStores)
	rg.GET("/datastores/:id", h.GetDataStore)
	rg.PUT("/datastores/:id", h.UpdateDataStore)
	rg.DELETE("/datastores/:id", h.DeleteDataStore)
	rg.POST("/datastores/:id/validate", h.ValidateDataStore)

	rg.POST("/datastores/:id/create-tables", h.CreateTablesForDatasource)

	// Data browser: list tables and paginated table data
	rg.GET("/datastores/:id/tables", h.ListDatastoreTables)
	rg.GET("/datastores/:id/tables/:tableName/data", h.GetDatastoreTableData)
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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
// @Security     BearerAuth
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
// @Description  Update name, description, type, dsn, or storage_path of a data store
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id       path      string  true  "Data store ID"
// @Param        request  body      UpdateDataStoreReq  true  "Fields to update"
// @Success      200      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Security     BearerAuth
// @Router       /datastores/{id} [put]
func (h *DataStoreHandler) UpdateDataStore(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	var req UpdateDataStoreReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	r := contracts.UpdateDataStoreRequest{}
	if req.Name != nil {
		r.Name = req.Name
	}
	if req.Description != nil {
		r.Description = req.Description
	}
	if req.Type != nil {
		t := datastore.DataStoreType(*req.Type)
		r.Type = &t
	}
	if req.DSN != nil {
		r.DSN = req.DSN
	}
	if req.StoragePath != nil {
		r.StoragePath = req.StoragePath
	}

	ds, err := h.dataStoreSvc.UpdateDataStore(c.Request.Context(), id, r)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, ds)
}

// DeleteDataStore handles DELETE /api/v1/datastores/:id
// @Summary      Delete a data store
// @Description  Delete a data store. Fails if any sync plan references it.
// @Tags         DataStores
// @Param        id   path      string  true  "Data store ID"
// @Success      204  "No Content"
// @Failure      400  {object}  Response
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datastores/{id} [delete]
func (h *DataStoreHandler) DeleteDataStore(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	if err := h.dataStoreSvc.DeleteDataStore(c.Request.Context(), id); err != nil {
		HandleError(c, err)
		return
	}
	c.Status(204)
}

// ValidateDataStore handles POST /api/v1/datastores/:id/validate
// @Summary      Validate a data store
// @Description  Check whether the data store's database actually exists and is reachable
// @Tags         DataStores
// @Produce      json
// @Param        id   path      string  true  "Data store ID"
// @Success      200  {object}  Response  "Valid: true/false, Message when invalid"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datastores/{id}/validate [post]
func (h *DataStoreHandler) ValidateDataStore(c *gin.Context) {
	id := shared.ID(c.Param("id"))

	result, err := h.dataStoreSvc.ValidateDataStore(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, result)
}

// CreateTablesForDatasource handles POST /api/v1/datastores/:id/create-tables
// @Summary      Create tables for datasource
// @Description  Create tables for all APIs of a data source in the data store using the built-in create_tables workflow
// @Tags         DataStores
// @Accept       json
// @Produce      json
// @Param        id       path      string                        true  "Data store ID"
// @Param        request  body      CreateTablesForDatasourceReq  true  "Create tables request"
// @Success      201      {object}  Response
// @Failure      400      {object}  Response
// @Failure      404      {object}  Response
// @Failure      500      {object}  Response
// @Security     BearerAuth
// @Router       /datastores/{id}/create-tables [post]
func (h *DataStoreHandler) CreateTablesForDatasource(c *gin.Context) {
	dataStoreID := shared.ID(c.Param("id"))

	var req CreateTablesForDatasourceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		BadRequest(c, "invalid request body: "+err.Error())
		return
	}

	instanceID, err := h.dataStoreSvc.CreateTablesForDatasource(c.Request.Context(), contracts.CreateTablesForDatasourceRequest{
		DataSourceID: shared.ID(req.DataSourceID),
		DataStoreID:  dataStoreID,
		MaxTables:    req.MaxTables,
	})
	if err != nil {
		HandleError(c, err)
		return
	}
	Created(c, gin.H{"instance_id": instanceID})
}

// ListDatastoreTables handles GET /api/v1/datastores/:id/tables
// @Summary      List tables in a data store
// @Description  Returns table names in the data store's database (e.g. main schema)
// @Tags         DataStores
// @Produce      json
// @Param        id   path      string  true  "Data store ID"
// @Success      200  {object}  Response  "data: []string"
// @Failure      404  {object}  Response
// @Failure      500  {object}  Response
// @Security     BearerAuth
// @Router       /datastores/{id}/tables [get]
func (h *DataStoreHandler) ListDatastoreTables(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	tables, err := h.dataStoreSvc.ListDatastoreTables(c.Request.Context(), id)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, gin.H{"data": tables})
}

// GetDatastoreTableData handles GET /api/v1/datastores/:id/tables/:tableName/data
// @Summary      Get paginated table data
// @Description  Returns a page of rows from a table and total count. Query: page, page_size, q (search), search_column (optional)
// @Tags         DataStores
// @Produce      json
// @Param        id             path      string  true   "Data store ID"
// @Param        tableName      path      string  true   "Table name"
// @Param        page           query     int     false  "Page number"       default(1)
// @Param        page_size      query     int     false  "Page size (max 100)" default(20)
// @Param        q              query     string  false  "Search term (ILIKE)"
// @Param        search_column  query     string  false  "Column to search in (omit for all)"
// @Success      200            {object}  Response  "data: rows, total: count"
// @Failure      404            {object}  Response
// @Failure      500            {object}  Response
// @Security     BearerAuth
// @Router       /datastores/{id}/tables/{tableName}/data [get]
func (h *DataStoreHandler) GetDatastoreTableData(c *gin.Context) {
	id := shared.ID(c.Param("id"))
	tableName := c.Param("tableName")
	if tableName == "" {
		BadRequest(c, "table name is required")
		return
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	searchQ := c.Query("q")
	searchColumn := c.Query("search_column")
	rows, total, err := h.dataStoreSvc.GetDatastoreTableData(c.Request.Context(), id, tableName, page, pageSize, searchQ, searchColumn)
	if err != nil {
		HandleError(c, err)
		return
	}
	Success(c, &contracts.TableDataPage{Rows: rows, Total: total})
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

// UpdateDataStoreReq represents the request body for updating a data store (all fields optional).
type UpdateDataStoreReq struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Type        *string `json:"type"` // duckdb, clickhouse, postgresql
	DSN         *string `json:"dsn"`
	StoragePath *string `json:"storage_path"`
}

// CreateTablesForDatasourceReq represents the request body for creating tables for a datasource.
type CreateTablesForDatasourceReq struct {
	DataSourceID string `json:"data_source_id" binding:"required"`
	MaxTables    *int   `json:"max_tables"`
}
