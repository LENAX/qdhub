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

	// Create tables for datasource
	rg.POST("/datastores/:id/create-tables", h.CreateTablesForDatasource)
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

// ==================== Request DTOs ====================

// CreateDataStoreReq represents the request body for creating a data store.
type CreateDataStoreReq struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Type        string `json:"type" binding:"required"` // duckdb, clickhouse, postgresql
	DSN         string `json:"dsn"`
	StoragePath string `json:"storage_path"`
}

// CreateTablesForDatasourceReq represents the request body for creating tables for a datasource.
type CreateTablesForDatasourceReq struct {
	DataSourceID string `json:"data_source_id" binding:"required"`
	MaxTables    *int   `json:"max_tables"`
}
