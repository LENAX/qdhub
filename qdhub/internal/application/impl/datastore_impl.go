// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

// DataStoreApplicationServiceImpl implements DataStoreApplicationService.
type DataStoreApplicationServiceImpl struct {
	dataStoreRepo   datastore.QuantDataStoreRepository
	mappingRuleRepo datastore.DataTypeMappingRuleRepository
	dataSourceRepo  metadata.DataSourceRepository

	schemaValidator    datastore.SchemaValidator
	schemaGenerator    datastore.SchemaGenerator
	typeMappingService datastore.TypeMappingService

	// Adapter for executing DDL on target databases
	quantDBAdapter QuantDBAdapter

	// Workflow service for executing built-in workflows
	workflowSvc contracts.WorkflowApplicationService
}

// QuantDBAdapter defines the interface for interacting with target databases.
// Implementation: infrastructure/quantdb/
type QuantDBAdapter interface {
	// TestConnection tests the connection to a data store.
	TestConnection(ctx context.Context, ds *datastore.QuantDataStore) error

	// ExecuteDDL executes DDL statement on a data store.
	ExecuteDDL(ctx context.Context, ds *datastore.QuantDataStore, ddl string) error

	// TableExists checks if a table exists in the data store.
	TableExists(ctx context.Context, ds *datastore.QuantDataStore, tableName string) (bool, error)
}

// NewDataStoreApplicationService creates a new DataStoreApplicationService implementation.
func NewDataStoreApplicationService(
	dataStoreRepo datastore.QuantDataStoreRepository,
	mappingRuleRepo datastore.DataTypeMappingRuleRepository,
	dataSourceRepo metadata.DataSourceRepository,
	quantDBAdapter QuantDBAdapter,
	workflowSvc contracts.WorkflowApplicationService,
) contracts.DataStoreApplicationService {
	return &DataStoreApplicationServiceImpl{
		dataStoreRepo:      dataStoreRepo,
		mappingRuleRepo:    mappingRuleRepo,
		dataSourceRepo:     dataSourceRepo,
		schemaValidator:    datastore.NewSchemaValidator(),
		schemaGenerator:    datastore.NewSchemaGenerator(),
		typeMappingService: datastore.NewTypeMappingService(),
		quantDBAdapter:     quantDBAdapter,
		workflowSvc:        workflowSvc,
	}
}

// ==================== Data Store Management ====================

// CreateDataStore creates a new data store.
func (s *DataStoreApplicationServiceImpl) CreateDataStore(ctx context.Context, req contracts.CreateDataStoreRequest) (*datastore.QuantDataStore, error) {
	// Create domain entity
	ds := datastore.NewQuantDataStore(
		req.Name,
		req.Description,
		req.Type,
		req.DSN,
		req.StoragePath,
	)

	// Validate
	if err := s.schemaValidator.ValidateDataStore(ds); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.dataStoreRepo.Create(ds); err != nil {
		return nil, fmt.Errorf("failed to create data store: %w", err)
	}

	return ds, nil
}

// GetDataStore retrieves a data store by ID.
func (s *DataStoreApplicationServiceImpl) GetDataStore(ctx context.Context, id shared.ID) (*datastore.QuantDataStore, error) {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}
	return ds, nil
}

// UpdateDataStore updates a data store.
func (s *DataStoreApplicationServiceImpl) UpdateDataStore(ctx context.Context, id shared.ID, req contracts.UpdateDataStoreRequest) error {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// Apply updates
	if req.Name != nil {
		ds.Name = *req.Name
	}
	if req.Description != nil {
		ds.Description = *req.Description
	}
	if req.DSN != nil || req.StoragePath != nil {
		newDSN := ds.DSN
		newStoragePath := ds.StoragePath
		if req.DSN != nil {
			newDSN = *req.DSN
		}
		if req.StoragePath != nil {
			newStoragePath = *req.StoragePath
		}
		ds.UpdateConnection(newDSN, newStoragePath)
	}

	// Validate
	if err := s.schemaValidator.ValidateDataStore(ds); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.dataStoreRepo.Update(ds); err != nil {
		return fmt.Errorf("failed to update data store: %w", err)
	}

	return nil
}

// DeleteDataStore deletes a data store.
func (s *DataStoreApplicationServiceImpl) DeleteDataStore(ctx context.Context, id shared.ID) error {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// Check for existing schemas
	schemas, err := s.dataStoreRepo.GetSchemasByDataStore(id)
	if err != nil {
		return fmt.Errorf("failed to check existing schemas: %w", err)
	}
	if len(schemas) > 0 {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot delete data store with existing schemas", nil)
	}

	if err := s.dataStoreRepo.Delete(id); err != nil {
		return fmt.Errorf("failed to delete data store: %w", err)
	}

	return nil
}

// ListDataStores lists all data stores.
func (s *DataStoreApplicationServiceImpl) ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error) {
	stores, err := s.dataStoreRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list data stores: %w", err)
	}
	return stores, nil
}

// TestConnection tests the connection to a data store.
func (s *DataStoreApplicationServiceImpl) TestConnection(ctx context.Context, id shared.ID) error {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	if err := s.quantDBAdapter.TestConnection(ctx, ds); err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}

	return nil
}

// ==================== Table Schema Management ====================

// GenerateTableSchema generates table schema from API metadata.
func (s *DataStoreApplicationServiceImpl) GenerateTableSchema(ctx context.Context, req contracts.GenerateSchemaRequest) (*datastore.TableSchema, error) {
	// Get data store
	ds, err := s.dataStoreRepo.Get(req.DataStoreID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// Get API metadata
	apiMeta, err := s.dataSourceRepo.GetAPIMetadata(req.APIMetadataID)
	if err != nil {
		return nil, fmt.Errorf("failed to get API metadata: %w", err)
	}
	if apiMeta == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "API metadata not found", nil)
	}

	// Get mapping rules for this data source and target DB
	rules, err := s.mappingRuleRepo.GetBySourceAndTarget("tushare", ds.Type.String())
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping rules: %w", err)
	}

	// Create table schema
	schema := datastore.NewTableSchema(req.DataStoreID, req.APIMetadataID, req.TableName)

	// Generate column definitions from API response fields
	columns := make([]datastore.ColumnDef, 0, len(apiMeta.ResponseFields))
	primaryKeys := make([]string, 0)

	for _, field := range apiMeta.ResponseFields {
		// Find matching mapping rule
		rule := s.typeMappingService.FindBestMatchingRule(rules, field.Name, field.Type)
		targetType := "TEXT" // Default type
		if rule != nil {
			targetType = rule.TargetType
		}

		col := datastore.ColumnDef{
			Name:       field.Name,
			SourceType: field.Type,
			TargetType: targetType,
			Nullable:   !field.IsPrimary, // Primary keys are not nullable
			Comment:    field.Description,
		}
		columns = append(columns, col)

		// Collect primary keys
		if field.IsPrimary {
			primaryKeys = append(primaryKeys, field.Name)
		}
	}

	schema.SetColumns(columns)
	schema.SetPrimaryKeys(primaryKeys)

	// Validate schema
	if err := s.schemaValidator.ValidateTableSchema(schema); err != nil {
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	// Persist schema
	if err := s.dataStoreRepo.AddSchema(schema); err != nil {
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	// Auto create table if requested
	if req.AutoCreate {
		if err := s.CreateTable(ctx, schema.ID); err != nil {
			// Mark schema as failed but don't return error
			schema.MarkFailed(err.Error())
			_ = s.dataStoreRepo.UpdateSchema(schema)
		}
	}

	return schema, nil
}

// CreateTable creates a table in the data store.
func (s *DataStoreApplicationServiceImpl) CreateTable(ctx context.Context, schemaID shared.ID) error {
	schema, err := s.dataStoreRepo.GetSchema(schemaID)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}
	if schema == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "schema not found", nil)
	}

	// Get data store
	ds, err := s.dataStoreRepo.Get(schema.DataStoreID)
	if err != nil {
		return fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// Generate DDL
	ddl, err := s.schemaGenerator.GenerateDDL(schema, ds.Type)
	if err != nil {
		return fmt.Errorf("failed to generate DDL: %w", err)
	}

	// Execute DDL
	if err := s.quantDBAdapter.ExecuteDDL(ctx, ds, ddl); err != nil {
		schema.MarkFailed(err.Error())
		_ = s.dataStoreRepo.UpdateSchema(schema)
		return fmt.Errorf("failed to execute DDL: %w", err)
	}

	// Mark schema as created
	schema.MarkCreated()
	if err := s.dataStoreRepo.UpdateSchema(schema); err != nil {
		return fmt.Errorf("failed to update schema status: %w", err)
	}

	return nil
}

// CreateTablesForDatasource creates tables for all APIs of a data source in the data store.
// This is an asynchronous operation that uses the built-in create_tables workflow.
func (s *DataStoreApplicationServiceImpl) CreateTablesForDatasource(ctx context.Context, req contracts.CreateTablesForDatasourceRequest) (shared.ID, error) {
	// Verify data source exists
	dataSource, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get data source: %w", err)
	}
	if dataSource == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// Verify data store exists
	dataStore, err := s.dataStoreRepo.Get(req.DataStoreID)
	if err != nil {
		return "", fmt.Errorf("failed to get data store: %w", err)
	}
	if dataStore == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// Get target database path
	// Prefer StoragePath, fallback to DSN if StoragePath is empty
	targetDBPath := dataStore.StoragePath
	if targetDBPath == "" {
		targetDBPath = dataStore.DSN
	}
	if targetDBPath == "" {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "data store must have either StoragePath or DSN configured", nil)
	}

	// Get data source name (use Name field as data source type name)
	// Note: DataSource.Name should be the type name like "tushare"
	dataSourceName := dataSource.Name
	if dataSourceName == "" {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "data source name cannot be empty", nil)
	}

	// Prepare max tables parameter
	maxTables := 0 // 0 means no limit
	if req.MaxTables != nil && *req.MaxTables > 0 {
		maxTables = *req.MaxTables
	}

	// Build workflow parameters
	// Note: Using built-in workflow ID "builtin:create_tables"
	// The workflow will use parameter placeholders that will be replaced at execution time
	triggerParams := map[string]interface{}{
		"data_source_id":   req.DataSourceID.String(),
		"data_source_name": dataSourceName,
		"target_db_path":  targetDBPath,
		"max_tables":       maxTables,
	}

	// Execute built-in workflow using fixed ID
	// According to the plan, built-in workflows have fixed IDs like "builtin:create_tables"
	workflowDefID := shared.ID("builtin:create_tables")
	instanceID, err := s.workflowSvc.ExecuteWorkflow(ctx, contracts.ExecuteWorkflowRequest{
		WorkflowDefID: workflowDefID,
		TriggerType:   workflow.TriggerTypeManual,
		TriggerParams: triggerParams,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute create_tables workflow: %w", err)
	}

	return instanceID, nil
}

// DropTable drops a table from the data store.
func (s *DataStoreApplicationServiceImpl) DropTable(ctx context.Context, schemaID shared.ID) error {
	schema, err := s.dataStoreRepo.GetSchema(schemaID)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}
	if schema == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "schema not found", nil)
	}

	// Get data store
	ds, err := s.dataStoreRepo.Get(schema.DataStoreID)
	if err != nil {
		return fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// Generate drop DDL
	dropDDL := s.schemaGenerator.GenerateDropDDL(schema.TableName, ds.Type)

	// Execute DDL
	if err := s.quantDBAdapter.ExecuteDDL(ctx, ds, dropDDL); err != nil {
		return fmt.Errorf("failed to execute drop DDL: %w", err)
	}

	// Delete schema record
	if err := s.dataStoreRepo.DeleteSchema(schemaID); err != nil {
		return fmt.Errorf("failed to delete schema: %w", err)
	}

	return nil
}

// GetTableSchema retrieves a table schema by ID.
func (s *DataStoreApplicationServiceImpl) GetTableSchema(ctx context.Context, id shared.ID) (*datastore.TableSchema, error) {
	schema, err := s.dataStoreRepo.GetSchema(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	if schema == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "schema not found", nil)
	}
	return schema, nil
}

// GetTableSchemaByAPI retrieves table schema by API metadata ID.
func (s *DataStoreApplicationServiceImpl) GetTableSchemaByAPI(ctx context.Context, apiMetadataID shared.ID) (*datastore.TableSchema, error) {
	schema, err := s.dataStoreRepo.GetSchemaByAPIMetadata(apiMetadataID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema by API: %w", err)
	}
	if schema == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "schema not found", nil)
	}
	return schema, nil
}

// ListTableSchemas lists all table schemas for a data store.
func (s *DataStoreApplicationServiceImpl) ListTableSchemas(ctx context.Context, dataStoreID shared.ID) ([]*datastore.TableSchema, error) {
	schemas, err := s.dataStoreRepo.GetSchemasByDataStore(dataStoreID)
	if err != nil {
		return nil, fmt.Errorf("failed to list schemas: %w", err)
	}
	return schemas, nil
}

// UpdateTableSchema updates a table schema.
func (s *DataStoreApplicationServiceImpl) UpdateTableSchema(ctx context.Context, id shared.ID, req contracts.UpdateSchemaRequest) error {
	schema, err := s.dataStoreRepo.GetSchema(id)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}
	if schema == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "schema not found", nil)
	}

	// Cannot update created schema (would require ALTER TABLE)
	if schema.Status == datastore.SchemaStatusCreated {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "cannot update schema that is already created in database", nil)
	}

	// Apply updates
	if req.Columns != nil {
		schema.SetColumns(*req.Columns)
	}
	if req.PrimaryKeys != nil {
		schema.SetPrimaryKeys(*req.PrimaryKeys)
	}
	if req.Indexes != nil {
		schema.Indexes = *req.Indexes
	}

	// Validate
	if err := s.schemaValidator.ValidateTableSchema(schema); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.dataStoreRepo.UpdateSchema(schema); err != nil {
		return fmt.Errorf("failed to update schema: %w", err)
	}

	return nil
}

// SyncSchemaStatus synchronizes schema status with actual database state.
func (s *DataStoreApplicationServiceImpl) SyncSchemaStatus(ctx context.Context, dataStoreID shared.ID) error {
	// Get data store
	ds, err := s.dataStoreRepo.Get(dataStoreID)
	if err != nil {
		return fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// Get all schemas for this data store
	schemas, err := s.dataStoreRepo.GetSchemasByDataStore(dataStoreID)
	if err != nil {
		return fmt.Errorf("failed to get schemas: %w", err)
	}

	// Check each schema's table existence
	for _, schema := range schemas {
		exists, err := s.quantDBAdapter.TableExists(ctx, ds, schema.TableName)
		if err != nil {
			continue // Skip on error
		}

		if exists && schema.Status != datastore.SchemaStatusCreated {
			schema.MarkCreated()
			_ = s.dataStoreRepo.UpdateSchema(schema)
		} else if !exists && schema.Status == datastore.SchemaStatusCreated {
			schema.MarkFailed("table no longer exists")
			_ = s.dataStoreRepo.UpdateSchema(schema)
		}
	}

	return nil
}

// ==================== Type Mapping Rule Management ====================

// CreateMappingRule creates a new type mapping rule.
func (s *DataStoreApplicationServiceImpl) CreateMappingRule(ctx context.Context, req contracts.CreateMappingRuleRequest) (*datastore.DataTypeMappingRule, error) {
	// Create domain entity
	rule := datastore.NewDataTypeMappingRule(
		req.DataSourceType,
		req.SourceType,
		req.TargetDBType,
		req.TargetType,
		req.Priority,
		false, // Not default rule
	)

	if req.FieldPattern != nil {
		rule.SetFieldPattern(*req.FieldPattern)
	}

	// Validate
	if err := s.typeMappingService.ValidateMappingRule(rule); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Persist
	if err := s.mappingRuleRepo.Create(rule); err != nil {
		return nil, fmt.Errorf("failed to create mapping rule: %w", err)
	}

	return rule, nil
}

// GetMappingRules retrieves mapping rules for data source and target DB.
func (s *DataStoreApplicationServiceImpl) GetMappingRules(ctx context.Context, dataSourceType, targetDBType string) ([]*datastore.DataTypeMappingRule, error) {
	rules, err := s.mappingRuleRepo.GetBySourceAndTarget(dataSourceType, targetDBType)
	if err != nil {
		return nil, fmt.Errorf("failed to get mapping rules: %w", err)
	}
	return rules, nil
}
