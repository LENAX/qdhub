// Package impl contains application service implementations.
package impl

import (
	"context"
	"fmt"
	"strings"

	"qdhub/internal/application/contracts"
	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
	"qdhub/internal/domain/workflow"
)

// DataStoreApplicationServiceImpl implements DataStoreApplicationService.
type DataStoreApplicationServiceImpl struct {
	dataStoreRepo  datastore.QuantDataStoreRepository
	dataSourceRepo metadata.DataSourceRepository
	syncPlanRepo   sync.SyncPlanRepository

	schemaValidator datastore.SchemaValidator
	quantDBAdapter  QuantDBAdapter // optional: for create-file/connect and validation

	workflowExecutor workflow.WorkflowExecutor
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

	// InvalidateConnection drops the cached connection for the given data store ID (e.g. after connection info update).
	InvalidateConnection(id shared.ID)

	// ListTables returns table names in the data store's database (e.g. main schema).
	ListTables(ctx context.Context, ds *datastore.QuantDataStore) ([]string, error)

	// Query executes a read-only SQL query and returns rows. Caller must ensure SQL is safe (e.g. table name whitelist).
	Query(ctx context.Context, ds *datastore.QuantDataStore, sql string, args ...any) ([]map[string]any, error)
}

// NewDataStoreApplicationService creates a new DataStoreApplicationService implementation.
// quantDBAdapter can be nil: then CreateDataStore will not run post-create validation, and ValidateDataStore will only do schema validation.
func NewDataStoreApplicationService(
	dataStoreRepo datastore.QuantDataStoreRepository,
	dataSourceRepo metadata.DataSourceRepository,
	syncPlanRepo sync.SyncPlanRepository,
	workflowExecutor workflow.WorkflowExecutor,
	quantDBAdapter QuantDBAdapter,
) contracts.DataStoreApplicationService {
	return &DataStoreApplicationServiceImpl{
		dataStoreRepo:    dataStoreRepo,
		dataSourceRepo:   dataSourceRepo,
		syncPlanRepo:     syncPlanRepo,
		schemaValidator:  datastore.NewSchemaValidator(),
		quantDBAdapter:   quantDBAdapter,
		workflowExecutor: workflowExecutor,
	}
}

// ==================== Data Store Management ====================

// CreateDataStore creates a new data store. For DuckDB, Connect creates the DB file if missing; for others, attempts connection. Runs validation after create.
func (s *DataStoreApplicationServiceImpl) CreateDataStore(ctx context.Context, req contracts.CreateDataStoreRequest) (*datastore.QuantDataStore, error) {
	ds := datastore.NewQuantDataStore(
		req.Name,
		req.Description,
		req.Type,
		req.DSN,
		req.StoragePath,
	)

	if err := s.schemaValidator.ValidateDataStore(ds); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	if err := s.dataStoreRepo.Create(ds); err != nil {
		return nil, fmt.Errorf("failed to create data store: %w", err)
	}

	// Post-create validation: DuckDB file is created on first Connect; others attempt connection
	if s.quantDBAdapter != nil {
		if err := s.quantDBAdapter.TestConnection(ctx, ds); err != nil {
			return ds, fmt.Errorf("data store created but validation failed (database unreachable): %w", err)
		}
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

// ListDataStores lists all data stores.
func (s *DataStoreApplicationServiceImpl) ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error) {
	stores, err := s.dataStoreRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list data stores: %w", err)
	}
	return stores, nil
}

// UpdateDataStore updates an existing data store.
func (s *DataStoreApplicationServiceImpl) UpdateDataStore(ctx context.Context, id shared.ID, req contracts.UpdateDataStoreRequest) (*datastore.QuantDataStore, error) {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	if req.Name != nil {
		ds.Name = strings.TrimSpace(*req.Name)
	}
	if req.Description != nil {
		ds.Description = *req.Description
	}
	if req.Type != nil {
		ds.Type = *req.Type
	}
	if req.DSN != nil {
		ds.DSN = *req.DSN
	}
	if req.StoragePath != nil {
		ds.StoragePath = *req.StoragePath
	}
	ds.UpdatedAt = shared.Now()

	if err := s.schemaValidator.ValidateDataStore(ds); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	if err := s.dataStoreRepo.Update(ds); err != nil {
		return nil, fmt.Errorf("failed to update data store: %w", err)
	}

	if s.quantDBAdapter != nil {
		s.quantDBAdapter.InvalidateConnection(id)
		if err := s.quantDBAdapter.TestConnection(ctx, ds); err != nil {
			return ds, fmt.Errorf("data store updated but validation failed (database unreachable): %w", err)
		}
	}

	return ds, nil
}

// DeleteDataStore deletes a data store. Fails if any sync plan references it.
func (s *DataStoreApplicationServiceImpl) DeleteDataStore(ctx context.Context, id shared.ID) error {
	_, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get data store: %w", err)
	}

	exists, err := s.syncPlanRepo.Exists(shared.Eq("data_store_id", id.String()))
	if err != nil {
		return fmt.Errorf("failed to check sync plans: %w", err)
	}
	if exists {
		return shared.NewDomainError(shared.ErrCodeValidation, "data store is in use by one or more sync plans", nil)
	}

	if err := s.dataStoreRepo.Delete(id); err != nil {
		return fmt.Errorf("failed to delete data store: %w", err)
	}
	return nil
}

// ValidateDataStore checks whether the data store's database actually exists and is reachable.
func (s *DataStoreApplicationServiceImpl) ValidateDataStore(ctx context.Context, id shared.ID) (*contracts.ValidateDataStoreResult, error) {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	if err := s.schemaValidator.ValidateDataStore(ds); err != nil {
		return &contracts.ValidateDataStoreResult{Valid: false, Message: err.Error()}, nil
	}

	if s.quantDBAdapter == nil {
		return &contracts.ValidateDataStoreResult{Valid: true}, nil
	}

	if err := s.quantDBAdapter.TestConnection(ctx, ds); err != nil {
		return &contracts.ValidateDataStoreResult{Valid: false, Message: err.Error()}, nil
	}
	return &contracts.ValidateDataStoreResult{Valid: true}, nil
}

// CreateTablesForDatasource creates tables for all APIs of a data source in the data store.
// This is an asynchronous operation that uses the built-in create_tables workflow.
//
// Pre-conditions validated:
//   - Data source must exist (validated using req.DataSourceID)
//   - Data store must exist (validated using req.DataStoreID)
//   - Data store must have StoragePath or DSN configured
//
// The same DataSourceID is used for both validation and workflow execution
// to ensure consistency.
func (s *DataStoreApplicationServiceImpl) CreateTablesForDatasource(ctx context.Context, req contracts.CreateTablesForDatasourceRequest) (shared.ID, error) {
	// 1. 验证数据源是否存在（前置条件校验）
	dataSource, err := s.dataSourceRepo.Get(req.DataSourceID)
	if err != nil {
		return "", fmt.Errorf("failed to get data source: %w", err)
	}
	if dataSource == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data source not found", nil)
	}

	// 2. 验证数据存储是否存在
	dataStore, err := s.dataStoreRepo.Get(req.DataStoreID)
	if err != nil {
		return "", fmt.Errorf("failed to get data store: %w", err)
	}
	if dataStore == nil {
		return "", shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}

	// 3. 获取目标数据库路径
	// 优先使用 StoragePath，如果为空则使用 DSN
	targetDBPath := dataStore.StoragePath
	if targetDBPath == "" {
		targetDBPath = dataStore.DSN
	}
	if targetDBPath == "" {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "data store must have either StoragePath or DSN configured", nil)
	}

	// 4. 验证数据源名称
	dataSourceName := dataSource.Name
	if dataSourceName == "" {
		return "", shared.NewDomainError(shared.ErrCodeValidation, "data source name cannot be empty", nil)
	}

	// 5. 准备 max tables 参数
	maxTables := 0 // 0 表示不限制
	if req.MaxTables != nil && *req.MaxTables > 0 {
		maxTables = *req.MaxTables
	}

	// 6. 验证 workflow executor 是否可用
	if s.workflowExecutor == nil {
		return "", fmt.Errorf("workflow executor is not available")
	}

	// 7. 执行内建的 create_tables workflow
	// 使用类型安全的 ExecuteCreateTables 方法
	// 注意：req.DataSourceID 既用于上面的校验，也用于 workflow 执行，确保一致性
	instanceID, err := s.workflowExecutor.ExecuteCreateTables(ctx, workflow.CreateTablesRequest{
		DataSourceID:   req.DataSourceID, // 与校验时使用的 ID 一致
		DataSourceName: dataSourceName,   // 从校验通过的数据源获取
		TargetDBPath:   targetDBPath,     // 从校验通过的数据存储获取
		MaxTables:      maxTables,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute create_tables workflow: %w", err)
	}

	return instanceID, nil
}

// ==================== Data Browser ====================

// ListDatastoreTables lists table names in the given data store's database.
func (s *DataStoreApplicationServiceImpl) ListDatastoreTables(ctx context.Context, id shared.ID) ([]string, error) {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}
	if s.quantDBAdapter == nil {
		return nil, shared.NewDomainError(shared.ErrCodeValidation, "data browser not available", nil)
	}
	tables, err := s.quantDBAdapter.ListTables(ctx, ds)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	return tables, nil
}

// GetDatastoreTableData returns a page of rows from a table and the total row count.
// tableName must be in the whitelist from ListDatastoreTables.
func (s *DataStoreApplicationServiceImpl) GetDatastoreTableData(ctx context.Context, id shared.ID, tableName string, page, pageSize int) ([]map[string]any, int64, error) {
	ds, err := s.dataStoreRepo.Get(id)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get data store: %w", err)
	}
	if ds == nil {
		return nil, 0, shared.NewDomainError(shared.ErrCodeNotFound, "data store not found", nil)
	}
	if s.quantDBAdapter == nil {
		return nil, 0, shared.NewDomainError(shared.ErrCodeValidation, "data browser not available", nil)
	}
	// Whitelist: only allow table names returned by ListTables
	tables, err := s.quantDBAdapter.ListTables(ctx, ds)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list tables: %w", err)
	}
	allowed := make(map[string]bool)
	for _, t := range tables {
		allowed[t] = true
	}
	if !allowed[tableName] {
		return nil, 0, shared.NewDomainError(shared.ErrCodeNotFound, "table not found", nil)
	}
	// Clamp page size (e.g. 1–100)
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * pageSize
	quoted := quoteTableName(tableName)

	// Total count
	countSQL := "SELECT COUNT(*) AS n FROM " + quoted
	countRows, err := s.quantDBAdapter.Query(ctx, ds, countSQL)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count rows: %w", err)
	}
	var total int64
	if len(countRows) > 0 {
		if n, ok := countRows[0]["n"]; ok {
			switch v := n.(type) {
			case int64:
				total = v
			case int:
				total = int64(v)
			case int32:
				total = int64(v)
			}
		}
	}

	// Page of rows
	dataSQL := "SELECT * FROM " + quoted + " LIMIT ? OFFSET ?"
	rows, err := s.quantDBAdapter.Query(ctx, ds, dataSQL, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query table data: %w", err)
	}
	return rows, total, nil
}

// quoteTableName quotes a table name for SQL (DuckDB: double-quote and escape).
// Only use for names that have passed whitelist.
func quoteTableName(name string) string {
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return `"` + escaped + `"`
}
