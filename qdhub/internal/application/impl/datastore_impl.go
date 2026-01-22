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
	dataStoreRepo  datastore.QuantDataStoreRepository
	dataSourceRepo metadata.DataSourceRepository

	schemaValidator datastore.SchemaValidator

	// Workflow executor for executing built-in workflows (领域服务接口)
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
}

// NewDataStoreApplicationService creates a new DataStoreApplicationService implementation.
func NewDataStoreApplicationService(
	dataStoreRepo datastore.QuantDataStoreRepository,
	dataSourceRepo metadata.DataSourceRepository,
	workflowExecutor workflow.WorkflowExecutor,
) contracts.DataStoreApplicationService {
	return &DataStoreApplicationServiceImpl{
		dataStoreRepo:    dataStoreRepo,
		dataSourceRepo:   dataSourceRepo,
		schemaValidator:  datastore.NewSchemaValidator(),
		workflowExecutor: workflowExecutor,
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

// ListDataStores lists all data stores.
func (s *DataStoreApplicationServiceImpl) ListDataStores(ctx context.Context) ([]*datastore.QuantDataStore, error) {
	stores, err := s.dataStoreRepo.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list data stores: %w", err)
	}
	return stores, nil
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
