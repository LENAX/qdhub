package repository

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// QuantDataStoreRepositoryImpl implements datastore.QuantDataStoreRepository.
type QuantDataStoreRepositoryImpl struct {
	db             *persistence.DB
	dataStoreDAO   *dao.QuantDataStoreDAO
	tableSchemaDAO *dao.TableSchemaDAO
}

// NewQuantDataStoreRepository creates a new QuantDataStoreRepositoryImpl.
func NewQuantDataStoreRepository(db *persistence.DB) *QuantDataStoreRepositoryImpl {
	return &QuantDataStoreRepositoryImpl{
		db:             db,
		dataStoreDAO:   dao.NewQuantDataStoreDAO(db.DB),
		tableSchemaDAO: dao.NewTableSchemaDAO(db.DB),
	}
}

// Create creates a new quant data store with its aggregated entities.
func (r *QuantDataStoreRepositoryImpl) Create(ds *datastore.QuantDataStore) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Create data store
		if err := r.dataStoreDAO.Create(tx, ds); err != nil {
			return err
		}

		// Create table schemas
		for _, schema := range ds.Schemas {
			if err := r.tableSchemaDAO.Create(tx, &schema); err != nil {
				return err
			}
		}

		return nil
	})
}

// Get retrieves a quant data store by ID with its aggregated entities.
func (r *QuantDataStoreRepositoryImpl) Get(id shared.ID) (*datastore.QuantDataStore, error) {
	ds, err := r.dataStoreDAO.GetByID(nil, id)
	if err != nil {
		return nil, err
	}
	if ds == nil {
		return nil, nil
	}

	// Load table schemas
	schemas, err := r.tableSchemaDAO.GetByDataStore(nil, id)
	if err != nil {
		return nil, fmt.Errorf("failed to load table schemas: %w", err)
	}
	ds.Schemas = make([]datastore.TableSchema, len(schemas))
	for i, schema := range schemas {
		ds.Schemas[i] = *schema
	}

	return ds, nil
}

// Update updates a quant data store.
func (r *QuantDataStoreRepositoryImpl) Update(ds *datastore.QuantDataStore) error {
	return r.dataStoreDAO.Update(nil, ds)
}

// Delete deletes a quant data store and its aggregated entities.
func (r *QuantDataStoreRepositoryImpl) Delete(id shared.ID) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		// Delete table schemas first
		if err := r.tableSchemaDAO.DeleteByDataStore(tx, id); err != nil {
			return err
		}

		// Delete data store
		if err := r.dataStoreDAO.DeleteByID(tx, id); err != nil {
			return err
		}

		return nil
	})
}

// List retrieves all quant data stores (without aggregated entities for performance).
func (r *QuantDataStoreRepositoryImpl) List() ([]*datastore.QuantDataStore, error) {
	return r.dataStoreDAO.ListAll(nil)
}

// ==================== Child Entity Operations (TableSchema) ====================

// AddSchema adds a new TableSchema to a QuantDataStore.
func (r *QuantDataStoreRepositoryImpl) AddSchema(schema *datastore.TableSchema) error {
	return r.tableSchemaDAO.Create(nil, schema)
}

// GetSchema retrieves a TableSchema by ID.
func (r *QuantDataStoreRepositoryImpl) GetSchema(id shared.ID) (*datastore.TableSchema, error) {
	return r.tableSchemaDAO.GetByID(nil, id)
}

// GetSchemaByAPIMetadata retrieves a TableSchema by API metadata ID.
func (r *QuantDataStoreRepositoryImpl) GetSchemaByAPIMetadata(apiMetadataID shared.ID) (*datastore.TableSchema, error) {
	return r.tableSchemaDAO.GetByAPIMetadata(nil, apiMetadataID)
}

// GetSchemasByDataStore retrieves all TableSchemas for a QuantDataStore.
func (r *QuantDataStoreRepositoryImpl) GetSchemasByDataStore(dataStoreID shared.ID) ([]*datastore.TableSchema, error) {
	return r.tableSchemaDAO.GetByDataStore(nil, dataStoreID)
}

// UpdateSchema updates a TableSchema.
func (r *QuantDataStoreRepositoryImpl) UpdateSchema(schema *datastore.TableSchema) error {
	return r.tableSchemaDAO.Update(nil, schema)
}

// DeleteSchema deletes a TableSchema by ID.
func (r *QuantDataStoreRepositoryImpl) DeleteSchema(id shared.ID) error {
	return r.tableSchemaDAO.DeleteByID(nil, id)
}

// ==================== Extended Query Operations ====================

// FindBy retrieves entities matching the given conditions.
func (r *QuantDataStoreRepositoryImpl) FindBy(conditions ...shared.QueryCondition) ([]*datastore.QuantDataStore, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *QuantDataStoreRepositoryImpl) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*datastore.QuantDataStore, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *QuantDataStoreRepositoryImpl) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[datastore.QuantDataStore], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *QuantDataStoreRepositoryImpl) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[datastore.QuantDataStore], error) {
	total, err := r.Count(conditions...)
	if err != nil {
		return nil, fmt.Errorf("failed to count entities: %w", err)
	}

	items, err := r.findByInternal(nil, &pagination, conditions...)
	if err != nil {
		return nil, err
	}

	return shared.NewPageResult(items, total, pagination), nil
}

// Count returns the total count of entities matching conditions.
func (r *QuantDataStoreRepositoryImpl) Count(conditions ...shared.QueryCondition) (int64, error) {
	whereClause, args := buildWhereClause(conditions...)
	query := fmt.Sprintf("SELECT COUNT(*) FROM quant_data_stores%s", whereClause)

	var count int64
	err := r.db.DB.Get(&count, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to count quant_data_stores: %w", err)
	}
	return count, nil
}

// Exists checks if any entity matching conditions exists.
func (r *QuantDataStoreRepositoryImpl) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *QuantDataStoreRepositoryImpl) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*datastore.QuantDataStore, error) {
	whereClause, args := buildWhereClause(conditions...)
	orderClause := buildOrderClause(orderBy)
	limitClause := buildLimitClause(pagination)

	query := fmt.Sprintf("SELECT * FROM quant_data_stores%s%s%s", whereClause, orderClause, limitClause)

	var rows []dao.QuantDataStoreRow
	err := r.db.DB.Select(&rows, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*datastore.QuantDataStore{}, nil
		}
		return nil, fmt.Errorf("failed to find quant_data_stores: %w", err)
	}

	entities := make([]*datastore.QuantDataStore, len(rows))
	for i, row := range rows {
		entities[i] = &datastore.QuantDataStore{
			ID:          shared.ID(row.ID),
			Name:        row.Name,
			Description: row.Description,
			Type:        datastore.DataStoreType(row.Type),
			DSN:         row.DSN,
			StoragePath: row.StoragePath,
			Status:      shared.Status(row.Status),
			CreatedAt:   shared.Timestamp(row.CreatedAt),
			UpdatedAt:   shared.Timestamp(row.UpdatedAt),
		}
	}
	return entities, nil
}

// DataTypeMappingRuleRepositoryImpl implements datastore.DataTypeMappingRuleRepository.
type DataTypeMappingRuleRepositoryImpl struct {
	db             *persistence.DB
	mappingRuleDAO *dao.DataTypeMappingRuleDAO
}

// NewDataTypeMappingRuleRepository creates a new DataTypeMappingRuleRepositoryImpl.
func NewDataTypeMappingRuleRepository(db *persistence.DB) *DataTypeMappingRuleRepositoryImpl {
	return &DataTypeMappingRuleRepositoryImpl{
		db:             db,
		mappingRuleDAO: dao.NewDataTypeMappingRuleDAO(db.DB),
	}
}

// Create creates a new mapping rule.
func (r *DataTypeMappingRuleRepositoryImpl) Create(rule *datastore.DataTypeMappingRule) error {
	return r.mappingRuleDAO.Create(nil, rule)
}

// Get retrieves a mapping rule by ID.
func (r *DataTypeMappingRuleRepositoryImpl) Get(id shared.ID) (*datastore.DataTypeMappingRule, error) {
	return r.mappingRuleDAO.GetByID(nil, id)
}

// GetBySourceAndTarget retrieves rules by source and target types.
func (r *DataTypeMappingRuleRepositoryImpl) GetBySourceAndTarget(dataSourceType, targetDBType string) ([]*datastore.DataTypeMappingRule, error) {
	return r.mappingRuleDAO.GetBySourceAndTarget(nil, dataSourceType, targetDBType)
}

// SaveBatch saves multiple rules in a batch.
func (r *DataTypeMappingRuleRepositoryImpl) SaveBatch(rules []*datastore.DataTypeMappingRule) error {
	return r.db.ExecInTx(func(tx *sqlx.Tx) error {
		return r.mappingRuleDAO.SaveBatch(tx, rules)
	})
}

// InitDefaultRules initializes default mapping rules.
func (r *DataTypeMappingRuleRepositoryImpl) InitDefaultRules() error {
	// Default mapping rules for Tushare -> DuckDB
	defaultRules := []*datastore.DataTypeMappingRule{
		datastore.NewDataTypeMappingRule("tushare", "str", "duckdb", "VARCHAR", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "float", "duckdb", "DOUBLE", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "int", "duckdb", "INTEGER", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "datetime", "duckdb", "TIMESTAMP", 100, true),
		datastore.NewDataTypeMappingRule("tushare", "date", "duckdb", "DATE", 100, true),
	}

	return r.SaveBatch(defaultRules)
}

// List retrieves all mapping rules.
func (r *DataTypeMappingRuleRepositoryImpl) List() ([]*datastore.DataTypeMappingRule, error) {
	return r.mappingRuleDAO.ListAll(nil)
}

// Update updates an existing rule.
func (r *DataTypeMappingRuleRepositoryImpl) Update(rule *datastore.DataTypeMappingRule) error {
	return r.mappingRuleDAO.Update(nil, rule)
}

// Delete deletes a rule by ID.
func (r *DataTypeMappingRuleRepositoryImpl) Delete(id shared.ID) error {
	return r.mappingRuleDAO.DeleteByID(nil, id)
}

// ==================== Extended Query Operations ====================

// FindBy retrieves entities matching the given conditions.
func (r *DataTypeMappingRuleRepositoryImpl) FindBy(conditions ...shared.QueryCondition) ([]*datastore.DataTypeMappingRule, error) {
	return r.findByInternal(nil, nil, conditions...)
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *DataTypeMappingRuleRepositoryImpl) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*datastore.DataTypeMappingRule, error) {
	return r.findByInternal(orderBy, nil, conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *DataTypeMappingRuleRepositoryImpl) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[datastore.DataTypeMappingRule], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *DataTypeMappingRuleRepositoryImpl) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[datastore.DataTypeMappingRule], error) {
	total, err := r.Count(conditions...)
	if err != nil {
		return nil, fmt.Errorf("failed to count entities: %w", err)
	}

	items, err := r.findByInternal(nil, &pagination, conditions...)
	if err != nil {
		return nil, err
	}

	return shared.NewPageResult(items, total, pagination), nil
}

// Count returns the total count of entities matching conditions.
func (r *DataTypeMappingRuleRepositoryImpl) Count(conditions ...shared.QueryCondition) (int64, error) {
	whereClause, args := buildWhereClause(conditions...)
	query := fmt.Sprintf("SELECT COUNT(*) FROM data_type_mapping_rules%s", whereClause)

	var count int64
	err := r.db.DB.Get(&count, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to count data_type_mapping_rules: %w", err)
	}
	return count, nil
}

// Exists checks if any entity matching conditions exists.
func (r *DataTypeMappingRuleRepositoryImpl) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *DataTypeMappingRuleRepositoryImpl) findByInternal(orderBy []shared.OrderBy, pagination *shared.Pagination, conditions ...shared.QueryCondition) ([]*datastore.DataTypeMappingRule, error) {
	whereClause, args := buildWhereClause(conditions...)
	orderClause := buildOrderClause(orderBy)
	limitClause := buildLimitClause(pagination)

	query := fmt.Sprintf("SELECT * FROM data_type_mapping_rules%s%s%s", whereClause, orderClause, limitClause)

	var rows []dao.DataTypeMappingRuleRow
	err := r.db.DB.Select(&rows, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*datastore.DataTypeMappingRule{}, nil
		}
		return nil, fmt.Errorf("failed to find data_type_mapping_rules: %w", err)
	}

	entities := make([]*datastore.DataTypeMappingRule, len(rows))
	for i, row := range rows {
		entities[i] = &datastore.DataTypeMappingRule{
			ID:             shared.ID(row.ID),
			DataSourceType: row.DataSourceType,
			SourceType:     row.SourceType,
			TargetDBType:   row.TargetDBType,
			TargetType:     row.TargetType,
			Priority:       row.Priority,
			IsDefault:      row.IsDefault,
			CreatedAt:      shared.Timestamp(row.CreatedAt),
			UpdatedAt:      shared.Timestamp(row.UpdatedAt),
		}
	}
	return entities, nil
}
