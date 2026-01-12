package repository

import (
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

// TableSchemaRepositoryImpl implements datastore.TableSchemaRepository.
type TableSchemaRepositoryImpl struct {
	db             *persistence.DB
	tableSchemaDAO *dao.TableSchemaDAO
}

// NewTableSchemaRepository creates a new TableSchemaRepositoryImpl.
func NewTableSchemaRepository(db *persistence.DB) *TableSchemaRepositoryImpl {
	return &TableSchemaRepositoryImpl{
		db:             db,
		tableSchemaDAO: dao.NewTableSchemaDAO(db.DB),
	}
}

// Create creates a new table schema.
func (r *TableSchemaRepositoryImpl) Create(schema *datastore.TableSchema) error {
	return r.tableSchemaDAO.Create(nil, schema)
}

// Get retrieves a table schema by ID.
func (r *TableSchemaRepositoryImpl) Get(id shared.ID) (*datastore.TableSchema, error) {
	return r.tableSchemaDAO.GetByID(nil, id)
}

// GetByDataStore retrieves all table schemas for a data store.
func (r *TableSchemaRepositoryImpl) GetByDataStore(dataStoreID shared.ID) ([]*datastore.TableSchema, error) {
	return r.tableSchemaDAO.GetByDataStore(nil, dataStoreID)
}

// GetByAPIMetadata retrieves a table schema by API metadata ID.
func (r *TableSchemaRepositoryImpl) GetByAPIMetadata(apiMetadataID shared.ID) (*datastore.TableSchema, error) {
	return r.tableSchemaDAO.GetByAPIMetadata(nil, apiMetadataID)
}

// Update updates a table schema.
func (r *TableSchemaRepositoryImpl) Update(schema *datastore.TableSchema) error {
	return r.tableSchemaDAO.Update(nil, schema)
}

// Delete deletes a table schema.
func (r *TableSchemaRepositoryImpl) Delete(id shared.ID) error {
	return r.tableSchemaDAO.DeleteByID(nil, id)
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
