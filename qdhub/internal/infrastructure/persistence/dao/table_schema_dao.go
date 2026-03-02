package dao

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// TableSchemaDAO provides data access operations for TableSchema.
type TableSchemaDAO struct {
	*SQLBaseDAO[TableSchemaRow]
}

// NewTableSchemaDAO creates a new TableSchemaDAO.
func NewTableSchemaDAO(db *sqlx.DB) *TableSchemaDAO {
	return &TableSchemaDAO{
		SQLBaseDAO: NewSQLBaseDAO[TableSchemaRow](db, "table_schemas", "id"),
	}
}

// Create inserts a new table schema record.
func (d *TableSchemaDAO) Create(tx *sqlx.Tx, entity *datastore.TableSchema) error {
	query := `INSERT INTO table_schemas (id, data_store_id, api_meta_id, table_name, columns, primary_keys, indexes, status, error_message, created_at, updated_at)
		VALUES (:id, :data_store_id, :api_meta_id, :table_name, :columns, :primary_keys, :indexes, :status, :error_message, :created_at, :updated_at)`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create table schema: %w", err)
	}
	return nil
}

// GetByID retrieves a table schema by ID.
func (d *TableSchemaDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*datastore.TableSchema, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing table schema record.
func (d *TableSchemaDAO) Update(tx *sqlx.Tx, entity *datastore.TableSchema) error {
	query := `UPDATE table_schemas SET
		table_name = :table_name, columns = :columns, primary_keys = :primary_keys,
		indexes = :indexes, status = :status, error_message = :error_message, updated_at = :updated_at
		WHERE id = :id`

	row, err := d.toRow(entity)
	if err != nil {
		return err
	}

	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update table schema: %w", err)
	}
	return nil
}

// DeleteByID deletes a table schema by ID.
func (d *TableSchemaDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// GetByDataStore retrieves all table schemas for a data store.
func (d *TableSchemaDAO) GetByDataStore(tx *sqlx.Tx, dataStoreID shared.ID) ([]*datastore.TableSchema, error) {
	query := d.DB().Rebind(`SELECT * FROM table_schemas WHERE data_store_id = ?`)
	var rows []*TableSchemaRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, dataStoreID.String())
	} else {
		err = d.DB().Select(&rows, query, dataStoreID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get table schemas by data store: %w", err)
	}

	entities := make([]*datastore.TableSchema, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// GetByAPIMetadata retrieves a table schema by API metadata ID.
func (d *TableSchemaDAO) GetByAPIMetadata(tx *sqlx.Tx, apiMetadataID shared.ID) (*datastore.TableSchema, error) {
	query := d.DB().Rebind(`SELECT * FROM table_schemas WHERE api_meta_id = ?`)
	var row TableSchemaRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, apiMetadataID.String())
	} else {
		err = d.DB().Get(&row, query, apiMetadataID.String())
	}

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema by api metadata: %w", err)
	}

	return d.toEntity(&row)
}

// DeleteByDataStore deletes all table schemas for a data store.
func (d *TableSchemaDAO) DeleteByDataStore(tx *sqlx.Tx, dataStoreID shared.ID) error {
	query := d.DB().Rebind(`DELETE FROM table_schemas WHERE data_store_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, dataStoreID.String())
	} else {
		_, err = d.DB().Exec(query, dataStoreID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete table schemas by data store: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
func (d *TableSchemaDAO) toRow(entity *datastore.TableSchema) (*TableSchemaRow, error) {
	columns, err := entity.MarshalColumnsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal columns: %w", err)
	}

	primaryKeys, err := json.Marshal(entity.PrimaryKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal primary keys: %w", err)
	}

	indexes, err := entity.MarshalIndexesJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal indexes: %w", err)
	}

	row := &TableSchemaRow{
		ID:            entity.ID.String(),
		DataStoreID:   entity.DataStoreID.String(),
		APIMetadataID: entity.APIMetadataID.String(),
		TableName:     entity.TableName,
		Columns:       columns,
		PrimaryKeys:   string(primaryKeys),
		Indexes:       indexes,
		Status:        entity.Status.String(),
		CreatedAt:     entity.CreatedAt.ToTime(),
		UpdatedAt:     entity.UpdatedAt.ToTime(),
	}

	if entity.ErrorMessage != nil {
		row.ErrorMessage = sql.NullString{String: *entity.ErrorMessage, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *TableSchemaDAO) toEntity(row *TableSchemaRow) (*datastore.TableSchema, error) {
	entity := &datastore.TableSchema{
		ID:            shared.ID(row.ID),
		DataStoreID:   shared.ID(row.DataStoreID),
		APIMetadataID: shared.ID(row.APIMetadataID),
		TableName:     row.TableName,
		Status:        datastore.SchemaStatus(row.Status),
		CreatedAt:     shared.Timestamp(row.CreatedAt),
		UpdatedAt:     shared.Timestamp(row.UpdatedAt),
	}

	if row.ErrorMessage.Valid {
		entity.ErrorMessage = &row.ErrorMessage.String
	}

	if row.Columns != "" {
		if err := entity.UnmarshalColumnsJSON(row.Columns); err != nil {
			return nil, fmt.Errorf("failed to unmarshal columns: %w", err)
		}
	}

	if row.PrimaryKeys != "" {
		if err := json.Unmarshal([]byte(row.PrimaryKeys), &entity.PrimaryKeys); err != nil {
			return nil, fmt.Errorf("failed to unmarshal primary keys: %w", err)
		}
	}

	if row.Indexes != "" {
		if err := entity.UnmarshalIndexesJSON(row.Indexes); err != nil {
			return nil, fmt.Errorf("failed to unmarshal indexes: %w", err)
		}
	}

	return entity, nil
}
