package dao

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

// QuantDataStoreDAO provides data access operations for QuantDataStore.
type QuantDataStoreDAO struct {
	*SQLBaseDAO[QuantDataStoreRow]
}

// NewQuantDataStoreDAO creates a new QuantDataStoreDAO.
func NewQuantDataStoreDAO(db *sqlx.DB) *QuantDataStoreDAO {
	return &QuantDataStoreDAO{
		SQLBaseDAO: NewSQLBaseDAO[QuantDataStoreRow](db, "quant_data_stores", "id"),
	}
}

// Create inserts a new quant data store record.
func (d *QuantDataStoreDAO) Create(tx *sqlx.Tx, entity *datastore.QuantDataStore) error {
	query := `INSERT INTO quant_data_stores (id, name, description, type, dsn, storage_path, status, created_at, updated_at)
		VALUES (:id, :name, :description, :type, :dsn, :storage_path, :status, :created_at, :updated_at)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create quant data store: %w", err)
	}
	return nil
}

// GetByID retrieves a quant data store by ID.
func (d *QuantDataStoreDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*datastore.QuantDataStore, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// Update updates an existing quant data store record.
func (d *QuantDataStoreDAO) Update(tx *sqlx.Tx, entity *datastore.QuantDataStore) error {
	query := `UPDATE quant_data_stores SET
		name = :name, description = :description, type = :type, dsn = :dsn,
		storage_path = :storage_path, status = :status, updated_at = :updated_at
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update quant data store: %w", err)
	}
	return nil
}

// DeleteByID deletes a quant data store by ID.
func (d *QuantDataStoreDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListAll retrieves all quant data stores.
func (d *QuantDataStoreDAO) ListAll(tx *sqlx.Tx) ([]*datastore.QuantDataStore, error) {
	rows, err := d.List(tx)
	if err != nil {
		return nil, err
	}

	entities := make([]*datastore.QuantDataStore, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// toRow converts domain entity to database row.
func (d *QuantDataStoreDAO) toRow(entity *datastore.QuantDataStore) *QuantDataStoreRow {
	return &QuantDataStoreRow{
		ID:          entity.ID.String(),
		Name:        entity.Name,
		Description: entity.Description,
		Type:        entity.Type.String(),
		DSN:         entity.DSN,
		StoragePath: entity.StoragePath,
		Status:      entity.Status.String(),
		CreatedAt:   entity.CreatedAt.ToTime(),
		UpdatedAt:   entity.UpdatedAt.ToTime(),
	}
}

// toEntity converts database row to domain entity.
func (d *QuantDataStoreDAO) toEntity(row *QuantDataStoreRow) *datastore.QuantDataStore {
	return &datastore.QuantDataStore{
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
