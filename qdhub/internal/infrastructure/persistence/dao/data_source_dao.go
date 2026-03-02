package dao

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// DataSourceDAO provides data access operations for DataSource.
type DataSourceDAO struct {
	*SQLBaseDAO[DataSourceRow]
}

// NewDataSourceDAO creates a new DataSourceDAO.
func NewDataSourceDAO(db *sqlx.DB) *DataSourceDAO {
	return &DataSourceDAO{
		SQLBaseDAO: NewSQLBaseDAO[DataSourceRow](db, "data_sources", "id"),
	}
}

// Create inserts a new data source record.
func (d *DataSourceDAO) Create(tx *sqlx.Tx, entity *metadata.DataSource) error {
	query := `INSERT INTO data_sources (id, name, description, base_url, doc_url, status, common_data_apis, created_at, updated_at)
		VALUES (:id, :name, :description, :base_url, :doc_url, :status, :common_data_apis, :created_at, :updated_at)`

	row, err := d.toRow(entity)
	if err != nil {
		return fmt.Errorf("failed to marshal entity: %w", err)
	}
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}
	if err != nil {
		return fmt.Errorf("failed to create data source: %w", err)
	}
	return nil
}

// GetByID retrieves a data source by ID.
func (d *DataSourceDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*metadata.DataSource, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// Update updates an existing data source record.
func (d *DataSourceDAO) Update(tx *sqlx.Tx, entity *metadata.DataSource) error {
	query := `UPDATE data_sources SET
		name = :name, description = :description, base_url = :base_url,
		doc_url = :doc_url, status = :status, common_data_apis = :common_data_apis, updated_at = :updated_at
		WHERE id = :id`

	row, err := d.toRow(entity)
	if err != nil {
		return fmt.Errorf("failed to marshal entity: %w", err)
	}
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}
	if err != nil {
		return fmt.Errorf("failed to update data source: %w", err)
	}
	return nil
}

// ListAll retrieves all data sources.
func (d *DataSourceDAO) ListAll(tx *sqlx.Tx) ([]*metadata.DataSource, error) {
	rows, err := d.List(tx)
	if err != nil {
		return nil, err
	}

	entities := make([]*metadata.DataSource, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// DeleteByID deletes a data source by ID.
func (d *DataSourceDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// GetByName retrieves a data source by name.
func (d *DataSourceDAO) GetByName(tx *sqlx.Tx, name string) (*metadata.DataSource, error) {
	query := d.DB().Rebind(`SELECT * FROM data_sources WHERE name = ?`)
	var row DataSourceRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, name)
	} else {
		err = d.DB().Get(&row, query, name)
	}

	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get data source by name: %w", err)
	}
	return d.toEntity(&row), nil
}

// toRow converts domain entity to database row.
func (d *DataSourceDAO) toRow(entity *metadata.DataSource) (*DataSourceRow, error) {
	commonDataAPIs, err := entity.MarshalCommonDataAPIsJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal common_data_apis: %w", err)
	}
	commonDataAPIsNull := sql.NullString{}
	if commonDataAPIs != "" {
		commonDataAPIsNull = sql.NullString{String: commonDataAPIs, Valid: true}
	}
	return &DataSourceRow{
		ID:             entity.ID.String(),
		Name:           entity.Name,
		Description:    entity.Description,
		BaseURL:        entity.BaseURL,
		DocURL:         entity.DocURL,
		Status:         entity.Status.String(),
		CommonDataAPIs: commonDataAPIsNull,
		CreatedAt:      entity.CreatedAt.ToTime(),
		UpdatedAt:      entity.UpdatedAt.ToTime(),
	}, nil
}

// toEntity converts database row to domain entity.
func (d *DataSourceDAO) toEntity(row *DataSourceRow) *metadata.DataSource {
	entity := &metadata.DataSource{
		ID:          shared.ID(row.ID),
		Name:        row.Name,
		Description: row.Description,
		BaseURL:     row.BaseURL,
		DocURL:      row.DocURL,
		Status:      shared.Status(row.Status),
		CreatedAt:   shared.Timestamp(row.CreatedAt),
		UpdatedAt:   shared.Timestamp(row.UpdatedAt),
	}
	if row.CommonDataAPIs.Valid && row.CommonDataAPIs.String != "" {
		_ = entity.UnmarshalCommonDataAPIsJSON(row.CommonDataAPIs.String)
	}
	return entity
}

// GetCreatedAt helper to get created_at for testing.
func (d *DataSourceDAO) GetCreatedAt(t time.Time) shared.Timestamp {
	return shared.Timestamp(t)
}
