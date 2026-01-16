package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// APICategoryDAO provides data access operations for APICategory.
type APICategoryDAO struct {
	*SQLBaseDAO[APICategoryRow]
}

// NewAPICategoryDAO creates a new APICategoryDAO.
func NewAPICategoryDAO(db *sqlx.DB) *APICategoryDAO {
	return &APICategoryDAO{
		SQLBaseDAO: NewSQLBaseDAO[APICategoryRow](db, "api_categories", "id"),
	}
}

// Create inserts a new API category record.
func (d *APICategoryDAO) Create(tx *sqlx.Tx, entity *metadata.APICategory) error {
	query := `INSERT INTO api_categories (id, data_source_id, name, description, parent_id, sort_order, doc_path, created_at)
		VALUES (:id, :data_source_id, :name, :description, :parent_id, :sort_order, :doc_path, :created_at)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create api category: %w", err)
	}
	return nil
}

// GetByID retrieves an API category by ID.
func (d *APICategoryDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*metadata.APICategory, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// Update updates an existing API category record.
func (d *APICategoryDAO) Update(tx *sqlx.Tx, entity *metadata.APICategory) error {
	query := `UPDATE api_categories SET
		name = :name, description = :description, parent_id = :parent_id,
		sort_order = :sort_order, doc_path = :doc_path
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update api category: %w", err)
	}
	return nil
}

// DeleteByID deletes an API category by ID.
func (d *APICategoryDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListByDataSource retrieves all API categories for a data source.
func (d *APICategoryDAO) ListByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) ([]*metadata.APICategory, error) {
	query := d.DB().Rebind(`SELECT * FROM api_categories WHERE data_source_id = ? ORDER BY sort_order`)
	var rows []*APICategoryRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, dataSourceID.String())
	} else {
		err = d.DB().Select(&rows, query, dataSourceID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list api categories: %w", err)
	}

	entities := make([]*metadata.APICategory, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// DeleteByDataSource deletes all API categories for a data source.
func (d *APICategoryDAO) DeleteByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) error {
	query := d.DB().Rebind(`DELETE FROM api_categories WHERE data_source_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, dataSourceID.String())
	} else {
		_, err = d.DB().Exec(query, dataSourceID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete api categories by data source: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
func (d *APICategoryDAO) toRow(entity *metadata.APICategory) *APICategoryRow {
	row := &APICategoryRow{
		ID:           entity.ID.String(),
		DataSourceID: entity.DataSourceID.String(),
		Name:         entity.Name,
		Description:  entity.Description,
		SortOrder:    entity.SortOrder,
		DocPath:      entity.DocPath,
		CreatedAt:    entity.CreatedAt.ToTime(),
	}

	if entity.ParentID != nil {
		row.ParentID = sql.NullString{String: entity.ParentID.String(), Valid: true}
	}

	return row
}

// toEntity converts database row to domain entity.
func (d *APICategoryDAO) toEntity(row *APICategoryRow) *metadata.APICategory {
	entity := &metadata.APICategory{
		ID:           shared.ID(row.ID),
		DataSourceID: shared.ID(row.DataSourceID),
		Name:         row.Name,
		Description:  row.Description,
		SortOrder:    row.SortOrder,
		DocPath:      row.DocPath,
		CreatedAt:    shared.Timestamp(row.CreatedAt),
	}

	if row.ParentID.Valid {
		parentID := shared.ID(row.ParentID.String)
		entity.ParentID = &parentID
	}

	return entity
}
