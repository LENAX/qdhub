package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// APIMetadataDAO provides data access operations for APIMetadata.
type APIMetadataDAO struct {
	*SQLBaseDAO[APIMetadataRow]
}

// NewAPIMetadataDAO creates a new APIMetadataDAO.
func NewAPIMetadataDAO(db *sqlx.DB) *APIMetadataDAO {
	return &APIMetadataDAO{
		SQLBaseDAO: NewSQLBaseDAO[APIMetadataRow](db, "api_metadata", "id"),
	}
}

// Create inserts a new API metadata record.
func (d *APIMetadataDAO) Create(tx *sqlx.Tx, entity *metadata.APIMetadata) error {
	query := `INSERT INTO api_metadata (id, data_source_id, category_id, name, display_name, description,
		endpoint, request_params, response_fields, rate_limit, permission, param_dependencies, status, created_at, updated_at)
		VALUES (:id, :data_source_id, :category_id, :name, :display_name, :description,
		:endpoint, :request_params, :response_fields, :rate_limit, :permission, :param_dependencies, :status, :created_at, :updated_at)`

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
		return fmt.Errorf("failed to create api metadata: %w", err)
	}
	return nil
}

// GetByID retrieves an API metadata by ID.
func (d *APIMetadataDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*metadata.APIMetadata, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing API metadata record.
func (d *APIMetadataDAO) Update(tx *sqlx.Tx, entity *metadata.APIMetadata) error {
	query := `UPDATE api_metadata SET
		category_id = :category_id, name = :name, display_name = :display_name, description = :description,
		endpoint = :endpoint, request_params = :request_params, response_fields = :response_fields,
		rate_limit = :rate_limit, permission = :permission, param_dependencies = :param_dependencies, 
		status = :status, updated_at = :updated_at
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
		return fmt.Errorf("failed to update api metadata: %w", err)
	}
	return nil
}

// DeleteByID deletes an API metadata by ID.
func (d *APIMetadataDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListByDataSource retrieves all API metadata for a data source.
func (d *APIMetadataDAO) ListByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) ([]*metadata.APIMetadata, error) {
	query := d.DB().Rebind(`SELECT * FROM api_metadata WHERE data_source_id = ?`)
	var rows []*APIMetadataRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, dataSourceID.String())
	} else {
		err = d.DB().Select(&rows, query, dataSourceID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list api metadata: %w", err)
	}

	entities := make([]*metadata.APIMetadata, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// ListByCategory retrieves all API metadata for a category.
func (d *APIMetadataDAO) ListByCategory(tx *sqlx.Tx, categoryID shared.ID) ([]*metadata.APIMetadata, error) {
	query := d.DB().Rebind(`SELECT * FROM api_metadata WHERE category_id = ?`)
	var rows []*APIMetadataRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, categoryID.String())
	} else {
		err = d.DB().Select(&rows, query, categoryID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list api metadata by category: %w", err)
	}

	entities := make([]*metadata.APIMetadata, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// DeleteByDataSource deletes all API metadata for a data source.
func (d *APIMetadataDAO) DeleteByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) error {
	query := d.DB().Rebind(`DELETE FROM api_metadata WHERE data_source_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, dataSourceID.String())
	} else {
		_, err = d.DB().Exec(query, dataSourceID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete api metadata by data source: %w", err)
	}
	return nil
}

// GetByDataSourceAndName retrieves an API metadata by data source ID and name.
// This is used to check for duplicates before insert (unique constraint: data_source_id, name).
func (d *APIMetadataDAO) GetByDataSourceAndName(tx *sqlx.Tx, dataSourceID shared.ID, name string) (*metadata.APIMetadata, error) {
	query := d.DB().Rebind(`SELECT * FROM api_metadata WHERE data_source_id = ? AND name = ?`)
	var row APIMetadataRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, dataSourceID.String(), name)
	} else {
		err = d.DB().Get(&row, query, dataSourceID.String(), name)
	}

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get api metadata by data source and name: %w", err)
	}
	return d.toEntity(&row)
}

// toRow converts domain entity to database row.
func (d *APIMetadataDAO) toRow(entity *metadata.APIMetadata) (*APIMetadataRow, error) {
	requestParams, err := entity.MarshalRequestParamsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request params: %w", err)
	}

	responseFields, err := entity.MarshalResponseFieldsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response fields: %w", err)
	}

	rateLimit, err := entity.MarshalRateLimitJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal rate limit: %w", err)
	}

	paramDependencies, err := entity.MarshalParamDependenciesJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal param dependencies: %w", err)
	}

	row := &APIMetadataRow{
		ID:             entity.ID.String(),
		DataSourceID:   entity.DataSourceID.String(),
		Name:           entity.Name,
		DisplayName:    entity.DisplayName,
		Description:    entity.Description,
		Endpoint:       entity.Endpoint,
		RequestParams:  requestParams,
		ResponseFields: responseFields,
		RateLimit:      rateLimit,
		Permission:     entity.Permission,
		Status:         entity.Status.String(),
		CreatedAt:      entity.CreatedAt.ToTime(),
		UpdatedAt:      entity.UpdatedAt.ToTime(),
	}

	if entity.CategoryID != nil {
		row.CategoryID = sql.NullString{String: entity.CategoryID.String(), Valid: true}
	}

	if paramDependencies != "" {
		row.ParamDependencies = sql.NullString{String: paramDependencies, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *APIMetadataDAO) toEntity(row *APIMetadataRow) (*metadata.APIMetadata, error) {
	entity := &metadata.APIMetadata{
		ID:           shared.ID(row.ID),
		DataSourceID: shared.ID(row.DataSourceID),
		Name:         row.Name,
		DisplayName:  row.DisplayName,
		Description:  row.Description,
		Endpoint:     row.Endpoint,
		Permission:   row.Permission,
		Status:       shared.Status(row.Status),
		CreatedAt:    shared.Timestamp(row.CreatedAt),
		UpdatedAt:    shared.Timestamp(row.UpdatedAt),
	}

	if row.CategoryID.Valid {
		categoryID := shared.ID(row.CategoryID.String)
		entity.CategoryID = &categoryID
	}

	if row.RequestParams != "" {
		if err := entity.UnmarshalRequestParamsJSON(row.RequestParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal request params: %w", err)
		}
	}

	if row.ResponseFields != "" {
		if err := entity.UnmarshalResponseFieldsJSON(row.ResponseFields); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response fields: %w", err)
		}
	}

	if row.RateLimit != "" {
		if err := entity.UnmarshalRateLimitJSON(row.RateLimit); err != nil {
			return nil, fmt.Errorf("failed to unmarshal rate limit: %w", err)
		}
	}

	if row.ParamDependencies.Valid && row.ParamDependencies.String != "" {
		if err := entity.UnmarshalParamDependenciesJSON(row.ParamDependencies.String); err != nil {
			return nil, fmt.Errorf("failed to unmarshal param dependencies: %w", err)
		}
	}

	return entity, nil
}
