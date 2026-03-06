package dao

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// APISyncStrategyDAO provides data access operations for APISyncStrategy.
type APISyncStrategyDAO struct {
	*SQLBaseDAO[APISyncStrategyRow]
}

// NewAPISyncStrategyDAO creates a new APISyncStrategyDAO.
func NewAPISyncStrategyDAO(db *sqlx.DB) *APISyncStrategyDAO {
	return &APISyncStrategyDAO{
		SQLBaseDAO: NewSQLBaseDAO[APISyncStrategyRow](db, "api_sync_strategies", "id"),
	}
}

// Create inserts a new API sync strategy record.
func (d *APISyncStrategyDAO) Create(tx *sqlx.Tx, entity *metadata.APISyncStrategy) error {
	query := `INSERT INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, 
		support_date_range, required_params, dependencies, fixed_params, fixed_param_keys, description, created_at, updated_at)
		VALUES (:id, :data_source_id, :api_name, :preferred_param, 
		:support_date_range, :required_params, :dependencies, :fixed_params, :fixed_param_keys, :description, :created_at, :updated_at)`

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
		return fmt.Errorf("failed to create api sync strategy: %w", err)
	}
	return nil
}

// GetByID retrieves an API sync strategy by ID.
func (d *APISyncStrategyDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*metadata.APISyncStrategy, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing API sync strategy record.
func (d *APISyncStrategyDAO) Update(tx *sqlx.Tx, entity *metadata.APISyncStrategy) error {
	query := `UPDATE api_sync_strategies SET
		preferred_param = :preferred_param, support_date_range = :support_date_range,
		required_params = :required_params, dependencies = :dependencies,
		fixed_params = :fixed_params, fixed_param_keys = :fixed_param_keys,
		description = :description, updated_at = :updated_at
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
		return fmt.Errorf("failed to update api sync strategy: %w", err)
	}
	return nil
}

// DeleteByID deletes an API sync strategy by ID.
func (d *APISyncStrategyDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// GetByDataSourceAndAPIName retrieves an API sync strategy by data source ID and API name.
func (d *APISyncStrategyDAO) GetByDataSourceAndAPIName(tx *sqlx.Tx, dataSourceID shared.ID, apiName string) (*metadata.APISyncStrategy, error) {
	query := d.DB().Rebind(`SELECT * FROM api_sync_strategies WHERE data_source_id = ? AND api_name = ?`)
	var row APISyncStrategyRow

	var err error
	if tx != nil {
		err = tx.Get(&row, query, dataSourceID.String(), apiName)
	} else {
		err = d.DB().Get(&row, query, dataSourceID.String(), apiName)
	}

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get api sync strategy by data source and api name: %w", err)
	}
	return d.toEntity(&row)
}

// ListByDataSource retrieves all API sync strategies for a data source.
func (d *APISyncStrategyDAO) ListByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) ([]*metadata.APISyncStrategy, error) {
	query := d.DB().Rebind(`SELECT * FROM api_sync_strategies WHERE data_source_id = ?`)
	var rows []*APISyncStrategyRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, dataSourceID.String())
	} else {
		err = d.DB().Select(&rows, query, dataSourceID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list api sync strategies: %w", err)
	}

	entities := make([]*metadata.APISyncStrategy, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// ListByAPINames retrieves API sync strategies for specific API names.
func (d *APISyncStrategyDAO) ListByAPINames(tx *sqlx.Tx, dataSourceID shared.ID, apiNames []string) ([]*metadata.APISyncStrategy, error) {
	if len(apiNames) == 0 {
		return []*metadata.APISyncStrategy{}, nil
	}

	query, args, err := sqlx.In(`SELECT * FROM api_sync_strategies WHERE data_source_id = ? AND api_name IN (?)`, dataSourceID.String(), apiNames)
	if err != nil {
		return nil, fmt.Errorf("failed to build IN query: %w", err)
	}
	query = d.DB().Rebind(query)

	var rows []*APISyncStrategyRow
	if tx != nil {
		err = tx.Select(&rows, query, args...)
	} else {
		err = d.DB().Select(&rows, query, args...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list api sync strategies by api names: %w", err)
	}

	entities := make([]*metadata.APISyncStrategy, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// DeleteByDataSource deletes all API sync strategies for a data source.
func (d *APISyncStrategyDAO) DeleteByDataSource(tx *sqlx.Tx, dataSourceID shared.ID) error {
	query := d.DB().Rebind(`DELETE FROM api_sync_strategies WHERE data_source_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, dataSourceID.String())
	} else {
		_, err = d.DB().Exec(query, dataSourceID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete api sync strategies by data source: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
func (d *APISyncStrategyDAO) toRow(entity *metadata.APISyncStrategy) (*APISyncStrategyRow, error) {
	row := &APISyncStrategyRow{
		ID:               entity.ID.String(),
		DataSourceID:     entity.DataSourceID.String(),
		APIName:          entity.APIName,
		PreferredParam:   string(entity.PreferredParam),
		SupportDateRange: boolToInt(entity.SupportDateRange),
		CreatedAt:        entity.CreatedAt.ToTime(),
		UpdatedAt:        entity.UpdatedAt.ToTime(),
	}

	if len(entity.RequiredParams) > 0 {
		data, err := json.Marshal(entity.RequiredParams)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal required params: %w", err)
		}
		row.RequiredParams = sql.NullString{String: string(data), Valid: true}
	}

	if len(entity.Dependencies) > 0 {
		data, err := json.Marshal(entity.Dependencies)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal dependencies: %w", err)
		}
		row.Dependencies = sql.NullString{String: string(data), Valid: true}
	}

	if entity.Description != "" {
		row.Description = sql.NullString{String: entity.Description, Valid: true}
	}

	if len(entity.FixedParams) > 0 {
		data, err := json.Marshal(entity.FixedParams)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal fixed params: %w", err)
		}
		row.FixedParams = sql.NullString{String: string(data), Valid: true}
	}

	if len(entity.FixedParamKeys) > 0 {
		data, err := json.Marshal(entity.FixedParamKeys)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal fixed param keys: %w", err)
		}
		row.FixedParamKeys = sql.NullString{String: string(data), Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *APISyncStrategyDAO) toEntity(row *APISyncStrategyRow) (*metadata.APISyncStrategy, error) {
	entity := &metadata.APISyncStrategy{
		ID:               shared.ID(row.ID),
		DataSourceID:     shared.ID(row.DataSourceID),
		APIName:          row.APIName,
		PreferredParam:   metadata.SyncParamType(row.PreferredParam),
		SupportDateRange: row.SupportDateRange == 1,
		CreatedAt:        shared.Timestamp(row.CreatedAt),
		UpdatedAt:        shared.Timestamp(row.UpdatedAt),
	}

	if row.RequiredParams.Valid && row.RequiredParams.String != "" {
		var requiredParams []string
		if err := json.Unmarshal([]byte(row.RequiredParams.String), &requiredParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal required params: %w", err)
		}
		entity.RequiredParams = requiredParams
	}

	if row.Dependencies.Valid && row.Dependencies.String != "" {
		var dependencies []string
		if err := json.Unmarshal([]byte(row.Dependencies.String), &dependencies); err != nil {
			return nil, fmt.Errorf("failed to unmarshal dependencies: %w", err)
		}
		entity.Dependencies = dependencies
	}

	if row.Description.Valid {
		entity.Description = row.Description.String
	}

	if row.FixedParams.Valid && row.FixedParams.String != "" {
		var fixed map[string]any
		if err := json.Unmarshal([]byte(row.FixedParams.String), &fixed); err != nil {
			return nil, fmt.Errorf("failed to unmarshal fixed params: %w", err)
		}
		entity.FixedParams = fixed
	}

	if row.FixedParamKeys.Valid && row.FixedParamKeys.String != "" {
		var keys []string
		if err := json.Unmarshal([]byte(row.FixedParamKeys.String), &keys); err != nil {
			return nil, fmt.Errorf("failed to unmarshal fixed param keys: %w", err)
		}
		entity.FixedParamKeys = keys
	}

	return entity, nil
}

// boolToInt converts bool to int (0 or 1).
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
