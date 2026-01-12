package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// SyncJobDAO provides data access operations for SyncJob.
type SyncJobDAO struct {
	*SQLBaseDAO[SyncJobRow]
}

// NewSyncJobDAO creates a new SyncJobDAO.
func NewSyncJobDAO(db *sqlx.DB) *SyncJobDAO {
	return &SyncJobDAO{
		SQLBaseDAO: NewSQLBaseDAO[SyncJobRow](db, "sync_jobs", "id"),
	}
}

// Create inserts a new sync job record.
func (d *SyncJobDAO) Create(tx *sqlx.Tx, entity *sync.SyncJob) error {
	query := `INSERT INTO sync_jobs (id, name, description, api_meta_id, data_store_id, workflow_def_id, mode,
		cron_expression, params, param_rules, status, last_run_at, next_run_at, created_at, updated_at)
		VALUES (:id, :name, :description, :api_meta_id, :data_store_id, :workflow_def_id, :mode,
		:cron_expression, :params, :param_rules, :status, :last_run_at, :next_run_at, :created_at, :updated_at)`

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
		return fmt.Errorf("failed to create sync job: %w", err)
	}
	return nil
}

// GetByID retrieves a sync job by ID.
func (d *SyncJobDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*sync.SyncJob, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing sync job record.
func (d *SyncJobDAO) Update(tx *sqlx.Tx, entity *sync.SyncJob) error {
	query := `UPDATE sync_jobs SET
		name = :name, description = :description, mode = :mode, cron_expression = :cron_expression,
		params = :params, param_rules = :param_rules, status = :status, last_run_at = :last_run_at,
		next_run_at = :next_run_at, updated_at = :updated_at
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
		return fmt.Errorf("failed to update sync job: %w", err)
	}
	return nil
}

// DeleteByID deletes a sync job by ID.
func (d *SyncJobDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListAll retrieves all sync jobs.
func (d *SyncJobDAO) ListAll(tx *sqlx.Tx) ([]*sync.SyncJob, error) {
	rows, err := d.List(tx)
	if err != nil {
		return nil, err
	}

	entities := make([]*sync.SyncJob, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// toRow converts domain entity to database row.
func (d *SyncJobDAO) toRow(entity *sync.SyncJob) (*SyncJobRow, error) {
	params, err := entity.MarshalParamsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	paramRules, err := entity.MarshalParamRulesJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal param rules: %w", err)
	}

	row := &SyncJobRow{
		ID:            entity.ID.String(),
		Name:          entity.Name,
		Description:   entity.Description,
		APIMetadataID: entity.APIMetadataID.String(),
		DataStoreID:   entity.DataStoreID.String(),
		WorkflowDefID: entity.WorkflowDefID.String(),
		Mode:          entity.Mode.String(),
		Params:        params,
		ParamRules:    paramRules,
		Status:        entity.Status.String(),
		CreatedAt:     entity.CreatedAt.ToTime(),
		UpdatedAt:     entity.UpdatedAt.ToTime(),
	}

	if entity.CronExpression != nil {
		row.CronExpression = sql.NullString{String: *entity.CronExpression, Valid: true}
	}

	if entity.LastRunAt != nil {
		row.LastRunAt = sql.NullTime{Time: *entity.LastRunAt, Valid: true}
	}

	if entity.NextRunAt != nil {
		row.NextRunAt = sql.NullTime{Time: *entity.NextRunAt, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *SyncJobDAO) toEntity(row *SyncJobRow) (*sync.SyncJob, error) {
	entity := &sync.SyncJob{
		ID:            shared.ID(row.ID),
		Name:          row.Name,
		Description:   row.Description,
		APIMetadataID: shared.ID(row.APIMetadataID),
		DataStoreID:   shared.ID(row.DataStoreID),
		WorkflowDefID: shared.ID(row.WorkflowDefID),
		Mode:          sync.SyncMode(row.Mode),
		Status:        sync.JobStatus(row.Status),
		CreatedAt:     shared.Timestamp(row.CreatedAt),
		UpdatedAt:     shared.Timestamp(row.UpdatedAt),
	}

	if row.CronExpression.Valid {
		entity.CronExpression = &row.CronExpression.String
	}

	if row.LastRunAt.Valid {
		entity.LastRunAt = &row.LastRunAt.Time
	}

	if row.NextRunAt.Valid {
		entity.NextRunAt = &row.NextRunAt.Time
	}

	if row.Params != "" {
		if err := entity.UnmarshalParamsJSON(row.Params); err != nil {
			return nil, fmt.Errorf("failed to unmarshal params: %w", err)
		}
	} else {
		entity.Params = make(map[string]interface{})
	}

	if row.ParamRules != "" {
		if err := entity.UnmarshalParamRulesJSON(row.ParamRules); err != nil {
			return nil, fmt.Errorf("failed to unmarshal param rules: %w", err)
		}
	} else {
		entity.ParamRules = []sync.ParamRule{}
	}

	return entity, nil
}
