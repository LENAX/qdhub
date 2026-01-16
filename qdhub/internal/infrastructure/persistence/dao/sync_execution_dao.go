package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

// SyncExecutionDAO provides data access operations for SyncExecution.
type SyncExecutionDAO struct {
	*SQLBaseDAO[SyncExecutionRow]
}

// NewSyncExecutionDAO creates a new SyncExecutionDAO.
func NewSyncExecutionDAO(db *sqlx.DB) *SyncExecutionDAO {
	return &SyncExecutionDAO{
		SQLBaseDAO: NewSQLBaseDAO[SyncExecutionRow](db, "sync_executions", "id"),
	}
}

// Create inserts a new sync execution record.
func (d *SyncExecutionDAO) Create(tx *sqlx.Tx, entity *sync.SyncExecution) error {
	query := `INSERT INTO sync_executions (id, sync_job_id, workflow_inst_id, status, started_at, finished_at, record_count, error_message)
		VALUES (:id, :sync_job_id, :workflow_inst_id, :status, :started_at, :finished_at, :record_count, :error_message)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create sync execution: %w", err)
	}
	return nil
}

// GetByID retrieves a sync execution by ID.
func (d *SyncExecutionDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*sync.SyncExecution, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// Update updates an existing sync execution record.
func (d *SyncExecutionDAO) Update(tx *sqlx.Tx, entity *sync.SyncExecution) error {
	query := `UPDATE sync_executions SET
		status = :status, finished_at = :finished_at, record_count = :record_count, error_message = :error_message
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update sync execution: %w", err)
	}
	return nil
}

// DeleteByID deletes a sync execution by ID.
func (d *SyncExecutionDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// GetBySyncJob retrieves all sync executions for a sync job.
func (d *SyncExecutionDAO) GetBySyncJob(tx *sqlx.Tx, syncJobID shared.ID) ([]*sync.SyncExecution, error) {
	query := d.DB().Rebind(`SELECT * FROM sync_executions WHERE sync_job_id = ? ORDER BY started_at DESC`)
	var rows []*SyncExecutionRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, syncJobID.String())
	} else {
		err = d.DB().Select(&rows, query, syncJobID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get sync executions: %w", err)
	}

	entities := make([]*sync.SyncExecution, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// DeleteBySyncJob deletes all sync executions for a sync job.
func (d *SyncExecutionDAO) DeleteBySyncJob(tx *sqlx.Tx, syncJobID shared.ID) error {
	query := d.DB().Rebind(`DELETE FROM sync_executions WHERE sync_job_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, syncJobID.String())
	} else {
		_, err = d.DB().Exec(query, syncJobID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete sync executions by sync job: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
func (d *SyncExecutionDAO) toRow(entity *sync.SyncExecution) *SyncExecutionRow {
	row := &SyncExecutionRow{
		ID:             entity.ID.String(),
		SyncJobID:      entity.SyncJobID.String(),
		WorkflowInstID: entity.WorkflowInstID.String(),
		Status:         entity.Status.String(),
		StartedAt:      entity.StartedAt.ToTime(),
		RecordCount:    entity.RecordCount,
	}

	if entity.FinishedAt != nil {
		row.FinishedAt = sql.NullTime{Time: entity.FinishedAt.ToTime(), Valid: true}
	}

	if entity.ErrorMessage != nil {
		row.ErrorMessage = sql.NullString{String: *entity.ErrorMessage, Valid: true}
	}

	return row
}

// toEntity converts database row to domain entity.
func (d *SyncExecutionDAO) toEntity(row *SyncExecutionRow) *sync.SyncExecution {
	entity := &sync.SyncExecution{
		ID:             shared.ID(row.ID),
		SyncJobID:      shared.ID(row.SyncJobID),
		WorkflowInstID: shared.ID(row.WorkflowInstID),
		Status:         sync.ExecStatus(row.Status),
		StartedAt:      shared.Timestamp(row.StartedAt),
		RecordCount:    row.RecordCount,
	}

	if row.FinishedAt.Valid {
		ts := shared.Timestamp(row.FinishedAt.Time)
		entity.FinishedAt = &ts
	}

	if row.ErrorMessage.Valid {
		entity.ErrorMessage = &row.ErrorMessage.String
	}

	return entity
}
