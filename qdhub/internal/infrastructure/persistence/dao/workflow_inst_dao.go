package dao

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	qdhubworkflow "qdhub/internal/domain/workflow"
)

// WorkflowInstanceDAO provides data access operations for WorkflowInstance.
type WorkflowInstanceDAO struct {
	*SQLBaseDAO[WorkflowInstanceRow]
}

// NewWorkflowInstanceDAO creates a new WorkflowInstanceDAO.
func NewWorkflowInstanceDAO(db *sqlx.DB) *WorkflowInstanceDAO {
	return &WorkflowInstanceDAO{
		SQLBaseDAO: NewSQLBaseDAO[WorkflowInstanceRow](db, "workflow_instances", "id"),
	}
}

// Create inserts a new workflow instance record.
func (d *WorkflowInstanceDAO) Create(tx *sqlx.Tx, entity *qdhubworkflow.WorkflowInstance) error {
	query := `INSERT INTO workflow_instances (id, workflow_def_id, engine_instance_id, trigger_type, trigger_params, status, progress, started_at, finished_at, error_message)
		VALUES (:id, :workflow_def_id, :engine_instance_id, :trigger_type, :trigger_params, :status, :progress, :started_at, :finished_at, :error_message)`

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
		return fmt.Errorf("failed to create workflow instance: %w", err)
	}
	return nil
}

// GetByID retrieves a workflow instance by ID.
func (d *WorkflowInstanceDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*qdhubworkflow.WorkflowInstance, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row)
}

// Update updates an existing workflow instance record.
func (d *WorkflowInstanceDAO) Update(tx *sqlx.Tx, entity *qdhubworkflow.WorkflowInstance) error {
	query := `UPDATE workflow_instances SET
		status = :status, progress = :progress, finished_at = :finished_at, error_message = :error_message
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
		return fmt.Errorf("failed to update workflow instance: %w", err)
	}
	return nil
}

// UpdateStatusByID updates only status, progress, finished_at, error_message for the given instance ID.
// Used when workflow reaches terminal state so DB stays in sync for frontend/API.
func (d *WorkflowInstanceDAO) UpdateStatusByID(tx *sqlx.Tx, id string, status string, progress float64, finishedAt *time.Time, errorMessage *string) error {
	query := d.DB().Rebind(`UPDATE workflow_instances SET status = ?, progress = ?, finished_at = ?, error_message = ? WHERE id = ?`)
	var finAt sql.NullTime
	if finishedAt != nil {
		finAt = sql.NullTime{Time: *finishedAt, Valid: true}
	}
	var errMsg sql.NullString
	if errorMessage != nil && *errorMessage != "" {
		errMsg = sql.NullString{String: *errorMessage, Valid: true}
	}
	var err error
	if tx != nil {
		_, err = tx.Exec(query, status, progress, finAt, errMsg, id)
	} else {
		_, err = d.DB().Exec(query, status, progress, finAt, errMsg, id)
	}
	if err != nil {
		return fmt.Errorf("failed to update workflow instance status: %w", err)
	}
	return nil
}

// DeleteByID deletes a workflow instance by ID.
func (d *WorkflowInstanceDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// GetByWorkflowDef retrieves all workflow instances for a workflow definition.
func (d *WorkflowInstanceDAO) GetByWorkflowDef(tx *sqlx.Tx, workflowDefID shared.ID) ([]*qdhubworkflow.WorkflowInstance, error) {
	query := d.DB().Rebind(`SELECT * FROM workflow_instances WHERE workflow_def_id = ? ORDER BY started_at DESC`)
	var rows []*WorkflowInstanceRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, workflowDefID.String())
	} else {
		err = d.DB().Select(&rows, query, workflowDefID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instances: %w", err)
	}

	entities := make([]*qdhubworkflow.WorkflowInstance, 0, len(rows))
	for _, row := range rows {
		entity, err := d.toEntity(row)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// DeleteByWorkflowDef deletes all workflow instances for a workflow definition.
func (d *WorkflowInstanceDAO) DeleteByWorkflowDef(tx *sqlx.Tx, workflowDefID shared.ID) error {
	query := d.DB().Rebind(`DELETE FROM workflow_instances WHERE workflow_def_id = ?`)
	var err error
	if tx != nil {
		_, err = tx.Exec(query, workflowDefID.String())
	} else {
		_, err = d.DB().Exec(query, workflowDefID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete workflow instances by workflow def: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
// Note: WorkflowInstance now uses Task Engine's WorkflowInstance type directly.
func (d *WorkflowInstanceDAO) toRow(entity *qdhubworkflow.WorkflowInstance) (*WorkflowInstanceRow, error) {
	if entity == nil {
		return nil, nil
	}

	// Task Engine WorkflowInstance doesn't have TriggerParams, TriggerType, EngineInstanceID, Progress
	// These fields are qdhub-specific and may need to be stored separately
	row := &WorkflowInstanceRow{
		ID:               entity.ID,
		WorkflowDefID:    entity.WorkflowID,
		EngineInstanceID: entity.ID, // Use instance ID as engine instance ID
		TriggerType:      "manual",  // Default
		TriggerParams:    "{}",      // Empty JSON
		Status:           entity.Status,
		Progress:         0.0, // Task Engine doesn't store progress in instance
		StartedAt:        entity.StartTime,
	}

	if entity.EndTime != nil {
		row.FinishedAt = sql.NullTime{Time: *entity.EndTime, Valid: true}
	}

	if entity.ErrorMessage != "" {
		row.ErrorMessage = sql.NullString{String: entity.ErrorMessage, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
// Note: WorkflowInstance now uses Task Engine's WorkflowInstance type directly.
func (d *WorkflowInstanceDAO) toEntity(row *WorkflowInstanceRow) (*qdhubworkflow.WorkflowInstance, error) {
	entity := &qdhubworkflow.WorkflowInstance{
		ID:         row.ID,
		WorkflowID: row.WorkflowDefID,
		Status:     row.Status,
		StartTime:  row.StartedAt,
	}

	if row.FinishedAt.Valid {
		entity.EndTime = &row.FinishedAt.Time
	}

	if row.ErrorMessage.Valid {
		entity.ErrorMessage = row.ErrorMessage.String
	}

	return entity, nil
}
