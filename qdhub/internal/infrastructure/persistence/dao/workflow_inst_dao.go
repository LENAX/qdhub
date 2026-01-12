package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
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
func (d *WorkflowInstanceDAO) Create(tx *sqlx.Tx, entity *workflow.WorkflowInstance) error {
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
func (d *WorkflowInstanceDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*workflow.WorkflowInstance, error) {
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
func (d *WorkflowInstanceDAO) Update(tx *sqlx.Tx, entity *workflow.WorkflowInstance) error {
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

// DeleteByID deletes a workflow instance by ID.
func (d *WorkflowInstanceDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// GetByWorkflowDef retrieves all workflow instances for a workflow definition.
func (d *WorkflowInstanceDAO) GetByWorkflowDef(tx *sqlx.Tx, workflowDefID shared.ID) ([]*workflow.WorkflowInstance, error) {
	query := `SELECT * FROM workflow_instances WHERE workflow_def_id = ? ORDER BY started_at DESC`
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

	entities := make([]*workflow.WorkflowInstance, 0, len(rows))
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
	query := `DELETE FROM workflow_instances WHERE workflow_def_id = ?`
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
func (d *WorkflowInstanceDAO) toRow(entity *workflow.WorkflowInstance) (*WorkflowInstanceRow, error) {
	triggerParams, err := entity.MarshalTriggerParamsJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal trigger params: %w", err)
	}

	row := &WorkflowInstanceRow{
		ID:               entity.ID.String(),
		WorkflowDefID:    entity.WorkflowDefID.String(),
		EngineInstanceID: entity.EngineInstanceID,
		TriggerType:      entity.TriggerType.String(),
		TriggerParams:    triggerParams,
		Status:           entity.Status.String(),
		Progress:         entity.Progress,
		StartedAt:        entity.StartedAt.ToTime(),
	}

	if entity.FinishedAt != nil {
		row.FinishedAt = sql.NullTime{Time: entity.FinishedAt.ToTime(), Valid: true}
	}

	if entity.ErrorMessage != nil {
		row.ErrorMessage = sql.NullString{String: *entity.ErrorMessage, Valid: true}
	}

	return row, nil
}

// toEntity converts database row to domain entity.
func (d *WorkflowInstanceDAO) toEntity(row *WorkflowInstanceRow) (*workflow.WorkflowInstance, error) {
	entity := &workflow.WorkflowInstance{
		ID:               shared.ID(row.ID),
		WorkflowDefID:    shared.ID(row.WorkflowDefID),
		EngineInstanceID: row.EngineInstanceID,
		TriggerType:      workflow.TriggerType(row.TriggerType),
		Status:           workflow.WfInstStatus(row.Status),
		Progress:         row.Progress,
		StartedAt:        shared.Timestamp(row.StartedAt),
	}

	if row.FinishedAt.Valid {
		ts := shared.Timestamp(row.FinishedAt.Time)
		entity.FinishedAt = &ts
	}

	if row.ErrorMessage.Valid {
		entity.ErrorMessage = &row.ErrorMessage.String
	}

	if row.TriggerParams != "" {
		if err := entity.UnmarshalTriggerParamsJSON(row.TriggerParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal trigger params: %w", err)
		}
	} else {
		entity.TriggerParams = make(map[string]interface{})
	}

	return entity, nil
}
