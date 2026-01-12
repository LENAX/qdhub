package dao

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	qdhubworkflow "qdhub/internal/domain/workflow"
)

// TaskInstanceDAO provides data access operations for TaskInstance.
type TaskInstanceDAO struct {
	*SQLBaseDAO[TaskInstanceRow]
}

// NewTaskInstanceDAO creates a new TaskInstanceDAO.
func NewTaskInstanceDAO(db *sqlx.DB) *TaskInstanceDAO {
	return &TaskInstanceDAO{
		SQLBaseDAO: NewSQLBaseDAO[TaskInstanceRow](db, "task_instances", "id"),
	}
}

// Create inserts a new task instance record.
func (d *TaskInstanceDAO) Create(tx *sqlx.Tx, entity *qdhubworkflow.TaskInstance) error {
	query := `INSERT INTO task_instances (id, workflow_inst_id, task_name, status, started_at, finished_at, retry_count, error_message)
		VALUES (:id, :workflow_inst_id, :task_name, :status, :started_at, :finished_at, :retry_count, :error_message)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create task instance: %w", err)
	}
	return nil
}

// GetByID retrieves a task instance by ID.
func (d *TaskInstanceDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*qdhubworkflow.TaskInstance, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// Update updates an existing task instance record.
func (d *TaskInstanceDAO) Update(tx *sqlx.Tx, entity *qdhubworkflow.TaskInstance) error {
	query := `UPDATE task_instances SET
		status = :status, started_at = :started_at, finished_at = :finished_at,
		retry_count = :retry_count, error_message = :error_message
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update task instance: %w", err)
	}
	return nil
}

// DeleteByID deletes a task instance by ID.
func (d *TaskInstanceDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// GetByWorkflowInstance retrieves all task instances for a workflow instance.
func (d *TaskInstanceDAO) GetByWorkflowInstance(tx *sqlx.Tx, workflowInstID shared.ID) ([]*qdhubworkflow.TaskInstance, error) {
	query := `SELECT * FROM task_instances WHERE workflow_inst_id = ?`
	var rows []*TaskInstanceRow

	var err error
	if tx != nil {
		err = tx.Select(&rows, query, workflowInstID.String())
	} else {
		err = d.DB().Select(&rows, query, workflowInstID.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get task instances: %w", err)
	}

	entities := make([]*qdhubworkflow.TaskInstance, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// DeleteByWorkflowInstance deletes all task instances for a workflow instance.
func (d *TaskInstanceDAO) DeleteByWorkflowInstance(tx *sqlx.Tx, workflowInstID shared.ID) error {
	query := `DELETE FROM task_instances WHERE workflow_inst_id = ?`
	var err error
	if tx != nil {
		_, err = tx.Exec(query, workflowInstID.String())
	} else {
		_, err = d.DB().Exec(query, workflowInstID.String())
	}
	if err != nil {
		return fmt.Errorf("failed to delete task instances by workflow instance: %w", err)
	}
	return nil
}

// toRow converts domain entity to database row.
// Note: TaskInstance now uses Task Engine's TaskInstance type directly.
func (d *TaskInstanceDAO) toRow(entity *qdhubworkflow.TaskInstance) *TaskInstanceRow {
	row := &TaskInstanceRow{
		ID:             entity.ID,
		WorkflowInstID: entity.WorkflowInstanceID,
		TaskName:       entity.Name,
		Status:         entity.Status,
		RetryCount:     entity.RetryCount,
	}

	if entity.StartTime != nil {
		row.StartedAt = sql.NullTime{Time: *entity.StartTime, Valid: true}
	}

	if entity.EndTime != nil {
		row.FinishedAt = sql.NullTime{Time: *entity.EndTime, Valid: true}
	}

	if entity.ErrorMessage != "" {
		row.ErrorMessage = sql.NullString{String: entity.ErrorMessage, Valid: true}
	}

	return row
}

// toEntity converts database row to domain entity.
// Note: TaskInstance now uses Task Engine's TaskInstance type directly.
func (d *TaskInstanceDAO) toEntity(row *TaskInstanceRow) *qdhubworkflow.TaskInstance {
	entity := &qdhubworkflow.TaskInstance{
		ID:                 row.ID,
		WorkflowInstanceID: row.WorkflowInstID,
		Name:               row.TaskName,
		Status:             row.Status,
		RetryCount:         row.RetryCount,
	}

	if row.StartedAt.Valid {
		entity.StartTime = &row.StartedAt.Time
	}

	if row.FinishedAt.Valid {
		entity.EndTime = &row.FinishedAt.Time
	}

	if row.ErrorMessage.Valid {
		entity.ErrorMessage = row.ErrorMessage.String
	}

	return entity
}
