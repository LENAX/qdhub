package dao

import (
	"fmt"

	"github.com/LENAX/task-engine/pkg/core/workflow"
	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	qdhubworkflow "qdhub/internal/domain/workflow"
)

// WorkflowDefinitionDAO provides data access operations for WorkflowDefinition.
type WorkflowDefinitionDAO struct {
	*SQLBaseDAO[WorkflowDefinitionRow]
}

// NewWorkflowDefinitionDAO creates a new WorkflowDefinitionDAO.
func NewWorkflowDefinitionDAO(db *sqlx.DB) *WorkflowDefinitionDAO {
	return &WorkflowDefinitionDAO{
		SQLBaseDAO: NewSQLBaseDAO[WorkflowDefinitionRow](db, "workflow_definitions", "id"),
	}
}

// Create inserts a new workflow definition record.
func (d *WorkflowDefinitionDAO) Create(tx *sqlx.Tx, entity *qdhubworkflow.WorkflowDefinition) error {
	query := `INSERT INTO workflow_definitions (id, name, description, category, definition_yaml, version, status, is_system, created_at, updated_at)
		VALUES (:id, :name, :description, :category, :definition_yaml, :version, :status, :is_system, :created_at, :updated_at)`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to create workflow definition: %w", err)
	}
	return nil
}

// GetByID retrieves a workflow definition by ID.
func (d *WorkflowDefinitionDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*qdhubworkflow.WorkflowDefinition, error) {
	row, err := d.Get(tx, id.String())
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return d.toEntity(row), nil
}

// Update updates an existing workflow definition record.
func (d *WorkflowDefinitionDAO) Update(tx *sqlx.Tx, entity *qdhubworkflow.WorkflowDefinition) error {
	query := `UPDATE workflow_definitions SET
		name = :name, description = :description, category = :category, definition_yaml = :definition_yaml,
		version = :version, status = :status, is_system = :is_system, updated_at = :updated_at
		WHERE id = :id`

	row := d.toRow(entity)
	var err error
	if tx != nil {
		_, err = tx.NamedExec(query, row)
	} else {
		_, err = d.DB().NamedExec(query, row)
	}

	if err != nil {
		return fmt.Errorf("failed to update workflow definition: %w", err)
	}
	return nil
}

// DeleteByID deletes a workflow definition by ID.
func (d *WorkflowDefinitionDAO) DeleteByID(tx *sqlx.Tx, id shared.ID) error {
	return d.Delete(tx, id.String())
}

// ListAll retrieves all workflow definitions.
func (d *WorkflowDefinitionDAO) ListAll(tx *sqlx.Tx) ([]*qdhubworkflow.WorkflowDefinition, error) {
	rows, err := d.List(tx)
	if err != nil {
		return nil, err
	}

	entities := make([]*qdhubworkflow.WorkflowDefinition, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// toRow converts domain entity to database row.
// Note: WorkflowDefinition now embeds Task Engine's Workflow type.
func (d *WorkflowDefinitionDAO) toRow(entity *qdhubworkflow.WorkflowDefinition) *WorkflowDefinitionRow {
	if entity == nil || entity.Workflow == nil {
		return nil
	}

	return &WorkflowDefinitionRow{
		ID:             entity.ID(),
		Name:           entity.Workflow.Name,
		Description:    entity.Workflow.Description,
		Category:       entity.Category.String(),
		DefinitionYAML: entity.DefinitionYAML,
		Version:        entity.Version,
		Status:         entity.Status().String(),
		IsSystem:       entity.IsSystem,
		CreatedAt:      entity.Workflow.CreateTime,
		UpdatedAt:      entity.UpdatedAt.ToTime(),
	}
}

// toEntity converts database row to domain entity.
// Note: This method is deprecated as we now use Task Engine storage directly.
func (d *WorkflowDefinitionDAO) toEntity(row *WorkflowDefinitionRow) *qdhubworkflow.WorkflowDefinition {
	// Create Task Engine Workflow
	teWorkflow := workflow.NewWorkflow(row.Name, row.Description)
	teWorkflow.ID = row.ID
	if row.Status == "enabled" {
		teWorkflow.SetStatus("ENABLED")
	} else {
		teWorkflow.SetStatus("DISABLED")
	}

	// Wrap in WorkflowDefinition
	return &qdhubworkflow.WorkflowDefinition{
		Workflow:       teWorkflow,
		Category:       qdhubworkflow.WfCategory(row.Category),
		DefinitionYAML: row.DefinitionYAML,
		Version:        row.Version,
		IsSystem:       row.IsSystem,
		UpdatedAt:      shared.Timestamp(row.UpdatedAt),
	}
}
