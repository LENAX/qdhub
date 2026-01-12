package dao

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
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
func (d *WorkflowDefinitionDAO) Create(tx *sqlx.Tx, entity *workflow.WorkflowDefinition) error {
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
func (d *WorkflowDefinitionDAO) GetByID(tx *sqlx.Tx, id shared.ID) (*workflow.WorkflowDefinition, error) {
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
func (d *WorkflowDefinitionDAO) Update(tx *sqlx.Tx, entity *workflow.WorkflowDefinition) error {
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
func (d *WorkflowDefinitionDAO) ListAll(tx *sqlx.Tx) ([]*workflow.WorkflowDefinition, error) {
	rows, err := d.List(tx)
	if err != nil {
		return nil, err
	}

	entities := make([]*workflow.WorkflowDefinition, 0, len(rows))
	for _, row := range rows {
		entities = append(entities, d.toEntity(row))
	}
	return entities, nil
}

// toRow converts domain entity to database row.
func (d *WorkflowDefinitionDAO) toRow(entity *workflow.WorkflowDefinition) *WorkflowDefinitionRow {
	return &WorkflowDefinitionRow{
		ID:             entity.ID.String(),
		Name:           entity.Name,
		Description:    entity.Description,
		Category:       entity.Category.String(),
		DefinitionYAML: entity.DefinitionYAML,
		Version:        entity.Version,
		Status:         entity.Status.String(),
		IsSystem:       entity.IsSystem,
		CreatedAt:      entity.CreatedAt.ToTime(),
		UpdatedAt:      entity.UpdatedAt.ToTime(),
	}
}

// toEntity converts database row to domain entity.
func (d *WorkflowDefinitionDAO) toEntity(row *WorkflowDefinitionRow) *workflow.WorkflowDefinition {
	return &workflow.WorkflowDefinition{
		ID:             shared.ID(row.ID),
		Name:           row.Name,
		Description:    row.Description,
		Category:       workflow.WfCategory(row.Category),
		DefinitionYAML: row.DefinitionYAML,
		Version:        row.Version,
		Status:         workflow.WfDefStatus(row.Status),
		IsSystem:       row.IsSystem,
		CreatedAt:      shared.Timestamp(row.CreatedAt),
		UpdatedAt:      shared.Timestamp(row.UpdatedAt),
	}
}
