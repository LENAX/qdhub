// Package repository provides repository implementations using Task Engine storage.
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/LENAX/task-engine/pkg/storage"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"

	"qdhub/internal/domain/shared"
	qdhubworkflow "qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
)

// WorkflowDefinitionRepositoryTaskEngineImpl implements workflow.WorkflowDefinitionRepository using Task Engine storage.
type WorkflowDefinitionRepositoryTaskEngineImpl struct {
	aggregateRepo storage.WorkflowAggregateRepository
	db            *persistence.DB // Database connection for updating qdhub-specific fields
}

// NewWorkflowDefinitionRepositoryTaskEngine creates a new WorkflowDefinitionRepositoryTaskEngineImpl.
func NewWorkflowDefinitionRepositoryTaskEngine(db *persistence.DB, dsn string) (*WorkflowDefinitionRepositoryTaskEngineImpl, error) {
	repo, err := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create task engine repository: %w", err)
	}

	return &WorkflowDefinitionRepositoryTaskEngineImpl{
		aggregateRepo: repo,
		db:            db,
	}, nil
}

// Create creates a new workflow definition with its aggregated entities.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Create(def *qdhubworkflow.WorkflowDefinition) error {
	ctx := context.Background()

	// Directly use Task Engine Workflow (embedded in WorkflowDefinition)
	if err := r.aggregateRepo.SaveWorkflow(ctx, def.Workflow); err != nil {
		return fmt.Errorf("failed to save workflow: %w", err)
	}

	// Update qdhub-specific fields in workflow_definition table
	// Note: Task Engine's SaveWorkflow saves to workflow_definition table,
	// but doesn't update qdhub-specific fields (is_system, category, etc.)
	query := `UPDATE workflow_definition SET
		category = :category,
		definition_yaml = :definition_yaml,
		version = :version,
		is_system = :is_system,
		updated_at = :updated_at
		WHERE id = :id`

	args := map[string]interface{}{
		"id":              def.ID(),
		"category":        def.Category.String(),
		"definition_yaml": def.DefinitionYAML,
		"version":         def.Version,
		"is_system":       r.boolToDBValue(def.IsSystem), // Convert boolean based on database type
		"updated_at":      def.UpdatedAt.ToTime(),
	}

	if _, err := r.db.DB.NamedExec(query, args); err != nil {
		return fmt.Errorf("failed to update qdhub-specific fields: %w", err)
	}

	return nil
}

// boolToDBValue converts boolean to database-appropriate value.
// SQLite uses INTEGER (0/1), PostgreSQL/MySQL can use BOOLEAN or INTEGER.
// We use INTEGER for compatibility across all databases.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) boolToDBValue(b bool) interface{} {
	// Use integer for cross-database compatibility
	// SQLite: INTEGER (0/1)
	// PostgreSQL: Can use INTEGER or BOOLEAN (both work)
	// MySQL: Can use TINYINT(1) or BOOLEAN (both work)
	if b {
		return 1
	}
	return 0
}

// intToBool converts database integer value to boolean.
func intToBool(val sql.NullInt64) bool {
	if !val.Valid {
		return false
	}
	return val.Int64 != 0
}

// Get retrieves a workflow definition by ID with its aggregated entities.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Get(id string) (*qdhubworkflow.WorkflowDefinition, error) {
	ctx := context.Background()

	// Get workflow with tasks from task engine
	teWorkflow, err := r.aggregateRepo.GetWorkflowWithTasks(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}
	if teWorkflow == nil {
		return nil, nil
	}

	// Load qdhub-specific fields from database
	var category, definitionYAML sql.NullString
	var version, isSystem sql.NullInt64
	var updatedAt sql.NullTime

	query := `SELECT category, definition_yaml, version, is_system, updated_at 
		FROM workflow_definition WHERE id = ?`
	err = r.db.DB.QueryRow(query, id).Scan(&category, &definitionYAML, &version, &isSystem, &updatedAt)

	// Set defaults if fields don't exist or are null
	defCategory := "custom"
	if category.Valid {
		defCategory = category.String
	}
	defYAML := ""
	if definitionYAML.Valid {
		defYAML = definitionYAML.String
	}
	defVersion := 1
	if version.Valid {
		defVersion = int(version.Int64)
	}
	defIsSystem := intToBool(isSystem)
	defUpdatedAt := shared.Now()
	if updatedAt.Valid {
		defUpdatedAt = shared.Timestamp(updatedAt.Time)
	}

	// Wrap Task Engine Workflow in WorkflowDefinition
	def := &qdhubworkflow.WorkflowDefinition{
		Workflow:       teWorkflow,
		Category:       qdhubworkflow.WfCategory(defCategory),
		DefinitionYAML: defYAML,
		Version:        defVersion,
		IsSystem:       defIsSystem,
		UpdatedAt:      defUpdatedAt,
	}

	return def, nil
}

// Update updates a workflow definition.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Update(def *qdhubworkflow.WorkflowDefinition) error {
	ctx := context.Background()

	// Directly use Task Engine Workflow
	if err := r.aggregateRepo.SaveWorkflow(ctx, def.Workflow); err != nil {
		return fmt.Errorf("failed to update workflow: %w", err)
	}

	// Update qdhub-specific fields
	query := `UPDATE workflow_definition SET
		category = :category,
		definition_yaml = :definition_yaml,
		version = :version,
		is_system = :is_system,
		updated_at = :updated_at
		WHERE id = :id`

	args := map[string]interface{}{
		"id":              def.ID(),
		"category":        def.Category.String(),
		"definition_yaml": def.DefinitionYAML,
		"version":         def.Version,
		"is_system":       r.boolToDBValue(def.IsSystem),
		"updated_at":      def.UpdatedAt.ToTime(),
	}

	if _, err := r.db.DB.NamedExec(query, args); err != nil {
		return fmt.Errorf("failed to update qdhub-specific fields: %w", err)
	}

	return nil
}

// Delete deletes a workflow definition and its aggregated entities.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) Delete(id string) error {
	ctx := context.Background()

	if err := r.aggregateRepo.DeleteWorkflow(ctx, id); err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}

	return nil
}

// List retrieves all workflow definitions (without aggregated entities for performance).
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) List() ([]*qdhubworkflow.WorkflowDefinition, error) {
	ctx := context.Background()

	teWorkflows, err := r.aggregateRepo.ListWorkflows(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}

	// Load qdhub-specific fields for all workflows in batch
	query := `SELECT id, category, definition_yaml, version, is_system, updated_at 
		FROM workflow_definition WHERE id IN (`
	ids := make([]string, 0, len(teWorkflows))
	for _, teWorkflow := range teWorkflows {
		ids = append(ids, teWorkflow.GetID())
	}

	// Build query with placeholders
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	query += strings.Join(placeholders, ",") + ")"

	rows, err := r.db.DB.Query(query, args...)
	if err != nil {
		// If query fails, return workflows without qdhub fields
		result := make([]*qdhubworkflow.WorkflowDefinition, 0, len(teWorkflows))
		for _, teWorkflow := range teWorkflows {
			result = append(result, &qdhubworkflow.WorkflowDefinition{
				Workflow:       teWorkflow,
				Category:       qdhubworkflow.WfCategoryCustom,
				DefinitionYAML: "",
				Version:        1,
				IsSystem:       false,
				UpdatedAt:      shared.Now(),
			})
		}
		return result, nil
	}
	defer rows.Close()

	// Map qdhub fields by ID
	qdhubFields := make(map[string]struct {
		category       string
		definitionYAML string
		version        int
		isSystem       bool
		updatedAt      time.Time
	})

	for rows.Next() {
		var id string
		var category, definitionYAML sql.NullString
		var version, isSystem sql.NullInt64
		var updatedAt sql.NullTime

		if err := rows.Scan(&id, &category, &definitionYAML, &version, &isSystem, &updatedAt); err != nil {
			continue
		}

		defCategory := "custom"
		if category.Valid {
			defCategory = category.String
		}
		defYAML := ""
		if definitionYAML.Valid {
			defYAML = definitionYAML.String
		}
		defVersion := 1
		if version.Valid {
			defVersion = int(version.Int64)
		}
		defIsSystem := intToBool(isSystem)
		defUpdatedAt := time.Now()
		if updatedAt.Valid {
			defUpdatedAt = updatedAt.Time
		}

		qdhubFields[id] = struct {
			category       string
			definitionYAML string
			version        int
			isSystem       bool
			updatedAt      time.Time
		}{
			category:       defCategory,
			definitionYAML: defYAML,
			version:        defVersion,
			isSystem:       defIsSystem,
			updatedAt:      defUpdatedAt,
		}
	}

	// Combine Task Engine workflows with qdhub fields
	result := make([]*qdhubworkflow.WorkflowDefinition, 0, len(teWorkflows))
	for _, teWorkflow := range teWorkflows {
		id := teWorkflow.GetID()
		fields, exists := qdhubFields[id]
		if !exists {
			// Use defaults if fields don't exist
			fields = struct {
				category       string
				definitionYAML string
				version        int
				isSystem       bool
				updatedAt      time.Time
			}{
				category:       "custom",
				definitionYAML: "",
				version:        1,
				isSystem:       false,
				updatedAt:      time.Now(),
			}
		}

		def := &qdhubworkflow.WorkflowDefinition{
			Workflow:       teWorkflow,
			Category:       qdhubworkflow.WfCategory(fields.category),
			DefinitionYAML: fields.definitionYAML,
			Version:        fields.version,
			IsSystem:       fields.isSystem,
			UpdatedAt:      shared.Timestamp(fields.updatedAt),
		}
		result = append(result, def)
	}

	return result, nil
}

// ==================== Child Entity Operations (WorkflowInstance) ====================

// AddInstance adds a new WorkflowInstance to a WorkflowDefinition.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) AddInstance(inst *qdhubworkflow.WorkflowInstance) error {
	ctx := context.Background()

	// Get workflow first to use StartWorkflow
	teWorkflow, err := r.aggregateRepo.GetWorkflow(ctx, inst.WorkflowID)
	if err != nil {
		return fmt.Errorf("failed to get workflow: %w", err)
	}
	if teWorkflow == nil {
		return fmt.Errorf("workflow not found: %s", inst.WorkflowID)
	}

	// Use StartWorkflow to create instance
	teInstance, err := r.aggregateRepo.StartWorkflow(ctx, teWorkflow)
	if err != nil {
		return fmt.Errorf("failed to start workflow: %w", err)
	}

	// Update instance ID if needed
	if inst.ID != teInstance.ID {
		inst.ID = teInstance.ID
	}

	return nil
}

// GetInstance retrieves a WorkflowInstance by ID.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) GetInstance(id string) (*qdhubworkflow.WorkflowInstance, error) {
	ctx := context.Background()

	teInstance, taskInstances, err := r.aggregateRepo.GetWorkflowInstanceWithTasks(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow instance: %w", err)
	}
	if teInstance == nil {
		return nil, nil
	}

	// Directly return Task Engine WorkflowInstance (it's a type alias)
	_ = taskInstances // TODO: Handle task instances if needed

	return teInstance, nil
}

// GetInstancesByDef retrieves all WorkflowInstances for a WorkflowDefinition.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) GetInstancesByDef(workflowDefID string) ([]*qdhubworkflow.WorkflowInstance, error) {
	ctx := context.Background()

	teInstances, err := r.aggregateRepo.ListWorkflowInstances(ctx, workflowDefID)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow instances: %w", err)
	}

	result := make([]*qdhubworkflow.WorkflowInstance, 0, len(teInstances))
	for _, inst := range teInstances {
		result = append(result, inst)
	}

	return result, nil
}

// UpdateInstance updates a WorkflowInstance.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) UpdateInstance(inst *qdhubworkflow.WorkflowInstance) error {
	ctx := context.Background()

	if err := r.aggregateRepo.UpdateWorkflowInstanceStatus(ctx, inst.ID, inst.Status); err != nil {
		return fmt.Errorf("failed to update workflow instance status: %w", err)
	}

	return nil
}

// DeleteInstance deletes a WorkflowInstance by ID.
func (r *WorkflowDefinitionRepositoryTaskEngineImpl) DeleteInstance(id string) error {
	ctx := context.Background()

	if err := r.aggregateRepo.DeleteWorkflowInstance(ctx, id); err != nil {
		return fmt.Errorf("failed to delete workflow instance: %w", err)
	}

	return nil
}
