package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/dao"
)

// WorkflowDefinitionRepositoryImpl implements workflow.WorkflowDefinitionRepository using Task Engine storage.
// Following DDD principles, this repository handles both the aggregate root (WorkflowDefinition)
// and its child entities (WorkflowInstance) to maintain aggregate boundaries.
type WorkflowDefinitionRepositoryImpl struct {
	taskEngineRepo *WorkflowDefinitionRepositoryTaskEngineImpl
	instanceDAO    *dao.WorkflowInstanceDAO
}

// NewWorkflowDefinitionRepository creates a new WorkflowDefinitionRepositoryImpl using Task Engine storage.
func NewWorkflowDefinitionRepository(db *persistence.DB) (*WorkflowDefinitionRepositoryImpl, error) {
	dsn := extractDSNFromDB(db)

	taskEngineRepo, err := NewWorkflowDefinitionRepositoryTaskEngine(db, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create task engine repository: %w", err)
	}

	// Create DAO for storing qdhub-specific instance fields (trigger_type, trigger_params)
	instanceDAO := dao.NewWorkflowInstanceDAO(db.DB)

	return &WorkflowDefinitionRepositoryImpl{
		taskEngineRepo: taskEngineRepo,
		instanceDAO:    instanceDAO,
	}, nil
}

// extractDSNFromDB extracts DSN from the DB connection.
func extractDSNFromDB(db *persistence.DB) string {
	// DSN is now stored in the DB struct
	return db.DSN()
}

// ==================== Aggregate Root Operations ====================

// Create creates a new workflow definition with its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Create(def *workflow.WorkflowDefinition) error {
	return r.taskEngineRepo.Create(def)
}

// Get retrieves a workflow definition by ID with its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Get(id string) (*workflow.WorkflowDefinition, error) {
	return r.taskEngineRepo.Get(id)
}

// Update updates a workflow definition.
func (r *WorkflowDefinitionRepositoryImpl) Update(def *workflow.WorkflowDefinition) error {
	return r.taskEngineRepo.Update(def)
}

// Delete deletes a workflow definition and its aggregated entities.
func (r *WorkflowDefinitionRepositoryImpl) Delete(id string) error {
	return r.taskEngineRepo.Delete(id)
}

// List retrieves all workflow definitions (without aggregated entities for performance).
func (r *WorkflowDefinitionRepositoryImpl) List() ([]*workflow.WorkflowDefinition, error) {
	return r.taskEngineRepo.List()
}

// ==================== Child Entity Operations (WorkflowInstance) ====================

// AddInstance adds a new WorkflowInstance to a WorkflowDefinition.
func (r *WorkflowDefinitionRepositoryImpl) AddInstance(inst *workflow.WorkflowInstance) error {
	return r.taskEngineRepo.AddInstance(inst)
}

// AddInstanceWithTriggerInfo adds a new WorkflowInstance with trigger information.
// This method stores qdhub-specific fields (trigger_type, trigger_params) that are not
// part of Task Engine's WorkflowInstance type.
// The instance should already exist in Task Engine (created by SubmitWorkflow).
func (r *WorkflowDefinitionRepositoryImpl) AddInstanceWithTriggerInfo(
	inst *workflow.WorkflowInstance,
	triggerType string,
	triggerParams map[string]interface{},
) error {
	// Serialize trigger_params to JSON
	triggerParamsJSON := "{}"
	if triggerParams != nil {
		paramsBytes, err := json.Marshal(triggerParams)
		if err != nil {
			return fmt.Errorf("failed to serialize trigger params: %w", err)
		}
		triggerParamsJSON = string(paramsBytes)
	}

	// Create a row with trigger information
	row := &dao.WorkflowInstanceRow{
		ID:               inst.ID,
		WorkflowDefID:    inst.WorkflowID,
		EngineInstanceID: inst.ID,
		TriggerType:      triggerType,
		TriggerParams:    triggerParamsJSON,
		Status:           inst.Status,
		Progress:         0.0,
		StartedAt:        inst.StartTime,
	}

	if inst.EndTime != nil {
		row.FinishedAt = sql.NullTime{Time: *inst.EndTime, Valid: true}
	}

	if inst.ErrorMessage != "" {
		row.ErrorMessage = sql.NullString{String: inst.ErrorMessage, Valid: true}
	}

	// Use DAO to insert or update the record
	// Note: Task Engine may have already created the instance, so we use INSERT OR REPLACE (SQLite)
	// For other databases, this would need to be INSERT ... ON CONFLICT
	query := `INSERT OR REPLACE INTO workflow_instances 
		(id, workflow_def_id, engine_instance_id, trigger_type, trigger_params, status, progress, started_at, finished_at, error_message)
		VALUES (:id, :workflow_def_id, :engine_instance_id, :trigger_type, :trigger_params, :status, :progress, :started_at, :finished_at, :error_message)`

	db := r.instanceDAO.DB()
	reboundQuery := db.Rebind(query)
	_, err := db.NamedExec(reboundQuery, row)
	if err != nil {
		return fmt.Errorf("failed to store workflow instance with trigger info: %w", err)
	}

	return nil
}

// GetInstance retrieves a WorkflowInstance by ID.
func (r *WorkflowDefinitionRepositoryImpl) GetInstance(id string) (*workflow.WorkflowInstance, error) {
	return r.taskEngineRepo.GetInstance(id)
}

// GetInstancesByDef retrieves all WorkflowInstances for a WorkflowDefinition.
func (r *WorkflowDefinitionRepositoryImpl) GetInstancesByDef(workflowDefID string) ([]*workflow.WorkflowInstance, error) {
	return r.taskEngineRepo.GetInstancesByDef(workflowDefID)
}

// UpdateInstance updates a WorkflowInstance.
func (r *WorkflowDefinitionRepositoryImpl) UpdateInstance(inst *workflow.WorkflowInstance) error {
	return r.taskEngineRepo.UpdateInstance(inst)
}

// DeleteInstance deletes a WorkflowInstance by ID.
func (r *WorkflowDefinitionRepositoryImpl) DeleteInstance(id string) error {
	return r.taskEngineRepo.DeleteInstance(id)
}

// ==================== Extended Query Operations ====================
// Note: These are simplified implementations that filter in memory.
// For large datasets, consider implementing filtering at the storage layer.

// FindBy retrieves entities matching the given conditions.
func (r *WorkflowDefinitionRepositoryImpl) FindBy(conditions ...shared.QueryCondition) ([]*workflow.WorkflowDefinition, error) {
	all, err := r.List()
	if err != nil {
		return nil, err
	}
	return filterWorkflowDefinitions(all, conditions...), nil
}

// FindByWithOrder retrieves entities matching conditions with ordering.
func (r *WorkflowDefinitionRepositoryImpl) FindByWithOrder(orderBy []shared.OrderBy, conditions ...shared.QueryCondition) ([]*workflow.WorkflowDefinition, error) {
	// Note: Ordering not implemented for Task Engine storage, returning filtered results only
	return r.FindBy(conditions...)
}

// ListWithPagination retrieves entities with pagination.
func (r *WorkflowDefinitionRepositoryImpl) ListWithPagination(pagination shared.Pagination) (*shared.PageResult[workflow.WorkflowDefinition], error) {
	return r.FindByWithPagination(pagination)
}

// FindByWithPagination retrieves entities matching conditions with pagination.
func (r *WorkflowDefinitionRepositoryImpl) FindByWithPagination(pagination shared.Pagination, conditions ...shared.QueryCondition) (*shared.PageResult[workflow.WorkflowDefinition], error) {
	all, err := r.List()
	if err != nil {
		return nil, err
	}

	filtered := filterWorkflowDefinitions(all, conditions...)
	total := int64(len(filtered))

	// Apply pagination
	start := pagination.Offset()
	end := start + pagination.Limit()
	if start > len(filtered) {
		start = len(filtered)
	}
	if end > len(filtered) {
		end = len(filtered)
	}

	return shared.NewPageResult(filtered[start:end], total, pagination), nil
}

// Count returns the total count of entities matching conditions.
func (r *WorkflowDefinitionRepositoryImpl) Count(conditions ...shared.QueryCondition) (int64, error) {
	all, err := r.List()
	if err != nil {
		return 0, err
	}
	return int64(len(filterWorkflowDefinitions(all, conditions...))), nil
}

// Exists checks if any entity matching conditions exists.
func (r *WorkflowDefinitionRepositoryImpl) Exists(conditions ...shared.QueryCondition) (bool, error) {
	count, err := r.Count(conditions...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// filterWorkflowDefinitions filters workflow definitions by conditions in memory.
func filterWorkflowDefinitions(defs []*workflow.WorkflowDefinition, conditions ...shared.QueryCondition) []*workflow.WorkflowDefinition {
	if len(conditions) == 0 {
		return defs
	}

	result := make([]*workflow.WorkflowDefinition, 0)
	for _, def := range defs {
		if matchesWorkflowConditions(def, conditions...) {
			result = append(result, def)
		}
	}
	return result
}

// matchesWorkflowConditions checks if a workflow definition matches all conditions.
func matchesWorkflowConditions(def *workflow.WorkflowDefinition, conditions ...shared.QueryCondition) bool {
	for _, cond := range conditions {
		if !matchesWorkflowCondition(def, cond) {
			return false
		}
	}
	return true
}

// matchesWorkflowCondition checks if a workflow definition matches a single condition.
func matchesWorkflowCondition(def *workflow.WorkflowDefinition, cond shared.QueryCondition) bool {
	// Get field value using reflection
	var fieldValue interface{}
	switch strings.ToLower(cond.Field) {
	case "id":
		fieldValue = def.ID
	case "name":
		fieldValue = def.Name
	case "status":
		if def.Workflow != nil {
			fieldValue = string(def.Workflow.GetStatus())
		}
	default:
		// Try to find field in embedded Workflow using reflection
		if def.Workflow != nil {
			v := reflect.ValueOf(def.Workflow).Elem()
			f := v.FieldByName(cond.Field)
			if f.IsValid() {
				fieldValue = f.Interface()
			}
		}
	}

	return matchesConditionValue(fieldValue, cond)
}

// matchesConditionValue checks if a value matches a condition.
func matchesConditionValue(fieldValue interface{}, cond shared.QueryCondition) bool {
	switch cond.Operator {
	case shared.OpEqual:
		return fmt.Sprintf("%v", fieldValue) == fmt.Sprintf("%v", cond.Value)
	case shared.OpNotEqual:
		return fmt.Sprintf("%v", fieldValue) != fmt.Sprintf("%v", cond.Value)
	case shared.OpLike:
		pattern := strings.ReplaceAll(fmt.Sprintf("%v", cond.Value), "%", "")
		return strings.Contains(fmt.Sprintf("%v", fieldValue), pattern)
	case shared.OpIsNull:
		return fieldValue == nil || fmt.Sprintf("%v", fieldValue) == ""
	case shared.OpIsNotNull:
		return fieldValue != nil && fmt.Sprintf("%v", fieldValue) != ""
	default:
		return true // Unsupported operators default to true
	}
}
