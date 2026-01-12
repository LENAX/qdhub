// Package workflow contains the workflow domain entities.
package workflow

import (
	"encoding/json"
	"time"

	"qdhub/internal/domain/shared"
)

// ==================== 聚合根 ====================

// WorkflowDefinition represents a workflow definition aggregate root.
// Responsibilities:
//   - Manage workflow definition
//   - Manage workflow instances
//   - Manage task instances
type WorkflowDefinition struct {
	ID             shared.ID
	Name           string
	Description    string
	Category       WfCategory
	DefinitionYAML string
	Version        int
	Status         WfDefStatus
	IsSystem       bool
	CreatedAt      shared.Timestamp
	UpdatedAt      shared.Timestamp

	// Aggregated entities (lazy loaded)
	Instances []WorkflowInstance
}

// NewWorkflowDefinition creates a new WorkflowDefinition aggregate.
func NewWorkflowDefinition(name, description string, category WfCategory, definitionYAML string, isSystem bool) *WorkflowDefinition {
	now := shared.Now()
	return &WorkflowDefinition{
		ID:             shared.NewID(),
		Name:           name,
		Description:    description,
		Category:       category,
		DefinitionYAML: definitionYAML,
		Version:        1,
		Status:         WfDefStatusEnabled,
		IsSystem:       isSystem,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// Enable enables the workflow definition.
func (wf *WorkflowDefinition) Enable() {
	wf.Status = WfDefStatusEnabled
	wf.UpdatedAt = shared.Now()
}

// Disable disables the workflow definition.
func (wf *WorkflowDefinition) Disable() {
	wf.Status = WfDefStatusDisabled
	wf.UpdatedAt = shared.Now()
}

// UpdateDefinition updates the workflow definition.
func (wf *WorkflowDefinition) UpdateDefinition(definitionYAML string) {
	wf.DefinitionYAML = definitionYAML
	wf.Version++
	wf.UpdatedAt = shared.Now()
}

// IsEnabled checks if the workflow definition is enabled.
func (wf *WorkflowDefinition) IsEnabled() bool {
	return wf.Status == WfDefStatusEnabled
}

// CanCreateInstance checks if a new instance can be created.
func (wf *WorkflowDefinition) CanCreateInstance() error {
	if !wf.IsEnabled() {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "workflow definition is disabled", nil)
	}
	return nil
}

// ==================== 聚合内实体 ====================

// WorkflowInstance represents a workflow instance entity.
// Belongs to: WorkflowDefinition aggregate
type WorkflowInstance struct {
	ID               shared.ID
	WorkflowDefID    shared.ID
	EngineInstanceID string
	TriggerType      TriggerType
	TriggerParams    map[string]interface{}
	Status           WfInstStatus
	Progress         float64
	StartedAt        shared.Timestamp
	FinishedAt       *shared.Timestamp
	ErrorMessage     *string

	// Aggregated entities
	TaskInstances []TaskInstance
}

// NewWorkflowInstance creates a new WorkflowInstance.
func NewWorkflowInstance(workflowDefID shared.ID, engineInstanceID string, triggerType TriggerType, triggerParams map[string]interface{}) *WorkflowInstance {
	return &WorkflowInstance{
		ID:               shared.NewID(),
		WorkflowDefID:    workflowDefID,
		EngineInstanceID: engineInstanceID,
		TriggerType:      triggerType,
		TriggerParams:    triggerParams,
		Status:           WfInstStatusPending,
		Progress:         0.0,
		StartedAt:        shared.Now(),
		TaskInstances:    []TaskInstance{},
	}
}

// MarkRunning marks the instance as running.
func (wi *WorkflowInstance) MarkRunning() {
	wi.Status = WfInstStatusRunning
}

// MarkPaused marks the instance as paused.
func (wi *WorkflowInstance) MarkPaused() {
	wi.Status = WfInstStatusPaused
}

// MarkSuccess marks the instance as successful.
func (wi *WorkflowInstance) MarkSuccess() {
	now := shared.Now()
	wi.Status = WfInstStatusSuccess
	wi.Progress = 100.0
	wi.FinishedAt = &now
}

// MarkFailed marks the instance as failed.
func (wi *WorkflowInstance) MarkFailed(errorMsg string) {
	now := shared.Now()
	wi.Status = WfInstStatusFailed
	wi.FinishedAt = &now
	wi.ErrorMessage = &errorMsg
}

// MarkCancelled marks the instance as cancelled.
func (wi *WorkflowInstance) MarkCancelled() {
	now := shared.Now()
	wi.Status = WfInstStatusCancelled
	wi.FinishedAt = &now
}

// UpdateProgress updates the progress.
func (wi *WorkflowInstance) UpdateProgress(progress float64) {
	wi.Progress = progress
}

// MarshalTriggerParamsJSON marshals trigger params to JSON string.
func (wi *WorkflowInstance) MarshalTriggerParamsJSON() (string, error) {
	data, err := json.Marshal(wi.TriggerParams)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// UnmarshalTriggerParamsJSON unmarshals trigger params from JSON string.
func (wi *WorkflowInstance) UnmarshalTriggerParamsJSON(jsonStr string) error {
	return json.Unmarshal([]byte(jsonStr), &wi.TriggerParams)
}

// TaskInstance represents a task instance entity.
// Belongs to: WorkflowDefinition aggregate (via WorkflowInstance)
type TaskInstance struct {
	ID             shared.ID
	WorkflowInstID shared.ID
	TaskName       string
	Status         TaskStatus
	StartedAt      *time.Time
	FinishedAt     *time.Time
	RetryCount     int
	ErrorMessage   *string
}

// NewTaskInstance creates a new TaskInstance.
func NewTaskInstance(workflowInstID shared.ID, taskName string) *TaskInstance {
	return &TaskInstance{
		ID:             shared.NewID(),
		WorkflowInstID: workflowInstID,
		TaskName:       taskName,
		Status:         TaskStatusPending,
		RetryCount:     0,
	}
}

// MarkRunning marks the task as running.
func (ti *TaskInstance) MarkRunning() {
	now := time.Now()
	ti.Status = TaskStatusRunning
	ti.StartedAt = &now
}

// MarkSuccess marks the task as successful.
func (ti *TaskInstance) MarkSuccess() {
	now := time.Now()
	ti.Status = TaskStatusSuccess
	ti.FinishedAt = &now
}

// MarkFailed marks the task as failed.
func (ti *TaskInstance) MarkFailed(errorMsg string) {
	now := time.Now()
	ti.Status = TaskStatusFailed
	ti.FinishedAt = &now
	ti.ErrorMessage = &errorMsg
}

// MarkSkipped marks the task as skipped.
func (ti *TaskInstance) MarkSkipped() {
	ti.Status = TaskStatusSkipped
}

// IncrementRetryCount increments the retry count.
func (ti *TaskInstance) IncrementRetryCount() {
	ti.RetryCount++
}

// ==================== 枚举类型 ====================

// WfCategory represents workflow category.
type WfCategory string

const (
	WfCategoryMetadata WfCategory = "metadata"
	WfCategorySync     WfCategory = "sync"
	WfCategoryCustom   WfCategory = "custom"
)

// String returns the string representation of the workflow category.
func (wc WfCategory) String() string {
	return string(wc)
}

// WfDefStatus represents workflow definition status.
type WfDefStatus string

const (
	WfDefStatusEnabled  WfDefStatus = "enabled"
	WfDefStatusDisabled WfDefStatus = "disabled"
)

// String returns the string representation of the workflow definition status.
func (wds WfDefStatus) String() string {
	return string(wds)
}

// TriggerType represents trigger type.
type TriggerType string

const (
	TriggerTypeManual TriggerType = "manual"
	TriggerTypeCron   TriggerType = "cron"
	TriggerTypeEvent  TriggerType = "event"
)

// String returns the string representation of the trigger type.
func (tt TriggerType) String() string {
	return string(tt)
}

// WfInstStatus represents workflow instance status.
type WfInstStatus string

const (
	WfInstStatusPending   WfInstStatus = "pending"
	WfInstStatusRunning   WfInstStatus = "running"
	WfInstStatusPaused    WfInstStatus = "paused"
	WfInstStatusSuccess   WfInstStatus = "success"
	WfInstStatusFailed    WfInstStatus = "failed"
	WfInstStatusCancelled WfInstStatus = "cancelled"
)

// String returns the string representation of the workflow instance status.
func (wis WfInstStatus) String() string {
	return string(wis)
}

// TaskStatus represents task status.
type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusSuccess TaskStatus = "success"
	TaskStatusFailed  TaskStatus = "failed"
	TaskStatusSkipped TaskStatus = "skipped"
)

// String returns the string representation of the task status.
func (ts TaskStatus) String() string {
	return string(ts)
}
