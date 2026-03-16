// Package workflow contains the workflow domain entities.
// This package uses Task Engine types directly to avoid duplication.
package workflow

import (
	"strings"

	"qdhub/internal/domain/shared"

	"github.com/LENAX/task-engine/pkg/core/workflow"
	taskenginestorage "github.com/LENAX/task-engine/pkg/storage"
)

// ==================== 类型别名：直接使用 Task Engine 的类型 ====================

// Workflow 直接使用 Task Engine 的 Workflow 类型
type Workflow = workflow.Workflow

// WorkflowInstance 直接使用 Task Engine 的 WorkflowInstance 类型
type WorkflowInstance = workflow.WorkflowInstance

// TaskInstance 直接使用 Task Engine 的 TaskInstance 类型
type TaskInstance = taskenginestorage.TaskInstance

// ==================== 扩展类型（qdhub 特定的业务字段）====================

// WorkflowDefinition 扩展 Task Engine 的 Workflow，添加 qdhub 特定的业务字段
type WorkflowDefinition struct {
	*workflow.Workflow // 嵌入 Task Engine 的 Workflow

	// qdhub 特定的业务字段
	Category       WfCategory
	DefinitionYAML string
	Version        int
	IsSystem       bool
	UpdatedAt      shared.Timestamp
}

// ID returns the workflow ID (from embedded Workflow).
func (wf *WorkflowDefinition) ID() string {
	if wf.Workflow == nil {
		return ""
	}
	return wf.Workflow.GetID()
}

// Status returns the workflow status as WfDefStatus.
func (wf *WorkflowDefinition) Status() WfDefStatus {
	if wf.Workflow == nil {
		return WfDefStatusDisabled
	}
	status := wf.Workflow.GetStatus()
	if strings.EqualFold(status, "ENABLED") {
		return WfDefStatusEnabled
	}
	return WfDefStatusDisabled
}

// NewWorkflowDefinition creates a new WorkflowDefinition aggregate.
func NewWorkflowDefinition(name, description string, category WfCategory, definitionYAML string, isSystem bool) *WorkflowDefinition {
	teWorkflow := workflow.NewWorkflow(name, description)
	now := shared.Now()
	return &WorkflowDefinition{
		Workflow:       teWorkflow,
		Category:       category,
		DefinitionYAML: definitionYAML,
		Version:        1,
		IsSystem:       isSystem,
		UpdatedAt:      now,
	}
}

// Enable enables the workflow definition.
func (wf *WorkflowDefinition) Enable() {
	wf.Workflow.SetStatus("ENABLED")
	wf.UpdatedAt = shared.Now()
}

// Disable disables the workflow definition.
func (wf *WorkflowDefinition) Disable() {
	wf.Workflow.SetStatus("DISABLED")
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
	return strings.EqualFold(wf.Workflow.GetStatus(), "ENABLED")
}

// CanCreateInstance checks if a new instance can be created.
func (wf *WorkflowDefinition) CanCreateInstance() error {
	if !wf.IsEnabled() {
		return shared.NewDomainError(shared.ErrCodeInvalidState, "workflow definition is disabled", nil)
	}
	return nil
}

// ==================== 辅助方法（使用 Task Engine 类型的方法）====================

// NewWorkflowInstance creates a new WorkflowInstance using Task Engine.
func NewWorkflowInstance(workflowID string) *WorkflowInstance {
	return workflow.NewWorkflowInstance(workflowID)
}

// ==================== qdhub 特定的枚举类型 ====================

// WfCategory represents workflow category (qdhub specific).
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

// TriggerType represents trigger type (qdhub specific, for compatibility).
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

// WfInstStatus represents workflow instance status (qdhub specific, for compatibility).
// Maps to Task Engine status: Ready->Pending, Running->Running, Paused->Paused, Success->Success, Failed->Failed, Terminated->Cancelled
type WfInstStatus string

const (
	WfInstStatusPending   WfInstStatus = "pending"   // Maps to "Ready"
	WfInstStatusRunning   WfInstStatus = "running"   // Maps to "Running"
	WfInstStatusPaused    WfInstStatus = "paused"    // Maps to "Paused"
	WfInstStatusSuccess   WfInstStatus = "success"   // Maps to "Success"
	WfInstStatusFailed    WfInstStatus = "failed"    // Maps to "Failed"
	WfInstStatusCancelled WfInstStatus = "cancelled" // Maps to "Terminated"
)

// String returns the string representation of the workflow instance status.
func (wis WfInstStatus) String() string {
	return string(wis)
}

// WfDefStatus represents workflow definition status (qdhub specific, for compatibility).
type WfDefStatus string

const (
	WfDefStatusEnabled  WfDefStatus = "enabled"  // Maps to "ENABLED"
	WfDefStatusDisabled WfDefStatus = "disabled" // Maps to "DISABLED"
)

// String returns the string representation of the workflow definition status.
func (wds WfDefStatus) String() string {
	return string(wds)
}

// TaskStatus represents task status (qdhub specific, for compatibility).
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
