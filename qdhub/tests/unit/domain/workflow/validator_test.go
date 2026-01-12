package workflow_test

import (
	"testing"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

func TestWorkflowValidator_ValidateWorkflowDefinition(t *testing.T) {
	validator := workflow.NewWorkflowValidator()

	validDefinition := func() *workflow.WorkflowDefinition {
		return &workflow.WorkflowDefinition{
			ID:          shared.NewID(),
			Name:        "Daily Sync Workflow",
			Description: "每日数据同步工作流",
			Category:    workflow.WfCategorySync,
			DefinitionYAML: `
name: daily_sync
tasks:
  - name: fetch_data
    job: fetch_stock_data
  - name: save_data
    job: save_to_db
    depends_on:
      - fetch_data
`,
			Version:  1,
			Status:   workflow.WfDefStatusEnabled,
			IsSystem: false,
		}
	}

	tests := []struct {
		name    string
		modify  func(*workflow.WorkflowDefinition)
		wantErr bool
	}{
		{
			name:    "valid workflow definition",
			modify:  func(wf *workflow.WorkflowDefinition) {},
			wantErr: false,
		},
		{
			name:    "nil workflow definition",
			modify:  func(wf *workflow.WorkflowDefinition) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty name",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Name = "" },
			wantErr: true,
		},
		{
			name:    "invalid category",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Category = "invalid" },
			wantErr: true,
		},
		{
			name:    "valid metadata category",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Category = workflow.WfCategoryMetadata },
			wantErr: false,
		},
		{
			name:    "valid custom category",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Category = workflow.WfCategoryCustom },
			wantErr: false,
		},
		{
			name:    "empty definition YAML",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.DefinitionYAML = "" },
			wantErr: true,
		},
		{
			name:    "invalid YAML syntax",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.DefinitionYAML = "invalid: yaml: content:" },
			wantErr: true,
		},
		{
			name:    "zero version",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Version = 0 },
			wantErr: true,
		},
		{
			name:    "negative version",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Version = -1 },
			wantErr: true,
		},
		{
			name:    "invalid status",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Status = "invalid" },
			wantErr: true,
		},
		{
			name:    "disabled status is valid",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Status = workflow.WfDefStatusDisabled },
			wantErr: false,
		},
		{
			name:    "system workflow is valid",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.IsSystem = true },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wf *workflow.WorkflowDefinition
			if tt.name == "nil workflow definition" {
				wf = nil
			} else {
				wf = validDefinition()
				tt.modify(wf)
			}

			err := validator.ValidateWorkflowDefinition(wf)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateWorkflowDefinition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorkflowValidator_ValidateWorkflowInstance(t *testing.T) {
	validator := workflow.NewWorkflowValidator()

	validInstance := func() *workflow.WorkflowInstance {
		return &workflow.WorkflowInstance{
			ID:               shared.NewID(),
			WorkflowDefID:    shared.NewID(),
			EngineInstanceID: "engine-instance-123",
			TriggerType:      workflow.TriggerTypeManual,
			TriggerParams:    map[string]interface{}{"key": "value"},
			Status:           workflow.WfInstStatusRunning,
			Progress:         50.0,
			StartedAt:        shared.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*workflow.WorkflowInstance)
		wantErr bool
	}{
		{
			name:    "valid workflow instance",
			modify:  func(wi *workflow.WorkflowInstance) {},
			wantErr: false,
		},
		{
			name:    "nil workflow instance",
			modify:  func(wi *workflow.WorkflowInstance) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(wi *workflow.WorkflowInstance) { wi.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty WorkflowDefID",
			modify:  func(wi *workflow.WorkflowInstance) { wi.WorkflowDefID = "" },
			wantErr: true,
		},
		{
			name:    "empty EngineInstanceID",
			modify:  func(wi *workflow.WorkflowInstance) { wi.EngineInstanceID = "" },
			wantErr: true,
		},
		{
			name:    "invalid trigger type",
			modify:  func(wi *workflow.WorkflowInstance) { wi.TriggerType = "invalid" },
			wantErr: true,
		},
		{
			name:    "cron trigger type is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.TriggerType = workflow.TriggerTypeCron },
			wantErr: false,
		},
		{
			name:    "event trigger type is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.TriggerType = workflow.TriggerTypeEvent },
			wantErr: false,
		},
		{
			name:    "invalid status",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "invalid" },
			wantErr: true,
		},
		{
			name:    "pending status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = workflow.WfInstStatusPending },
			wantErr: false,
		},
		{
			name:    "paused status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = workflow.WfInstStatusPaused },
			wantErr: false,
		},
		{
			name:    "success status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = workflow.WfInstStatusSuccess },
			wantErr: false,
		},
		{
			name:    "failed status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = workflow.WfInstStatusFailed },
			wantErr: false,
		},
		{
			name:    "cancelled status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = workflow.WfInstStatusCancelled },
			wantErr: false,
		},
		{
			name:    "negative progress",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Progress = -1.0 },
			wantErr: true,
		},
		{
			name:    "progress over 100",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Progress = 101.0 },
			wantErr: true,
		},
		{
			name:    "zero progress is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Progress = 0.0 },
			wantErr: false,
		},
		{
			name:    "100 progress is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Progress = 100.0 },
			wantErr: false,
		},
		{
			name:    "nil trigger params is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.TriggerParams = nil },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wi *workflow.WorkflowInstance
			if tt.name == "nil workflow instance" {
				wi = nil
			} else {
				wi = validInstance()
				tt.modify(wi)
			}

			err := validator.ValidateWorkflowInstance(wi)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateWorkflowInstance() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorkflowValidator_ValidateDefinitionYAML(t *testing.T) {
	validator := workflow.NewWorkflowValidator()

	tests := []struct {
		name    string
		yaml    string
		wantErr bool
	}{
		{
			name: "valid YAML",
			yaml: `
name: test_workflow
tasks:
  - name: task1
    job: job1
`,
			wantErr: false,
		},
		{
			name:    "empty YAML",
			yaml:    "",
			wantErr: true,
		},
		{
			name:    "whitespace only YAML",
			yaml:    "   ",
			wantErr: true,
		},
		{
			name:    "invalid YAML syntax",
			yaml:    "invalid: yaml: syntax:",
			wantErr: true,
		},
		{
			name: "valid minimal YAML",
			yaml: "name: test",
			wantErr: false,
		},
		{
			name: "valid complex YAML",
			yaml: `
name: complex_workflow
description: A complex workflow
tasks:
  - name: task1
    job: fetch_data
    timeout: 60
    retry: 3
  - name: task2
    job: process_data
    depends_on:
      - task1
  - name: task3
    job: save_data
    depends_on:
      - task2
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateDefinitionYAML(tt.yaml)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDefinitionYAML() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWorkflowValidator_ValidateTriggerParams(t *testing.T) {
	validator := workflow.NewWorkflowValidator()

	tests := []struct {
		name        string
		triggerType workflow.TriggerType
		params      map[string]interface{}
		wantErr     bool
	}{
		{
			name:        "manual trigger with nil params",
			triggerType: workflow.TriggerTypeManual,
			params:      nil,
			wantErr:     false,
		},
		{
			name:        "manual trigger with params",
			triggerType: workflow.TriggerTypeManual,
			params:      map[string]interface{}{"key": "value"},
			wantErr:     false,
		},
		{
			name:        "cron trigger with valid expression",
			triggerType: workflow.TriggerTypeCron,
			params:      map[string]interface{}{"cron_expression": "0 9 * * *"},
			wantErr:     false,
		},
		{
			name:        "cron trigger with empty expression",
			triggerType: workflow.TriggerTypeCron,
			params:      map[string]interface{}{"cron_expression": ""},
			wantErr:     true,
		},
		{
			name:        "event trigger with valid event type",
			triggerType: workflow.TriggerTypeEvent,
			params:      map[string]interface{}{"event_type": "data_updated"},
			wantErr:     false,
		},
		{
			name:        "event trigger with empty event type",
			triggerType: workflow.TriggerTypeEvent,
			params:      map[string]interface{}{"event_type": ""},
			wantErr:     true,
		},
		{
			name:        "event trigger without event type",
			triggerType: workflow.TriggerTypeEvent,
			params:      map[string]interface{}{},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateTriggerParams(tt.triggerType, tt.params)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTriggerParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
