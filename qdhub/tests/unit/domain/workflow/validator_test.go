package workflow_test

import (
	"testing"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

func TestWorkflowValidator_ValidateWorkflowDefinition(t *testing.T) {
	validator := workflow.NewWorkflowValidator()

	validDefinition := func() *workflow.WorkflowDefinition {
		return workflow.NewWorkflowDefinition(
			"Daily Sync Workflow",
			"每日数据同步工作流",
			workflow.WfCategorySync,
			`name: daily_sync
tasks:
  - name: fetch_data
    job: fetch_stock_data
  - name: save_data
    job: save_to_db
    depends_on:
      - fetch_data`,
			false,
		)
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
			name:    "empty name",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Workflow.Name = "" },
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
			name:    "disabled status is valid",
			modify:  func(wf *workflow.WorkflowDefinition) { wf.Disable() },
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
			ID:         shared.NewID().String(),
			WorkflowID: shared.NewID().String(),
			Status:     "Running",
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
			name:    "empty WorkflowID",
			modify:  func(wi *workflow.WorkflowInstance) { wi.WorkflowID = "" },
			wantErr: true,
		},
		{
			name:    "invalid status",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "InvalidStatus" },
			wantErr: true,
		},
		{
			name:    "invalid status",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "InvalidStatus" },
			wantErr: true,
		},
		{
			name:    "Ready status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "Ready" },
			wantErr: false,
		},
		{
			name:    "Running status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "Running" },
			wantErr: false,
		},
		{
			name:    "Paused status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "Paused" },
			wantErr: false,
		},
		{
			name:    "Success status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "Success" },
			wantErr: false,
		},
		{
			name:    "Failed status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "Failed" },
			wantErr: false,
		},
		{
			name:    "Terminated status is valid",
			modify:  func(wi *workflow.WorkflowInstance) { wi.Status = "Terminated" },
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
			name:    "valid minimal YAML",
			yaml:    "name: test",
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
