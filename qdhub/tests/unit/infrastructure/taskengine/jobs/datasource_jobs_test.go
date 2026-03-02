package jobs_test

import (
	"testing"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/infrastructure/taskengine/jobs"
)

// mockTaskContext creates a mock TaskContext for testing
// Note: Without a valid Registry, GetDependency will panic, so we only test parameter validation
func mockTaskContext(params map[string]interface{}) *task.TaskContext {
	tc := &task.TaskContext{
		TaskID:             "test-task-id",
		TaskName:           "TestTask",
		WorkflowInstanceID: "test-workflow-instance-id",
		Params:             params,
	}
	return tc
}

func TestQueryDataJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.QueryDataJob(tc)
	if err == nil {
		t.Error("expected error for missing params")
	}
	if err.Error() != "data_source_name and api_name are required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestQueryDataJob_MissingDataSourceName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"api_name": "daily",
	})

	_, err := jobs.QueryDataJob(tc)
	if err == nil {
		t.Error("expected error for missing data_source_name")
	}
}

func TestQueryDataJob_MissingAPIName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"data_source_name": "tushare",
	})

	_, err := jobs.QueryDataJob(tc)
	if err == nil {
		t.Error("expected error for missing api_name")
	}
}

func TestValidateTokenJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.ValidateTokenJob(tc)
	if err == nil {
		t.Error("expected error for missing params")
	}
	if err.Error() != "data_source_name and token are required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestValidateTokenJob_MissingToken(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"data_source_name": "tushare",
	})

	_, err := jobs.ValidateTokenJob(tc)
	if err == nil {
		t.Error("expected error for missing token")
	}
}

func TestTestConnectionJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.TestConnectionJob(tc)
	if err == nil {
		t.Error("expected error for missing data_source_name")
	}
	if err.Error() != "data_source_name is required" {
		t.Errorf("unexpected error message: %v", err)
	}
}
