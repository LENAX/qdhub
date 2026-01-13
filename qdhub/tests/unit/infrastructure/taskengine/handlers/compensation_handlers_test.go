package handlers_test

import (
	"testing"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/infrastructure/taskengine/handlers"
)

// mockTaskContext creates a mock TaskContext for testing
// Note: Without a valid Registry, GetDependency will return false
func mockTaskContext(params map[string]interface{}) *task.TaskContext {
	tc := &task.TaskContext{
		TaskID:             "test-task-id",
		TaskName:           "TestTask",
		WorkflowInstanceID: "test-workflow-instance-id",
		Params:             params,
	}
	return tc
}

func TestCompensateSaveCategoriesHandler_NoDataSourceID(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic, just log warning
	handlers.CompensateSaveCategoriesHandler(tc)
}

func TestCompensateSaveAPIMetadataHandler_NoAPIID(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic, just log warning
	handlers.CompensateSaveAPIMetadataHandler(tc)
}

func TestCompensateSaveAPIMetadataBatchHandler_NoDataSourceID(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic, just log warning
	handlers.CompensateSaveAPIMetadataBatchHandler(tc)
}

func TestCompensateCreateTableHandler_NoParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic, just log warning
	handlers.CompensateCreateTableHandler(tc)
}

func TestCompensateSyncDataHandler_NoParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic, just log warning
	handlers.CompensateSyncDataHandler(tc)
}

func TestCompensateGenericHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"param1": "value1",
		"param2": 123,
	})

	// Should not panic, just log
	handlers.CompensateGenericHandler(tc)
}
