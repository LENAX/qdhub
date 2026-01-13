package handlers_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/handlers"
)

func TestDataSyncStartHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.DataSyncStartHandler(tc)
}

func TestDataSyncSuccessHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_result_data": map[string]interface{}{
			"count":     100,
			"total":     150,
			"api_name":  "daily",
			"has_more":  true,
			"generated": 5,
		},
	})

	// Should not panic
	handlers.DataSyncSuccessHandler(tc)
}

func TestDataSyncSuccessHandler_NoResultData(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.DataSyncSuccessHandler(tc)
}

func TestDataSyncFailureHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_error_message": "rate limit exceeded",
	})

	// Should not panic
	handlers.DataSyncFailureHandler(tc)
}

func TestDataSyncCompleteHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_status": "Success",
	})

	// Should not panic
	handlers.DataSyncCompleteHandler(tc)
}

func TestTableCreationStartHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.TableCreationStartHandler(tc)
}

func TestTableCreationSuccessHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_result_data": map[string]interface{}{
			"table_name":  "daily",
			"field_count": 10,
			"generated":   3,
		},
	})

	// Should not panic
	handlers.TableCreationSuccessHandler(tc)
}

func TestTableCreationFailureHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_error_message": "table already exists",
	})

	// Should not panic
	handlers.TableCreationFailureHandler(tc)
}

func TestTableCreationCompleteHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_status": "Success",
	})

	// Should not panic
	handlers.TableCreationCompleteHandler(tc)
}
