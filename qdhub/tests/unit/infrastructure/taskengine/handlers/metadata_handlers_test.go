package handlers_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/handlers"
)

func TestMetadataRefreshStartHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.MetadataRefreshStartHandler(tc)
}

func TestMetadataRefreshSuccessHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_result_data": map[string]interface{}{
			"saved_count": 10,
			"api_count":   5,
			"generated":   3,
		},
	})

	// Should not panic
	handlers.MetadataRefreshSuccessHandler(tc)
}

func TestMetadataRefreshSuccessHandler_NoResultData(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.MetadataRefreshSuccessHandler(tc)
}

func TestMetadataRefreshFailureHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_error_message": "connection timeout",
	})

	// Should not panic
	handlers.MetadataRefreshFailureHandler(tc)
}

func TestMetadataRefreshCompleteHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_status": "Success",
	})

	// Should not panic
	handlers.MetadataRefreshCompleteHandler(tc)
}

func TestLogProgressHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.LogProgressHandler(tc)
}

func TestLogErrorHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_error_message": "test error",
	})

	// Should not panic
	handlers.LogErrorHandler(tc)
}

func TestLogSuccessHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_result_data": map[string]interface{}{
			"status": "completed",
		},
	})

	// Should not panic
	handlers.LogSuccessHandler(tc)
}

func TestLogSuccessHandler_NoResultData(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.LogSuccessHandler(tc)
}
