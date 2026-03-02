package handlers_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/handlers"
)

func TestTokenValidationSuccessHandler_Valid(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_result_data": map[string]interface{}{
			"valid": true,
		},
	})

	// Should not panic
	handlers.TokenValidationSuccessHandler(tc)
}

func TestTokenValidationSuccessHandler_Invalid(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_result_data": map[string]interface{}{
			"valid": false,
		},
	})

	// Should not panic
	handlers.TokenValidationSuccessHandler(tc)
}

func TestTokenValidationSuccessHandler_NoResultData(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	// Should not panic
	handlers.TokenValidationSuccessHandler(tc)
}

func TestTokenValidationFailureHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_error_message": "token expired",
	})

	// Should not panic
	handlers.TokenValidationFailureHandler(tc)
}

func TestDataSourceConnectSuccessHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_result_data": map[string]interface{}{
			"data_source_name": "tushare",
		},
	})

	// Should not panic
	handlers.DataSourceConnectSuccessHandler(tc)
}

func TestDataSourceConnectFailureHandler(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"_error_message": "connection refused",
	})

	// Should not panic
	handlers.DataSourceConnectFailureHandler(tc)
}
