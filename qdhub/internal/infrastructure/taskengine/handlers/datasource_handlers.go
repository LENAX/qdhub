// Package handlers provides Task Engine task handlers for QDHub workflows.
package handlers

import (
	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/task"
)

// ==================== Token 验证 Handlers ====================

// TokenValidationSuccessHandler handles successful token validation.
func TokenValidationSuccessHandler(tc *task.TaskContext) {
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if valid, ok := resultMap["valid"].(bool); ok {
				if valid {
					logrus.Printf("[TokenValidation] ✅ Token is valid - TaskID: %s", tc.TaskID)
				} else {
					logrus.Printf("[TokenValidation] ⚠️ Token is invalid - TaskID: %s", tc.TaskID)
				}
			}
		}
	}
}

// TokenValidationFailureHandler handles failed token validation.
func TokenValidationFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	logrus.Printf("[TokenValidation] ❌ Validation failed - TaskID: %s, Error: %s",
		tc.TaskID, errMsg)
}

// ==================== 数据源连接 Handlers ====================

// DataSourceConnectSuccessHandler handles successful data source connection.
func DataSourceConnectSuccessHandler(tc *task.TaskContext) {
	logrus.Printf("[DataSource] ✅ Connection succeeded - Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if dataSourceName, ok := resultMap["data_source_name"]; ok {
				logrus.Printf("[DataSource] Connected to: %v", dataSourceName)
			}
		}
	}
}

// DataSourceConnectFailureHandler handles failed data source connection.
func DataSourceConnectFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	logrus.Printf("[DataSource] ❌ Connection failed - Task: %s, TaskID: %s, Error: %s",
		tc.TaskName, tc.TaskID, errMsg)
}
