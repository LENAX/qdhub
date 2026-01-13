// Package handlers provides Task Engine task handlers for QDHub workflows.
package handlers

import (
	"log"

	"github.com/LENAX/task-engine/pkg/core/task"
)

// ==================== 数据同步工作流 Handlers ====================

// DataSyncStartHandler handles the start of a data sync workflow.
func DataSyncStartHandler(tc *task.TaskContext) {
	log.Printf("[DataSync] 🚀 Workflow started - Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)
}

// DataSyncSuccessHandler handles successful data sync tasks.
func DataSyncSuccessHandler(tc *task.TaskContext) {
	log.Printf("[DataSync] ✅ Task succeeded - Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	// Get result data if available
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			// Log sync metrics
			if count, ok := resultMap["count"]; ok {
				log.Printf("[DataSync] 💾 Saved %v records", count)
			}
			if total, ok := resultMap["total"]; ok {
				log.Printf("[DataSync] 📊 Total fetched: %v records", total)
			}
			if apiName, ok := resultMap["api_name"]; ok {
				log.Printf("[DataSync] 📡 API: %v", apiName)
			}
			if hasMore, ok := resultMap["has_more"].(bool); ok && hasMore {
				log.Printf("[DataSync] ⚠️ More records available for pagination")
			}
			// 模板任务生成的子任务数量
			if generated, ok := resultMap["generated"]; ok {
				log.Printf("[DataSync] 🔄 Generated %v sub-tasks", generated)
			}
		}
	}
}

// DataSyncFailureHandler handles failed data sync tasks.
func DataSyncFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	log.Printf("[DataSync] ❌ Task failed - Task: %s, TaskID: %s, Error: %s",
		tc.TaskName, tc.TaskID, errMsg)

	// Check for specific error types
	// Could implement retry logic or fallback strategies here
}

// DataSyncCompleteHandler handles completion of the entire data sync workflow.
func DataSyncCompleteHandler(tc *task.TaskContext) {
	status := tc.GetParamString("_status")
	log.Printf("[DataSync] 🏁 Workflow completed - WorkflowInstanceID: %s, Status: %s",
		tc.WorkflowInstanceID, status)
}

// ==================== 建表工作流 Handlers ====================

// TableCreationStartHandler handles the start of a table creation workflow.
func TableCreationStartHandler(tc *task.TaskContext) {
	log.Printf("[TableCreation] 🔨 Workflow started - Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)
}

// TableCreationSuccessHandler handles successful table creation tasks.
func TableCreationSuccessHandler(tc *task.TaskContext) {
	log.Printf("[TableCreation] ✅ Task succeeded - Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	// Get result data if available
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			// Log table creation metrics
			if tableName, ok := resultMap["table_name"]; ok {
				log.Printf("[TableCreation] 📋 Table: %v", tableName)
			}
			if fieldCount, ok := resultMap["field_count"]; ok {
				log.Printf("[TableCreation] 📊 Fields: %v", fieldCount)
			}
			// 模板任务生成的子任务数量
			if generated, ok := resultMap["generated"]; ok {
				log.Printf("[TableCreation] 🔄 Generated %v sub-tasks", generated)
			}
		}
	}
}

// TableCreationFailureHandler handles failed table creation tasks.
func TableCreationFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	log.Printf("[TableCreation] ❌ Task failed - Task: %s, TaskID: %s, Error: %s",
		tc.TaskName, tc.TaskID, errMsg)
}

// TableCreationCompleteHandler handles completion of the entire table creation workflow.
func TableCreationCompleteHandler(tc *task.TaskContext) {
	status := tc.GetParamString("_status")
	log.Printf("[TableCreation] 🏁 Workflow completed - WorkflowInstanceID: %s, Status: %s",
		tc.WorkflowInstanceID, status)
}
