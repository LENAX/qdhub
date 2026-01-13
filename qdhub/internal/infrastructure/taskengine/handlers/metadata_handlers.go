// Package handlers provides Task Engine task handlers for QDHub workflows.
package handlers

import (
	"log"

	"github.com/LENAX/task-engine/pkg/core/task"
)

// ==================== 元数据刷新工作流 Handlers ====================

// MetadataRefreshStartHandler handles the start of a metadata refresh workflow.
func MetadataRefreshStartHandler(tc *task.TaskContext) {
	log.Printf("[MetadataRefresh] 🚀 Workflow started - Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)
}

// MetadataRefreshSuccessHandler handles successful completion of metadata tasks.
func MetadataRefreshSuccessHandler(tc *task.TaskContext) {
	log.Printf("[MetadataRefresh] ✅ Task succeeded - Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	// Get result data if available
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			// Log specific success metrics
			if savedCount, ok := resultMap["saved_count"]; ok {
				log.Printf("[MetadataRefresh] 💾 Saved %v items", savedCount)
			}
			if apiCount, ok := resultMap["api_count"]; ok {
				log.Printf("[MetadataRefresh] 📡 Found %v APIs", apiCount)
			}
			// 模板任务生成的子任务数量
			if generated, ok := resultMap["generated"]; ok {
				log.Printf("[MetadataRefresh] 🔄 Generated %v sub-tasks", generated)
			}
		}
	}
}

// MetadataRefreshFailureHandler handles failed metadata tasks.
func MetadataRefreshFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	log.Printf("[MetadataRefresh] ❌ Task failed - Task: %s, TaskID: %s, Error: %s",
		tc.TaskName, tc.TaskID, errMsg)

	// Could trigger alerts, notifications, or cleanup logic here
}

// MetadataRefreshCompleteHandler handles completion of the entire metadata refresh workflow.
func MetadataRefreshCompleteHandler(tc *task.TaskContext) {
	status := tc.GetParamString("_status")
	log.Printf("[MetadataRefresh] 🏁 Workflow completed - WorkflowInstanceID: %s, Status: %s",
		tc.WorkflowInstanceID, status)
}

// ==================== 通用 Handlers ====================

// LogProgressHandler logs task progress for monitoring.
func LogProgressHandler(tc *task.TaskContext) {
	log.Printf("[Progress] 📊 Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)
}

// LogErrorHandler logs errors for debugging.
func LogErrorHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	log.Printf("[Error] ❌ Task: %s, TaskID: %s, Error: %s",
		tc.TaskName, tc.TaskID, errMsg)
}

// LogSuccessHandler logs successful task completion.
func LogSuccessHandler(tc *task.TaskContext) {
	log.Printf("[Success] ✅ Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	// Log result summary if available
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if status, ok := resultMap["status"]; ok {
				log.Printf("[Success] Status: %v", status)
			}
		}
	}
}
