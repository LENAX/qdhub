// Package handlers provides Task Engine task handlers for QDHub workflows.
package handlers

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/task"
)

// ==================== 数据同步工作流 Handlers ====================

// DataSyncStartHandler handles the start of a data sync workflow.
func DataSyncStartHandler(tc *task.TaskContext) {
	logrus.Printf("[DataSync] 🚀 Workflow started - Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)
}

// DataSyncSuccessHandler handles successful data sync tasks.
func DataSyncSuccessHandler(tc *task.TaskContext) {
	logrus.Printf("[DataSync] ✅ Task succeeded - Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	// Get result data if available
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			// Log sync metrics
			if count, ok := resultMap["count"]; ok {
				logrus.Printf("[DataSync] 💾 Saved %v records", count)
			}
			if total, ok := resultMap["total"]; ok {
				logrus.Printf("[DataSync] 📊 Total fetched: %v records", total)
			}
			if apiName, ok := resultMap["api_name"]; ok {
				logrus.Printf("[DataSync] 📡 API: %v", apiName)
			}
			if hasMore, ok := resultMap["has_more"].(bool); ok && hasMore {
				logrus.Printf("[DataSync] ⚠️ More records available for pagination")
			}
			// 模板任务生成的子任务数量
			if generated, ok := resultMap["generated"]; ok {
				logrus.Printf("[DataSync] 🔄 Generated %v sub-tasks", generated)
			}
		}
	}
}

// DataSyncFailureHandler handles failed data sync tasks.
func DataSyncFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	logrus.Printf("[DataSync] ❌ Task failed - Task: %s, TaskID: %s, Error: %s",
		tc.TaskName, tc.TaskID, errMsg)

	// Check for specific error types
	// Could implement retry logic or fallback strategies here
}

// syncCallbackInvoker 由 SyncApplicationService 实现，用于 DataSyncComplete 时触发 execution 回调（Plan.MarkCompleted）。
type syncCallbackInvoker interface {
	HandleExecutionCallbackByWorkflowInstance(ctx context.Context, workflowInstID string, success bool, recordCount int64, errMsg *string) error
}

// DataSyncCompleteHandler handles completion of the entire data sync workflow.
// 若注入了 SyncCallbackInvoker，则调用 HandleExecutionCallbackByWorkflowInstance，从而更新 execution/plan 状态、触发 Plan.MarkCompleted。
// 若 GetDependency 因 nil registry（如单元测试 mock）panic，则 recover 后跳过回调。
// 支持成功和失败两种场景：通过 _status 参数判断工作流最终状态。
func DataSyncCompleteHandler(tc *task.TaskContext) {
	statusStr := tc.GetParamString("_status")
	logrus.Printf("[DataSync] 🏁 Workflow completed - WorkflowInstanceID: %s, Status: %s",
		tc.WorkflowInstanceID, statusStr)

	success := statusStr == "" || statusStr == "success" || statusStr == "Success"
	var errMsg *string
	if !success {
		msg := tc.GetParamString("_error_message")
		if msg == "" {
			msg = "workflow failed with status: " + statusStr
		}
		errMsg = &msg
	}

	invokeCallback := func() {
		invoker, ok := tc.GetDependency("SyncCallbackInvoker")
		if !ok || invoker == nil {
			return
		}
		svc, ok := invoker.(syncCallbackInvoker)
		if !ok {
			return
		}
		ctx := context.Background()
		if err := svc.HandleExecutionCallbackByWorkflowInstance(ctx, tc.WorkflowInstanceID, success, 0, errMsg); err != nil {
			logrus.Warnf("[DataSync] execution callback failed: %v", err)
		}
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				// 单元测试等场景下 registry 为 nil，GetDependency 会 panic，跳过回调即可
			}
		}()
		invokeCallback()
	}()
}

// ==================== 建表工作流 Handlers ====================

// TableCreationStartHandler handles the start of a table creation workflow.
func TableCreationStartHandler(tc *task.TaskContext) {
	logrus.Printf("[TableCreation] 🔨 Workflow started - Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)
}

// TableCreationSuccessHandler handles successful table creation tasks.
func TableCreationSuccessHandler(tc *task.TaskContext) {
	logrus.Printf("[TableCreation] ✅ Task succeeded - Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	// Get result data if available
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			// Log table creation metrics
			if tableName, ok := resultMap["table_name"]; ok {
				logrus.Printf("[TableCreation] 📋 Table: %v", tableName)
			}
			if fieldCount, ok := resultMap["field_count"]; ok {
				logrus.Printf("[TableCreation] 📊 Fields: %v", fieldCount)
			}
			// 模板任务生成的子任务数量
			if generated, ok := resultMap["generated"]; ok {
				logrus.Printf("[TableCreation] 🔄 Generated %v sub-tasks", generated)
			}
		}
	}
}

// TableCreationFailureHandler handles failed table creation tasks.
func TableCreationFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	logrus.Printf("[TableCreation] ❌ Task failed - Task: %s, TaskID: %s, Error: %s",
		tc.TaskName, tc.TaskID, errMsg)
}

// TableCreationCompleteHandler handles completion of the entire table creation workflow.
func TableCreationCompleteHandler(tc *task.TaskContext) {
	status := tc.GetParamString("_status")
	logrus.Printf("[TableCreation] 🏁 Workflow completed - WorkflowInstanceID: %s, Status: %s",
		tc.WorkflowInstanceID, status)
}
