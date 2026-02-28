// Package handlers provides Task Engine task handlers for QDHub workflows.
package handlers

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/task"
)

// ==================== 数据同步工作流 Handlers ====================

// DataSyncStartHandler handles the start of a data sync workflow.
func DataSyncStartHandler(tc *task.TaskContext) {
	logrus.Printf("[DataSync] 🚀 Workflow started - Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)
}

// sumCountFromSubTaskResults 从模板任务的子任务结果中汇总 count，用于动态生成任务的执行历史数量统计。
// 当模板任务（如 GenerateDataSyncSubTasks）的 _result_data 仅有 generated/api_name 无 count 时，
// 若引擎已注入子任务结果，则汇总各子任务的 count 返回；否则返回 0。
func sumCountFromSubTaskResults(tc *task.TaskContext) int64 {
	results := tc.GetSubTaskResults()
	var sum int64
	for _, r := range results {
		if !r.IsSuccess() || r.Result == nil {
			continue
		}
		v := r.Result["count"]
		switch n := v.(type) {
		case int:
			sum += int64(n)
		case int32:
			sum += int64(n)
		case int64:
			sum += n
		case float64:
			sum += int64(n)
		}
	}
	return sum
}

// DataSyncSuccessHandler handles successful data sync tasks.
func DataSyncSuccessHandler(tc *task.TaskContext) {
	logrus.Printf("[DataSync] ✅ Task succeeded - Task: %s, TaskID: %s",
		tc.TaskName, tc.TaskID)

	// Get result data if available
	result := tc.GetParam("_result_data")
	if result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			apiName, _ := resultMap["api_name"].(string)
			var count64 int64
			if v, ok := resultMap["count"]; ok {
				switch n := v.(type) {
				case int:
					count64 = int64(n)
				case int32:
					count64 = int64(n)
				case int64:
					count64 = n
				case float64:
					count64 = int64(n)
				}
			}

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

			// 动态生成的任务：模板任务返回 generated/api_name 无 count 时，尝试从子任务结果聚合（兼容引擎未在子任务完成时回调的场景）
			if count64 == 0 && apiName != "" {
				if agg := sumCountFromSubTaskResults(tc); agg > 0 {
					count64 = agg
					logrus.Printf("[DataSync] 📊 从 %s 子任务结果聚合 record_count=%d", apiName, count64)
				}
			}

			// 模板任务仅有 generated、无 count 且未聚合到子任务 count 时，不写入明细，由各子任务完成时各自写入（避免一条 0 的冗余行）
			_, isTemplateOnly := resultMap["generated"]
			if isTemplateOnly && count64 == 0 {
				// skip RecordTaskResult
			} else {
				// Persist per-task detail for stats/debugging (best-effort; must not break workflow)
				func() {
					defer func() { _ = recover() }()
					invoker, ok := tc.GetDependency("SyncCallbackInvoker")
					if ok && invoker != nil {
						type recorder interface {
							RecordTaskResult(ctx context.Context, workflowInstID, apiName, taskID string, recordCount int64, success bool, errorMessage string) error
						}
						if svc, ok := invoker.(recorder); ok && apiName != "" {
							if err := svc.RecordTaskResult(context.Background(), tc.WorkflowInstanceID, apiName, tc.TaskID, count64, true, ""); err != nil {
								logrus.Warnf("[DataSync] record task result failed: %v", err)
							}
						}
					}
				}()
			}
		}
	}
}

// DataSyncFailureHandler handles failed data sync tasks.
func DataSyncFailureHandler(tc *task.TaskContext) {
	errMsg := tc.GetParamString("_error_message")
	if errMsg == "" {
		errMsg = "unknown error"
	}
	logrus.Errorf("[DataSync] task failed: taskName=%s, taskID=%s, workflowInstID=%s, err=%s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID, errMsg)

	// Persist per-task failure detail for stats/debugging (best-effort; must not break workflow)
	apiName := tc.GetParamString("api_name")
	if apiName == "" {
		// fallback: some tasks use name like "SyncAPIData_daily"
		apiName = tc.TaskName
	}
	func() {
		defer func() { _ = recover() }()
		invoker, ok := tc.GetDependency("SyncCallbackInvoker")
		if ok && invoker != nil && apiName != "" {
			type recorder interface {
				RecordTaskResult(ctx context.Context, workflowInstID, apiName, taskID string, recordCount int64, success bool, errorMessage string) error
			}
			if svc, ok := invoker.(recorder); ok {
				if err := svc.RecordTaskResult(context.Background(), tc.WorkflowInstanceID, apiName, tc.TaskID, 0, false, errMsg); err != nil {
					logrus.Warnf("[DataSync] record task result failed: %v", err)
				}
			}
		}
	}()
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

	// 引擎可能传 "SUCCESS" / "Success" / "success"，统一按忽略大小写判定
	success := statusStr == "" || strings.EqualFold(statusStr, "success")
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
