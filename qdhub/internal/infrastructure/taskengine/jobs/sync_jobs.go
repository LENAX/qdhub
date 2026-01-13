// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/infrastructure/datasource"
)

// ==================== 数据同步 Job Functions ====================

// SyncAPIDataJob 同步单个 API 的数据
// 从 metadata 获取 API 定义，动态调用数据源 API 并保存结果
//
// Input params:
//   - data_source_name: string - 数据源名称 (e.g., "tushare")
//   - api_name: string - API 名称 (e.g., "daily", "stock_basic")
//   - params: map[string]interface{} - 查询参数（可选，覆盖默认参数）
//   - token: string - API Token
//   - target_db_path: string - 目标数据库路径
//   - sync_batch_id: string - 同步批次 ID（用于回滚，默认为 WorkflowInstanceID）
//
// Output:
//   - count: int - 保存的记录数
//   - api_name: string - API 名称
//   - fields: []string - 返回的字段列表
//   - sync_batch_id: string - 同步批次 ID
func SyncAPIDataJob(tc *task.TaskContext) (interface{}, error) {
	// 获取参数
	dataSourceName := tc.GetParamString("data_source_name")
	apiName := tc.GetParamString("api_name")
	token := tc.GetParamString("token")
	targetDBPath := tc.GetParamString("target_db_path")
	syncBatchID := tc.GetParamString("sync_batch_id")

	// 使用 WorkflowInstanceID 作为默认批次 ID
	if syncBatchID == "" {
		syncBatchID = tc.WorkflowInstanceID
	}

	if dataSourceName == "" || apiName == "" {
		return nil, fmt.Errorf("data_source_name and api_name are required")
	}

	log.Printf("📡 [SyncAPIData] 开始同步: %s/%s, BatchID=%s", dataSourceName, apiName, syncBatchID)

	// 获取 DataSourceRegistry
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		return nil, fmt.Errorf("DataSourceRegistry dependency not found")
	}
	registry := registryInterface.(*datasource.Registry)

	// 获取 API Client
	client, err := registry.GetClient(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to get client: %w", err)
	}

	// 设置 Token
	if token != "" {
		client.SetToken(token)
	}

	// 获取查询参数（可以从上游任务注入，也可以直接传入）
	var params map[string]interface{}
	paramsRaw := tc.GetParam("params")
	if paramsRaw != nil {
		if p, ok := paramsRaw.(map[string]interface{}); ok {
			params = p
		}
	}

	// 调用 API
	ctx := context.Background()
	result, err := client.Query(ctx, apiName, params)
	if err != nil {
		return nil, fmt.Errorf("failed to query %s: %w", apiName, err)
	}

	log.Printf("✅ [SyncAPIData] 获取数据: %s, 记录数=%d", apiName, len(result.Data))

	// 如果指定了目标数据库，保存数据
	savedCount := 0
	var fields []string
	if targetDBPath != "" && len(result.Data) > 0 {
		savedCount, fields, err = saveAPIDataWithBatch(targetDBPath, apiName, result.Data, syncBatchID)
		if err != nil {
			return nil, fmt.Errorf("failed to save data: %w", err)
		}
		log.Printf("💾 [SyncAPIData] 保存数据: %s, 保存记录数=%d", apiName, savedCount)
	}

	// 提取特定字段用于下游任务（如 ts_codes）
	extractedData := extractKeyFields(result.Data, []string{"ts_code", "trade_date", "cal_date"})

	return map[string]interface{}{
		"count":          savedCount,
		"total":          len(result.Data),
		"api_name":       apiName,
		"fields":         fields,
		"has_more":       result.HasMore,
		"extracted_data": extractedData,
		"sync_batch_id":  syncBatchID,
	}, nil
}

// GenerateDataSyncSubTasksJob 生成数据同步子任务（模板任务 Job Function）
// 根据上游任务结果（如 ts_codes 列表）为每个项目生成同步子任务
//
// Input params:
//   - data_source_name: string - 数据源名称
//   - api_name: string - 要调用的 API 名称
//   - param_key: string - 参数键名（如 "ts_code"）
//   - token: string - API Token
//   - target_db_path: string - 目标数据库路径
//   - max_sub_tasks: int - 最大子任务数量（0=不限制）
//   - extra_params: map[string]interface{} - 额外的固定参数
//
// Output:
//   - status: string - 操作状态
//   - generated: int - 生成的子任务数量
func GenerateDataSyncSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	log.Printf("📋 [GenerateDataSyncSubTasks] Job Function 执行, Params: %v", getParamKeys(tc.Params))

	// 获取参数
	dataSourceName := tc.GetParamString("data_source_name")
	apiName := tc.GetParamString("api_name")
	paramKey := tc.GetParamString("param_key")
	token := tc.GetParamString("token")
	targetDBPath := tc.GetParamString("target_db_path")
	maxSubTasks, _ := tc.GetParamInt("max_sub_tasks")

	// 获取额外参数
	var extraParams map[string]interface{}
	if ep := tc.GetParam("extra_params"); ep != nil {
		if epm, ok := ep.(map[string]interface{}); ok {
			extraParams = epm
		}
	}

	// 获取 Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("Engine dependency not found")
	}
	eng := engineInterface.(*engine.Engine)
	taskRegistry := eng.GetRegistry()

	// 从上游任务提取参数值列表
	paramValues := extractParamValuesFromUpstream(tc, paramKey)
	if len(paramValues) == 0 {
		log.Printf("⚠️ [GenerateDataSyncSubTasks] 未找到 %s 列表", paramKey)
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"message":   fmt.Sprintf("未找到 %s 列表，跳过子任务生成", paramKey),
		}, nil
	}

	// 应用数量限制
	if maxSubTasks > 0 && len(paramValues) > maxSubTasks {
		log.Printf("📡 [GenerateDataSyncSubTasks] 限制子任务数量从 %d 到 %d", len(paramValues), maxSubTasks)
		paramValues = paramValues[:maxSubTasks]
	}

	log.Printf("📡 [GenerateDataSyncSubTasks] 为 %d 个 %s 生成子任务", len(paramValues), paramKey)

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	var subTaskInfos []map[string]interface{}
	for _, paramValue := range paramValues {
		subTaskName := fmt.Sprintf("Sync_%s_%s", apiName, paramValue)

		// 构建子任务参数
		subTaskParams := map[string]interface{}{
			"data_source_name": dataSourceName,
			"api_name":         apiName,
			"token":            token,
			"target_db_path":   targetDBPath,
			"sync_batch_id":    workflowInstanceID, // 使用工作流实例 ID 作为批次 ID
			"params": map[string]interface{}{
				paramKey: paramValue,
			},
		}

		// 合并额外参数
		if extraParams != nil {
			paramsMap := subTaskParams["params"].(map[string]interface{})
			for k, v := range extraParams {
				paramsMap[k] = v
			}
		}

		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("同步 %s: %s=%s", apiName, paramKey, paramValue), taskRegistry).
			WithJobFunction("SyncAPIData", subTaskParams).
			WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
			WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
			WithCompensationFunction("CompensateSyncData"). // SAGA 补偿
			Build()
		if err != nil {
			log.Printf("❌ [GenerateDataSyncSubTasks] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			log.Printf("❌ [GenerateDataSyncSubTasks] 添加子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		generatedCount++
		subTaskInfos = append(subTaskInfos, map[string]interface{}{
			"name":      subTaskName,
			"api_name":  apiName,
			"param_key": paramKey,
			paramKey:    paramValue,
		})
		log.Printf("✅ [GenerateDataSyncSubTasks] 子任务已添加: %s", subTaskName)
	}

	log.Printf("✅ [GenerateDataSyncSubTasks] 共生成 %d 个子任务", generatedCount)

	return map[string]interface{}{
		"status":    "success",
		"generated": generatedCount,
		"api_name":  apiName,
		"param_key": paramKey,
		"sub_tasks": subTaskInfos,
	}, nil
}

// DeleteSyncedDataJob 删除同步的数据（用于回滚）
//
// Input params:
//   - api_name: string - API 名称（表名）
//   - target_db_path: string - 目标数据库路径
//   - sync_batch_id: string - 同步批次 ID
//
// Output:
//   - deleted_count: int - 删除的记录数
//   - api_name: string - API 名称
func DeleteSyncedDataJob(tc *task.TaskContext) (interface{}, error) {
	apiName := tc.GetParamString("api_name")
	targetDBPath := tc.GetParamString("target_db_path")
	syncBatchID := tc.GetParamString("sync_batch_id")

	if apiName == "" || targetDBPath == "" || syncBatchID == "" {
		return nil, fmt.Errorf("api_name, target_db_path and sync_batch_id are required")
	}

	log.Printf("🗑️ [DeleteSyncedData] 删除同步数据: %s, BatchID=%s", apiName, syncBatchID)

	db, err := sql.Open("sqlite3", targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	deleteSQL := fmt.Sprintf(`DELETE FROM "%s" WHERE sync_batch_id = ?`, apiName)
	result, err := db.Exec(deleteSQL, syncBatchID)
	if err != nil {
		return nil, fmt.Errorf("failed to delete data: %w", err)
	}

	affected, _ := result.RowsAffected()
	log.Printf("✅ [DeleteSyncedData] 删除成功: %s, 记录数=%d", apiName, affected)

	return map[string]interface{}{
		"deleted_count": affected,
		"api_name":      apiName,
		"sync_batch_id": syncBatchID,
	}, nil
}

// ==================== 辅助函数 ====================

// getParamKeys 获取参数的所有 key（调试用）
func getParamKeys(params map[string]interface{}) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}

// extractParamValuesFromUpstream 从上游任务结果中提取参数值列表
func extractParamValuesFromUpstream(tc *task.TaskContext, paramKey string) []string {
	var values []string

	// 遍历所有 _cached_ 参数
	for key, val := range tc.Params {
		if !strings.HasPrefix(key, "_cached_") {
			continue
		}

		// 尝试从结果中提取
		if resultMap, ok := val.(map[string]interface{}); ok {
			// 检查 extracted_data
			if extracted, ok := resultMap["extracted_data"].(map[string]interface{}); ok {
				if vals, ok := extracted[paramKey]; ok {
					return convertToStringSlice(vals)
				}
			}

			// 直接检查 key (如 ts_codes)
			pluralKey := paramKey + "s"
			if vals, ok := resultMap[pluralKey]; ok {
				return convertToStringSlice(vals)
			}
			if vals, ok := resultMap[paramKey]; ok {
				return convertToStringSlice(vals)
			}
		}
	}

	return values
}

// extractKeyFields 从数据中提取关键字段
func extractKeyFields(data []map[string]interface{}, keys []string) map[string]interface{} {
	result := make(map[string]interface{})

	for _, key := range keys {
		var values []string
		seen := make(map[string]bool)

		for _, row := range data {
			if val, ok := row[key]; ok {
				strVal := fmt.Sprintf("%v", val)
				if !seen[strVal] {
					seen[strVal] = true
					values = append(values, strVal)
				}
			}
		}

		if len(values) > 0 {
			result[key+"s"] = values // 复数形式存储列表
		}
	}

	return result
}

// convertToStringSlice 将接口类型转换为字符串切片
func convertToStringSlice(raw interface{}) []string {
	switch v := raw.(type) {
	case []string:
		return v
	case []interface{}:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				result = append(result, s)
			} else {
				result = append(result, fmt.Sprintf("%v", item))
			}
		}
		return result
	}
	return nil
}

// saveAPIDataWithBatch 保存 API 数据到数据库（带批次 ID）
func saveAPIDataWithBatch(dbPath, tableName string, data []map[string]interface{}, syncBatchID string) (int, []string, error) {
	if len(data) == 0 {
		return 0, nil, nil
	}

	// 打开数据库
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// 从第一条数据获取字段列表
	var fields []string
	for key := range data[0] {
		fields = append(fields, key)
	}
	// 添加 sync_batch_id 字段
	fields = append(fields, "sync_batch_id")

	// 构建 INSERT 语句
	placeholders := make([]string, len(fields))
	quotedFields := make([]string, len(fields))
	for i, f := range fields {
		placeholders[i] = "?"
		quotedFields[i] = fmt.Sprintf("\"%s\"", f)
	}

	query := fmt.Sprintf("INSERT OR REPLACE INTO \"%s\" (%s) VALUES (%s)",
		tableName,
		strings.Join(quotedFields, ", "),
		strings.Join(placeholders, ", "))

	// 批量插入
	tx, err := db.Begin()
	if err != nil {
		return 0, fields, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, fields, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	count := 0
	for _, row := range data {
		values := make([]interface{}, len(fields))
		for i, f := range fields {
			if f == "sync_batch_id" {
				values[i] = syncBatchID
			} else {
				values[i] = row[f]
			}
		}

		if _, err := stmt.Exec(values...); err != nil {
			log.Printf("⚠️ [saveAPIDataWithBatch] 插入失败: %v", err)
			continue
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return 0, fields, fmt.Errorf("failed to commit: %w", err)
	}

	return count, fields, nil
}
