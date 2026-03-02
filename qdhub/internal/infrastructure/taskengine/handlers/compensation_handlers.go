// Package handlers provides Task Engine task handlers for QDHub workflows.
package handlers

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/taskengine/jobs"
)

// ==================== SAGA 补偿函数 ====================
// 补偿函数用于在事务失败时执行回滚操作
// 它们是 Task Handler，通过 WithCompensationFunction 关联到 Task

// extractDataSourceID 从 TaskContext 中提取 data_source_id
// 尝试多种方式查找：直接参数 -> _result_data -> _cached_ 上游结果 -> 通过 data_source_name 查找
func extractDataSourceID(tc *task.TaskContext) string {
	// 1. 直接从参数获取
	dataSourceID := tc.GetParamString("data_source_id")
	if dataSourceID != "" {
		return dataSourceID
	}

	// 2. 从 _result_data 获取（任务执行结果）
	if result := tc.GetParam("_result_data"); result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if id, ok := resultMap["data_source_id"].(string); ok && id != "" {
				return id
			}
		}
	}

	// 3. 从 _cached_ 上游任务结果获取
	for key, val := range tc.Params {
		if !strings.HasPrefix(key, "_cached_") {
			continue
		}
		if resultMap, ok := val.(map[string]interface{}); ok {
			if id, ok := resultMap["data_source_id"].(string); ok && id != "" {
				logrus.Debugf("[extractDataSourceID] 从 %s 获取到 data_source_id=%s", key, id)
				return id
			}
		}
	}

	// 4. 尝试通过 data_source_name 从 Registry 查找
	dataSourceName := tc.GetParamString("data_source_name")
	if dataSourceName == "" {
		// 也尝试从 _cached_ 获取 name
		for key, val := range tc.Params {
			if !strings.HasPrefix(key, "_cached_") {
				continue
			}
			if resultMap, ok := val.(map[string]interface{}); ok {
				if name, ok := resultMap["data_source_name"].(string); ok && name != "" {
					dataSourceName = name
					break
				}
			}
		}
	}

	if dataSourceName != "" {
		// 尝试从 DataSourceRegistry 获取适配器信息
		if registryInterface, ok := tc.GetDependency("DataSourceRegistry"); ok {
			if registry, ok := registryInterface.(*datasource.Registry); ok {
				// 获取适配器以验证数据源存在
				if _, err := registry.GetAdapter(dataSourceName); err == nil {
					logrus.Debugf("[extractDataSourceID] 通过 data_source_name=%s 找到数据源，但无法获取 ID", dataSourceName)
					// 注意：Registry 只存储适配器，不存储 ID 映射
					// 这里需要通过 MetadataRepo 查找
					if repoInterface, ok := tc.GetDependency("MetadataRepo"); ok {
						if repo, ok := repoInterface.(metadata.Repository); ok {
							// 尝试获取数据源（需要 Repository 支持按名称查找）
							ctx := context.Background()
							if ds, err := repo.GetDataSourceByName(ctx, dataSourceName); err == nil && ds != nil {
								logrus.Debugf("[extractDataSourceID] 通过 data_source_name=%s 查找到 ID=%s", dataSourceName, ds.ID)
								return ds.ID.String()
							}
						}
					}
				}
			}
		}
	}

	return ""
}

// CompensateSaveCategoriesHandler 回滚分类保存操作
// 当 SaveCategories 任务成功但后续任务失败时，删除已保存的分类
func CompensateSaveCategoriesHandler(tc *task.TaskContext) {
	logrus.Printf("[Compensate] 🔄 开始回滚 SaveCategories - TaskID: %s", tc.TaskID)

	dataSourceID := extractDataSourceID(tc)
	if dataSourceID == "" {
		logrus.Printf("[Compensate] ⚠️ data_source_id 未找到，无法回滚。可用参数: %v", getCompensationParamKeys(tc.Params))
		return
	}

	// 获取 MetadataRepo
	repoInterface, ok := tc.GetDependency("MetadataRepo")
	if !ok {
		logrus.Printf("[Compensate] ⚠️ MetadataRepo 依赖未找到，无法回滚")
		return
	}
	repo := repoInterface.(metadata.Repository)

	// 删除该数据源的所有分类
	ctx := context.Background()
	if err := repo.DeleteCategoriesByDataSource(ctx, shared.ID(dataSourceID)); err != nil {
		logrus.Printf("[Compensate] ❌ 回滚分类失败: %v", err)
		return
	}

	logrus.Printf("[Compensate] ✅ SaveCategories 回滚成功 - DataSourceID: %s", dataSourceID)
}

// CompensateSaveAPIMetadataHandler 回滚 API 元数据保存操作
// 当 SaveAPIMetadata 任务成功但后续任务失败时，删除已保存的元数据
func CompensateSaveAPIMetadataHandler(tc *task.TaskContext) {
	logrus.Printf("[Compensate] 🔄 开始回滚 SaveAPIMetadata - TaskID: %s", tc.TaskID)

	// 尝试从结果中获取 API ID
	apiID := tc.GetParamString("api_id")
	apiName := ""
	if apiID == "" {
		// 尝试从 _result_data 获取
		if result := tc.GetParam("_result_data"); result != nil {
			if resultMap, ok := result.(map[string]interface{}); ok {
				if id, ok := resultMap["api_id"].(string); ok {
					apiID = id
				}
				if name, ok := resultMap["api_name"].(string); ok {
					apiName = name
				}
			}
		}
	}

	if apiID == "" {
		logrus.Printf("[Compensate] ⚠️ api_id 未找到，无法回滚")
		return
	}

	// 获取 MetadataRepo
	repoInterface, ok := tc.GetDependency("MetadataRepo")
	if !ok {
		logrus.Printf("[Compensate] ⚠️ MetadataRepo 依赖未找到，无法回滚")
		return
	}
	repo := repoInterface.(metadata.Repository)

	// 删除 API 元数据
	ctx := context.Background()
	if err := repo.DeleteAPIMetadata(ctx, shared.ID(apiID)); err != nil {
		logrus.Printf("[Compensate] ❌ 回滚 API 元数据失败: %v", err)
		return
	}

	logrus.Printf("[Compensate] ✅ SaveAPIMetadata 回滚成功 - APIID: %s, APIName: %s", apiID, apiName)
}

// CompensateSaveAPIMetadataBatchHandler 回滚批量 API 元数据保存操作
// 当批量保存任务成功但后续任务失败时，删除该数据源的所有 API 元数据
func CompensateSaveAPIMetadataBatchHandler(tc *task.TaskContext) {
	logrus.Printf("[Compensate] 🔄 开始回滚 SaveAPIMetadataBatch - TaskID: %s", tc.TaskID)

	dataSourceID := extractDataSourceID(tc)
	if dataSourceID == "" {
		logrus.Printf("[Compensate] ⚠️ data_source_id 未找到，无法回滚。可用参数: %v", getCompensationParamKeys(tc.Params))
		return
	}

	// 获取 MetadataRepo
	repoInterface, ok := tc.GetDependency("MetadataRepo")
	if !ok {
		logrus.Printf("[Compensate] ⚠️ MetadataRepo 依赖未找到，无法回滚")
		return
	}
	repo := repoInterface.(metadata.Repository)

	// 删除该数据源的所有 API 元数据
	ctx := context.Background()
	if err := repo.DeleteAPIMetadataByDataSource(ctx, shared.ID(dataSourceID)); err != nil {
		logrus.Printf("[Compensate] ❌ 回滚 API 元数据失败: %v", err)
		return
	}

	logrus.Printf("[Compensate] ✅ SaveAPIMetadataBatch 回滚成功 - DataSourceID: %s", dataSourceID)
}

// CompensateCreateTableHandler 回滚建表操作
// 当 CreateTable 任务成功但后续任务失败时，删除已创建的表
func CompensateCreateTableHandler(tc *task.TaskContext) {
	logrus.Printf("[Compensate] 🔄 开始回滚 CreateTable - TaskID: %s", tc.TaskID)

	tableName := tc.GetParamString("table_name")
	targetDBPath := tc.GetParamString("target_db_path")

	// 尝试从 _result_data 获取
	if tableName == "" {
		if result := tc.GetParam("_result_data"); result != nil {
			if resultMap, ok := result.(map[string]interface{}); ok {
				if name, ok := resultMap["table_name"].(string); ok {
					tableName = name
				}
				if path, ok := resultMap["target_db_path"].(string); ok && targetDBPath == "" {
					targetDBPath = path
				}
			}
		}
	}

	if tableName == "" || targetDBPath == "" {
		logrus.Printf("[Compensate] ⚠️ table_name 或 target_db_path 未找到，无法回滚")
		return
	}

	quantDB, err := jobs.GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		logrus.Printf("[Compensate] ❌ 获取 QuantDB 失败: %v", err)
		return
	}

	ctx := context.Background()
	if err := quantDB.DropTable(ctx, tableName); err != nil {
		logrus.Printf("[Compensate] ❌ 删除表失败: %v", err)
		return
	}

	logrus.Printf("[Compensate] ✅ CreateTable 回滚成功 - Table: %s", tableName)
}

// CompensateSyncDataHandler 回滚数据同步操作
// 当 SyncData 任务成功但后续任务失败时，删除已同步的数据
func CompensateSyncDataHandler(tc *task.TaskContext) {
	logrus.Printf("[Compensate] 🔄 开始回滚 SyncData - TaskID: %s", tc.TaskID)

	apiName := tc.GetParamString("api_name")
	targetDBPath := tc.GetParamString("target_db_path")
	syncBatchID := tc.GetParamString("sync_batch_id")

	// 尝试从 _result_data 获取
	if apiName == "" || syncBatchID == "" {
		if result := tc.GetParam("_result_data"); result != nil {
			if resultMap, ok := result.(map[string]interface{}); ok {
				if name, ok := resultMap["api_name"].(string); ok && apiName == "" {
					apiName = name
				}
				if batchID, ok := resultMap["sync_batch_id"].(string); ok && syncBatchID == "" {
					syncBatchID = batchID
				}
			}
		}
	}

	// 使用 WorkflowInstanceID 作为默认批次 ID
	if syncBatchID == "" {
		syncBatchID = tc.WorkflowInstanceID
	}

	if apiName == "" || targetDBPath == "" {
		logrus.Printf("[Compensate] ⚠️ api_name 或 target_db_path 未找到，无法回滚")
		return
	}

	quantDB, err := jobs.GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		logrus.Printf("[Compensate] ❌ 获取 QuantDB 失败: %v", err)
		return
	}

	ctx := context.Background()
	affected, err := quantDB.DeleteBySyncBatchID(ctx, apiName, syncBatchID)
	if err != nil {
		logrus.Printf("[Compensate] ⚠️ 无法按批次删除: %v", err)
		logrus.Printf("[Compensate] 📝 需要手动回滚 - Table: %s, BatchID: %s", apiName, syncBatchID)
		return
	}

	logrus.Printf("[Compensate] ✅ SyncData 回滚成功 - Table: %s, 删除记录数: %d", apiName, affected)
}

// CompensateUpdateCheckpointHandler 回滚检查点更新操作
// 当 UpdateSyncCheckpoint 任务成功但后续任务失败时，恢复旧的检查点
func CompensateUpdateCheckpointHandler(tc *task.TaskContext) {
	logrus.Printf("[Compensate] 🔄 开始回滚 UpdateSyncCheckpoint - TaskID: %s", tc.TaskID)

	targetDBPath := tc.GetParamString("target_db_path")
	checkpointTable := tc.GetParamString("checkpoint_table")

	// 尝试从 _result_data 获取旧检查点
	var oldCheckpoints map[string]interface{}
	if result := tc.GetParam("_result_data"); result != nil {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if old, ok := resultMap["old_checkpoints"].(map[string]interface{}); ok {
				oldCheckpoints = old
			}
			if path, ok := resultMap["target_db_path"].(string); ok && targetDBPath == "" {
				targetDBPath = path
			}
		}
	}

	if targetDBPath == "" || checkpointTable == "" {
		logrus.Printf("[Compensate] ⚠️ target_db_path 或 checkpoint_table 未找到，无法回滚")
		return
	}

	if len(oldCheckpoints) == 0 {
		logrus.Printf("[Compensate] 📝 没有旧的检查点需要恢复")
		return
	}

	quantDB, err := jobs.GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		logrus.Printf("[Compensate] ❌ 获取 QuantDB 失败: %v", err)
		return
	}

	ctx := context.Background()
	for apiName, oldDate := range oldCheckpoints {
		dateStr, ok := oldDate.(string)
		if !ok {
			continue
		}

		updateSQL := `UPDATE "` + checkpointTable + `" SET last_sync_date = ? WHERE api_name = ?`
		if _, err := quantDB.Execute(ctx, updateSQL, dateStr, apiName); err != nil {
			logrus.Printf("[Compensate] ⚠️ 恢复检查点失败: %s, error=%v", apiName, err)
			continue
		}
		logrus.Printf("[Compensate] ✅ 检查点已恢复: %s -> %s", apiName, dateStr)
	}

	logrus.Printf("[Compensate] ✅ UpdateSyncCheckpoint 回滚完成")
}

// ==================== 通用补偿 Handlers ====================

// CompensateGenericHandler 通用补偿处理器
// 用于记录补偿操作日志，当没有特定补偿逻辑时使用
func CompensateGenericHandler(tc *task.TaskContext) {
	logrus.Printf("[Compensate] 🔄 通用补偿处理 - Task: %s, TaskID: %s, WorkflowInstanceID: %s",
		tc.TaskName, tc.TaskID, tc.WorkflowInstanceID)

	// 记录原始任务参数，便于手动回滚
	logrus.Printf("[Compensate] 📝 任务参数 keys: %v", getCompensationParamKeys(tc.Params))
}

// getCompensationParamKeys 获取参数的 key 列表（用于日志）
func getCompensationParamKeys(params map[string]interface{}) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}
