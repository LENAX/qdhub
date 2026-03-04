// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/types"

	"qdhub/internal/domain/sync"
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
//   - upstream_params: map[string]UpstreamParamConfig - 上游参数映射配置（可选）
//     格式: {"param_name": {"task_name": "TaskA", "field": "field_name", "extracted_field": "cal_dates"}}
//   - task_name: 上游任务名称
//   - field: 上游结果中的字段名（直接字段）
//   - extracted_field: 上游结果 extracted_data 中的字段名（用于获取 cal_dates, ts_codes 等列表）
//   - common_data_apis: []string - 可选，公共数据 API 名列表；当 api_name 在此列表中时走 Cache→DuckDB→API
//
// Output:
//   - count: int - 保存的记录数
//   - api_name: string - API 名称
//   - fields: []string - 返回的字段列表
//   - sync_batch_id: string - 同步批次 ID
func SyncAPIDataJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()

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
		err := fmt.Errorf("data_source_name and api_name are required")
		logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
		return nil, err
	}

	logrus.Printf("📡 [SyncAPIData] 开始同步: %s/%s, BatchID=%s", dataSourceName, apiName, syncBatchID)

	// 获取查询参数（可以从上游任务注入，也可以直接传入）
	var params map[string]interface{}
	paramsRaw := tc.GetParam("params")
	log.Printf("🔍 [SyncAPIData] API=%s, paramsRaw type=%T, value=%v", apiName, paramsRaw, paramsRaw)
	if paramsRaw != nil {
		switch p := paramsRaw.(type) {
		case map[string]interface{}:
			params = p
			log.Printf("🔍 [SyncAPIData] API=%s, parsed params (map)=%v", apiName, params)
		case string:
			if err := json.Unmarshal([]byte(p), &params); err != nil {
				log.Printf("⚠️ [SyncAPIData] API=%s, params is string but not JSON: %s, trying to parse Go map format", apiName, p)
				params = parseGoMapString(p)
			}
			log.Printf("🔍 [SyncAPIData] API=%s, parsed params (from string)=%v", apiName, params)
		default:
			log.Printf("⚠️ [SyncAPIData] API=%s, params type assertion failed, got %T", apiName, paramsRaw)
		}
	}
	if params == nil {
		params = make(map[string]interface{})
	}

	// 处理上游参数映射
	upstreamParams := resolveUpstreamParams(tc)
	const maxParamLogLen = 200
	for paramName, paramValue := range upstreamParams {
		if _, exists := params[paramName]; !exists {
			params[paramName] = paramValue
			logVal := fmt.Sprintf("%v", paramValue)
			if len(logVal) > maxParamLogLen {
				logVal = logVal[:maxParamLogLen] + "..."
			}
			logrus.Printf("📥 [SyncAPIData] 从上游任务获取参数: %s=%s", paramName, logVal)
		}
	}

	commonDataAPIs := getCommonDataAPIsFromParams(tc)
	isCommonData := false
	for _, n := range commonDataAPIs {
		if n == apiName {
			isCommonData = true
			break
		}
	}

	// 公共数据：Cache → DuckDB → API
	if isCommonData {
		if cacheInterface, ok := tc.GetDependency("CommonDataCache"); ok && cacheInterface != nil {
			if cache, ok := cacheInterface.(sync.CommonDataCache); ok {
				cacheKey := commonDataCacheKey(dataSourceName, apiName, params)
				if cached, hit := cache.Get(ctx, cacheKey); hit {
					logrus.Printf("📦 [SyncAPIData] 缓存命中: %s/%s", dataSourceName, apiName)
					return buildSyncResultFromData(cached, apiName, syncBatchID), nil
				}
				// DuckDB 已有表则直接读并回填缓存
				if targetDBPath != "" && isSafeTableName(apiName) {
					quantDB, err := GetQuantDBForPath(tc, targetDBPath)
					if err == nil {
						exists, _ := quantDB.TableExists(ctx, apiName)
						if exists {
							// 使用已存在的表名安全拼接 SQL（apiName 已校验为安全）
							sqlSelect := fmt.Sprintf(`SELECT * FROM "%s"`, strings.ReplaceAll(apiName, `"`, `""`))
							rows, err := quantDB.Query(ctx, sqlSelect)
							if err == nil && len(rows) > 0 {
								_ = cache.Set(ctx, cacheKey, rows, 24*time.Hour)
								logrus.Printf("📦 [SyncAPIData] DuckDB 命中并回填缓存: %s/%s, 记录数=%d", dataSourceName, apiName, len(rows))
								return buildSyncResultFromData(rows, apiName, syncBatchID), nil
							}
						}
					}
				}
			}
		}
	}

	// 获取 DataSourceRegistry 并调用 API
	registryInterface, ok := tc.GetDependency("DataSourceRegistry")
	if !ok {
		err := fmt.Errorf("DataSourceRegistry dependency not found")
		logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
		return nil, err
	}
	registry := registryInterface.(*datasource.Registry)

	client, err := registry.GetClient(dataSourceName)
	if err != nil {
		logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
		return nil, fmt.Errorf("failed to get client: %w", err)
	}

	if token != "" {
		client.SetToken(token)
	}

	// stk_mins：补全默认 freq=1min，并将 start_date/end_date 规范化为 yyyy-mm-dd HH:MM:SS
	if apiName == "stk_mins" {
		if _, ok := params["freq"]; !ok || params["freq"] == nil || params["freq"] == "" {
			params["freq"] = "1min"
			logrus.Printf("📡 [SyncAPIData] stk_mins 使用默认 freq=1min")
		}
		if sd, ok := params["start_date"].(string); ok && sd != "" {
			params["start_date"] = normalizeDateTimeToStkMinsFormat(sd, "09:30:00")
		}
		if ed, ok := params["end_date"].(string); ok && ed != "" {
			params["end_date"] = normalizeDateTimeToStkMinsFormat(ed, "15:00:00")
		}
	}

	result, err := client.Query(ctx, apiName, params)
	if err != nil {
		logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
		return nil, fmt.Errorf("failed to query %s: %w", apiName, err)
	}

	logrus.Printf("✅ [SyncAPIData] 获取数据: %s, 记录数=%d", apiName, len(result.Data))

	var savedCount int64
	var fields []string
	if targetDBPath != "" && len(result.Data) > 0 {
		quantDB, err := GetQuantDBForPath(tc, targetDBPath)
		if err != nil {
			logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
			return nil, fmt.Errorf("get QuantDB for target_db_path: %w", err)
		}

		exists, err := quantDB.TableExists(ctx, apiName)
		if err != nil {
			logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
			return nil, fmt.Errorf("check table existence for %s: %w", apiName, err)
		}
		if !exists {
			err := fmt.Errorf("table %q does not exist, please run create_tables workflow first", apiName)
			logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
			return nil, err
		}

		savedCount, err = quantDB.BulkInsertWithBatchID(ctx, apiName, result.Data, syncBatchID)
		if err != nil {
			logrus.Errorf("[SyncAPIData] task failed: taskID=%s, api=%s/%s, err=%v", tc.TaskID, dataSourceName, apiName, err)
			return nil, fmt.Errorf("failed to save data: %w", err)
		}

		if len(result.Data) > 0 {
			for key := range result.Data[0] {
				fields = append(fields, key)
			}
		}

		if apiName != "trade_cal" {
			logrus.Printf("💾 [SyncAPIData] 保存数据: %s, 保存记录数=%d", apiName, savedCount)
		}
	}

	// 公共数据回填缓存
	if isCommonData {
		if cacheInterface, ok := tc.GetDependency("CommonDataCache"); ok && cacheInterface != nil {
			if cache, ok := cacheInterface.(sync.CommonDataCache); ok {
				cacheKey := commonDataCacheKey(dataSourceName, apiName, params)
				dataAny := make([]map[string]any, len(result.Data))
				for i, m := range result.Data {
					row := make(map[string]any)
					for k, v := range m {
						row[k] = v
					}
					dataAny[i] = row
				}
				_ = cache.Set(ctx, cacheKey, dataAny, 24*time.Hour)
			}
		}
	}

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

// getCommonDataAPIsFromParams 从任务参数中解析 common_data_apis（公共数据 API 名列表）。
func getCommonDataAPIsFromParams(tc *task.TaskContext) []string {
	raw := tc.GetParam("common_data_apis")
	if raw == nil {
		return nil
	}
	return convertToStringSlice(raw)
}

// commonDataCacheKey 生成公共数据缓存 key：数据源:API:paramsHash。
func commonDataCacheKey(dataSourceName, apiName string, params map[string]interface{}) string {
	h := sha256.New()
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(fmt.Sprint(params[k])))
	}
	return fmt.Sprintf("%s:%s:%s", dataSourceName, apiName, hex.EncodeToString(h.Sum(nil)))
}

var safeTableNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// isSafeTableName 校验表名仅含字母、数字、下划线，防止 SQL 注入。
func isSafeTableName(name string) bool {
	return safeTableNameRe.MatchString(name)
}

// buildSyncResultFromData 用已有数据构造与 SyncAPIDataJob 一致的返回结构（cache/DuckDB 命中时使用）。
func buildSyncResultFromData(data []map[string]any, apiName, syncBatchID string) map[string]interface{} {
	var fields []string
	if len(data) > 0 {
		for k := range data[0] {
			fields = append(fields, k)
		}
	}
	// extractKeyFields 需要 []map[string]interface{}
	dataIf := make([]map[string]interface{}, len(data))
	for i, m := range data {
		row := make(map[string]interface{})
		for k, v := range m {
			row[k] = v
		}
		dataIf[i] = row
	}
	extracted := extractKeyFields(dataIf, []string{"ts_code", "trade_date", "cal_date"})
	return map[string]interface{}{
		"count":          int64(len(data)),
		"total":          len(data),
		"api_name":       apiName,
		"fields":         fields,
		"has_more":       false,
		"extracted_data": extracted,
		"sync_batch_id":  syncBatchID,
	}
}

// GenerateDataSyncSubTasksJob 生成数据同步子任务（模板任务 Job Function）
// 根据上游任务结果（如 ts_codes 列表）为每个项目生成同步子任务
//
// Input params:
//   - data_source_name: string - 数据源名称
//   - api_name: string - 要调用的 API 名称
//   - param_key: string - 参数键名（如 "ts_code"）
//   - upstream_task: string - 上游任务名称（可选，用于明确指定从哪个任务获取参数列表）
//   - token: string - API Token
//   - target_db_path: string - 目标数据库路径
//   - max_sub_tasks: int - 最大子任务数量（0=不限制）
//   - start_date: string - 日期范围开始（YYYYMMDD），用于过滤交易日
//   - end_date: string - 日期范围结束（YYYYMMDD），用于过滤交易日
//
// Output:
//   - status: string - 操作状态
//   - generated: int - 生成的子任务数量
func GenerateDataSyncSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	logrus.Printf("📋 [GenerateDataSyncSubTasks] Job Function 执行, Params: %v", getParamKeys(tc.Params))

	// 获取参数
	dataSourceName := tc.GetParamString("data_source_name")
	apiName := tc.GetParamString("api_name")
	paramKey := tc.GetParamString("param_key")
	upstreamTask := tc.GetParamString("upstream_task") // 新增：明确指定上游任务
	token := tc.GetParamString("token")
	targetDBPath := tc.GetParamString("target_db_path")
	maxSubTasks, _ := tc.GetParamInt("max_sub_tasks")

	// 获取 Engine（仅用于 taskRegistry）
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("Engine dependency not found")
	}
	eng := engineInterface.(*engine.Engine)
	taskRegistry := eng.GetRegistry()

	// 获取 InstanceManager（用于一次性 AtomicAddSubTasks）
	type instanceManagerWithAtomic interface {
		AtomicAddSubTasks(subTasks []types.Task, parentTaskID string) error
	}
	managerInterface := tc.GetInstanceManager()
	if managerInterface == nil {
		return nil, fmt.Errorf("InstanceManager not found (template task must run with InstanceManager set)")
	}
	manager, ok := managerInterface.(instanceManagerWithAtomic)
	if !ok {
		return nil, fmt.Errorf("InstanceManager does not support AtomicAddSubTasks")
	}

	// 从上游任务提取参数值列表
	var paramValues []string
	if upstreamTask != "" {
		// 使用新 API：从指定的上游任务获取
		paramValues = extractParamValuesFromSpecificUpstream(tc, upstreamTask, paramKey)
		logrus.Printf("📥 [GenerateDataSyncSubTasks] 从上游任务 %s 获取 %s 列表: %d 个", upstreamTask, paramKey, len(paramValues))
	} else {
		// 兼容旧逻辑：遍历所有上游任务
		paramValues = extractParamValuesFromUpstream(tc, paramKey)
	}

	if len(paramValues) == 0 {
		logrus.Printf("⚠️ [GenerateDataSyncSubTasks] 未找到 %s 列表", paramKey)
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"message":   fmt.Sprintf("未找到 %s 列表，跳过子任务生成", paramKey),
		}, nil
	}

	// 如果是 trade_date，根据日期范围过滤交易日
	// start_date/end_date 作为顶层参数传入
	if paramKey == "trade_date" {
		startDate := normalizeDateForFilter(tc.GetParamString("start_date"))
		endDate := normalizeDateForFilter(tc.GetParamString("end_date"))
		if startDate != "" || endDate != "" {
			filtered := filterDatesByRange(paramValues, startDate, endDate)
			logrus.Printf("📅 [GenerateDataSyncSubTasks] 日期范围过滤: 原始 %d 个交易日 -> 过滤后 %d 个 (范围: %s ~ %s)",
				len(paramValues), len(filtered), startDate, endDate)
			paramValues = filtered
		}
	}

	// 应用数量限制
	if maxSubTasks > 0 && len(paramValues) > maxSubTasks {
		logrus.Printf("📡 [GenerateDataSyncSubTasks] 限制子任务数量从 %d 到 %d", len(paramValues), maxSubTasks)
		paramValues = paramValues[:maxSubTasks]
	}

	logrus.Printf("📡 [GenerateDataSyncSubTasks] 为 %d 个 %s 生成子任务", len(paramValues), paramKey)

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID

	// 先收集所有子任务，再通过 AtomicAddSubTasks 一次性提交给 instance manager
	subTasks := make([]types.Task, 0, len(paramValues))
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

		// 仅当按 ts_code 等非日期拆分子任务时，将 start_date/end_date 传给 API（用于日期范围查询）。
		// 按 trade_date 拆分的 API（如 adj_factor、daily）每个子任务只查单日，传 start_date/end_date 会导致
		// Tushare 同时收到 trade_date 与日期范围，可能返回 0 条或行为异常，故不传。
		paramsMap := subTaskParams["params"].(map[string]interface{})
		if apiName == "stk_mins" {
			if _, ok := paramsMap["freq"]; !ok || paramsMap["freq"] == nil || paramsMap["freq"] == "" {
				paramsMap["freq"] = "1min"
			}
		}
		if paramKey != "trade_date" {
			if sd := tc.GetParamString("start_date"); sd != "" {
				if apiName == "stk_mins" {
					paramsMap["start_date"] = normalizeDateTimeToStkMinsFormat(sd, "09:30:00")
				} else {
					paramsMap["start_date"] = sd
				}
				if ed := tc.GetParamString("end_date"); ed != "" {
					if apiName == "stk_mins" {
						paramsMap["end_date"] = normalizeDateTimeToStkMinsFormat(ed, "15:00:00")
					} else {
						paramsMap["end_date"] = ed
					}
				}
			}
		}

		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("同步 %s: %s=%s", apiName, paramKey, paramValue), taskRegistry).
			WithJobFunction("SyncAPIData", subTaskParams).
			WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
			WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
			WithCompensationFunction("CompensateSyncData"). // SAGA 补偿
			Build()
		if err != nil {
			logrus.Printf("❌ [GenerateDataSyncSubTasks] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		subTasks = append(subTasks, subTask)
		subTaskInfos = append(subTaskInfos, map[string]interface{}{
			"name":      subTaskName,
			"api_name":  apiName,
			"param_key": paramKey,
			paramKey:    paramValue,
		})
	}

	if len(subTasks) == 0 {
		logrus.Printf("⚠️ [GenerateDataSyncSubTasks] 无有效子任务可提交")
		return map[string]interface{}{
			"status":    "success",
			"generated": 0,
			"api_name":  apiName,
			"param_key": paramKey,
			"sub_tasks": subTaskInfos,
		}, nil
	}

	if err := manager.AtomicAddSubTasks(subTasks, parentTaskID); err != nil {
		return nil, fmt.Errorf("AtomicAddSubTasks 失败: %w", err)
	}
	generatedCount := len(subTasks)
	logrus.Printf("✅ [GenerateDataSyncSubTasks] 共生成并一次性提交 %d 个子任务", generatedCount)

	return map[string]interface{}{
		"status":    "success",
		"generated": generatedCount,
		"api_name":  apiName,
		"param_key": paramKey,
		"sub_tasks": subTaskInfos,
	}, nil
}

// DeleteSyncedDataJob 删除同步的数据（用于回滚）
// 使用 QuantDBFactory 按 target_db_path 连接 DuckDB，与 Quant Data Store 一致。
//
// Input params:
//   - api_name: string - API 名称（表名）
//   - target_db_path: string - 目标数据库路径（DuckDB）
//   - sync_batch_id: string - 同步批次 ID
//
// Output:
//   - deleted_count: int - 删除的记录数
//   - api_name: string - API 名称
func DeleteSyncedDataJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()
	apiName := tc.GetParamString("api_name")
	targetDBPath := tc.GetParamString("target_db_path")
	syncBatchID := tc.GetParamString("sync_batch_id")

	if apiName == "" || targetDBPath == "" || syncBatchID == "" {
		return nil, fmt.Errorf("api_name, target_db_path and sync_batch_id are required")
	}

	logrus.Printf("🗑️ [DeleteSyncedData] 删除同步数据: %s, BatchID=%s", apiName, syncBatchID)

	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("get QuantDB for target_db_path: %w", err)
	}

	affected, err := quantDB.DeleteBySyncBatchID(ctx, apiName, syncBatchID)
	if err != nil {
		return nil, fmt.Errorf("failed to delete data: %w", err)
	}

	logrus.Printf("✅ [DeleteSyncedData] 删除成功: %s, 记录数=%d", apiName, affected)

	return map[string]interface{}{
		"deleted_count": affected,
		"api_name":      apiName,
		"sync_batch_id": syncBatchID,
	}, nil
}

// NotifySyncCompleteJob 无操作 job，用于 BatchSyncComplete 任务。
// 实际回调逻辑在 DataSyncCompleteHandler 中通过 SyncCallbackInvoker 执行。
func NotifySyncCompleteJob(tc *task.TaskContext) (interface{}, error) {
	return nil, nil
}

// ==================== 增量实时同步 Job Functions ====================

// GetSyncCheckpointJob 获取同步检查点
// 从检查点表中获取每个 API 的上次同步位置。使用 QuantDBFactory 按 target_db_path 连接 DuckDB。
//
// Input params:
//   - target_db_path: string - 目标数据库路径（DuckDB）
//   - checkpoint_table: string - 检查点表名
//   - api_names: []string - API 名称列表
//
// Output:
//   - checkpoints: map[string]string - API 名称到最后同步日期的映射
//   - has_checkpoint: bool - 是否存在检查点
func GetSyncCheckpointJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()
	targetDBPath := tc.GetParamString("target_db_path")
	checkpointTable := tc.GetParamString("checkpoint_table")

	if targetDBPath == "" || checkpointTable == "" {
		return nil, fmt.Errorf("target_db_path and checkpoint_table are required")
	}

	// 获取 API 名称列表
	var apiNames []string
	if raw := tc.GetParam("api_names"); raw != nil {
		apiNames = convertToStringSlice(raw)
	}

	logrus.Printf("📍 [GetSyncCheckpoint] 获取检查点: table=%s, apis=%v", checkpointTable, apiNames)

	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("get QuantDB for target_db_path: %w", err)
	}

	// 确保检查点表存在（DuckDB 兼容的 DDL）（DuckDB 兼容的 DDL）
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s" (
			api_name VARCHAR PRIMARY KEY,
			last_sync_date VARCHAR NOT NULL,
			last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			record_count INTEGER DEFAULT 0
		)
	`, checkpointTable)
	if _, err := quantDB.Execute(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	// 查询检查点
	checkpoints := make(map[string]string)
	hasCheckpoint := false

	for _, apiName := range apiNames {
		query := fmt.Sprintf(`SELECT last_sync_date FROM "%s" WHERE api_name = ?`, checkpointTable)
		rows, err := quantDB.Query(ctx, query, apiName)
		if err != nil {
			logrus.Printf("⚠️ [GetSyncCheckpoint] 查询失败: %s, error=%v", apiName, err)
			continue
		}
		if len(rows) > 0 {
			if v, ok := rows[0]["last_sync_date"]; ok {
				if s, ok := v.(string); ok {
					checkpoints[apiName] = s
					hasCheckpoint = true
					logrus.Printf("📍 [GetSyncCheckpoint] %s: 上次同步日期=%s", apiName, s)
				}
			}
		}
	}

	return map[string]interface{}{
		"checkpoints":    checkpoints,
		"has_checkpoint": hasCheckpoint,
		"api_count":      len(apiNames),
	}, nil
}

// FetchLatestTradingDateJob 获取最新交易日
// 调用 trade_cal API 获取最新的交易日期
//
// Input params:
//   - data_source_name: string - 数据源名称
//   - token: string - API Token
//   - exchange: string - 交易所代码（默认 SSE）
//
// Output:
//   - latest_trade_date: string - 最新交易日（格式: "20251201"）
//   - is_trading_day: bool - 今天是否是交易日
func FetchLatestTradingDateJob(tc *task.TaskContext) (interface{}, error) {
	dataSourceName := tc.GetParamString("data_source_name")
	token := tc.GetParamString("token")
	exchange := tc.GetParamString("exchange")

	if dataSourceName == "" {
		return nil, fmt.Errorf("data_source_name is required")
	}
	if exchange == "" {
		exchange = "SSE"
	}

	logrus.Printf("📅 [FetchLatestTradingDate] 获取最新交易日: source=%s, exchange=%s", dataSourceName, exchange)

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

	if token != "" {
		client.SetToken(token)
	}

	// 调用 trade_cal API
	ctx := context.Background()
	result, err := client.Query(ctx, "trade_cal", map[string]interface{}{
		"exchange":   exchange,
		"is_open":    1,
		"start_date": getRecentDateString(-30), // 最近30天
		"end_date":   getTodayDateString(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query trade_cal: %w", err)
	}

	if len(result.Data) == 0 {
		return nil, fmt.Errorf("no trading days found")
	}

	// 找到最新的交易日
	latestTradeDate := ""
	for _, row := range result.Data {
		if calDate, ok := row["cal_date"].(string); ok {
			if calDate > latestTradeDate {
				latestTradeDate = calDate
			}
		}
	}

	// 检查今天是否是交易日
	today := getTodayDateString()
	isTradingDay := latestTradeDate == today

	logrus.Printf("✅ [FetchLatestTradingDate] 最新交易日=%s, 今天是否交易日=%v", latestTradeDate, isTradingDay)

	return map[string]interface{}{
		"latest_trade_date": latestTradeDate,
		"is_trading_day":    isTradingDay,
		"today":             today,
	}, nil
}

// GenerateIncrementalSyncSubTasksJob 生成增量同步子任务（模板任务 Job Function）
// 根据检查点信息，为每个股票生成增量同步子任务
//
// Input params:
//   - data_source_name: string - 数据源名称
//   - api_name: string - 要调用的 API 名称
//   - param_key: string - 参数键名（如 "ts_code"）
//   - token: string - API Token
//   - target_db_path: string - 目标数据库路径
//   - checkpoint_table: string - 检查点表名
//   - max_sub_tasks: int - 最大子任务数量（0=不限制）
//
// Output:
//   - status: string - 操作状态
//   - generated: int - 生成的子任务数量
func GenerateIncrementalSyncSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	logrus.Printf("📋 [GenerateIncrementalSyncSubTasks] Job Function 执行")

	// 获取参数
	dataSourceName := tc.GetParamString("data_source_name")
	apiName := tc.GetParamString("api_name")
	paramKey := tc.GetParamString("param_key")
	token := tc.GetParamString("token")
	targetDBPath := tc.GetParamString("target_db_path")
	checkpointTable := tc.GetParamString("checkpoint_table")
	maxSubTasks, _ := tc.GetParamInt("max_sub_tasks")

	// 从上游任务获取最新交易日
	latestTradeDate := ""
	if cached := tc.GetParam("_cached_FetchLatestTradingDate"); cached != nil {
		if resultMap, ok := cached.(map[string]interface{}); ok {
			if date, ok := resultMap["latest_trade_date"].(string); ok {
				latestTradeDate = date
			}
		}
	}

	// 从上游任务获取检查点信息
	checkpoints := make(map[string]string)
	if cached := tc.GetParam("_cached_GetSyncCheckpoint"); cached != nil {
		if resultMap, ok := cached.(map[string]interface{}); ok {
			if cp, ok := resultMap["checkpoints"].(map[string]interface{}); ok {
				for k, v := range cp {
					if s, ok := v.(string); ok {
						checkpoints[k] = s
					}
				}
			}
		}
	}

	// 确定同步的开始日期
	startDate := ""
	if cp, ok := checkpoints[apiName]; ok && cp != "" {
		startDate = cp // 从检查点开始
	}

	logrus.Printf("📋 [GenerateIncrementalSyncSubTasks] api=%s, startDate=%s, endDate=%s",
		apiName, startDate, latestTradeDate)

	// 获取 Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("Engine dependency not found")
	}
	eng := engineInterface.(*engine.Engine)
	taskRegistry := eng.GetRegistry()

	// 从上游任务提取股票代码列表
	paramValues := extractParamValuesFromUpstream(tc, paramKey)
	if len(paramValues) == 0 {
		logrus.Printf("⚠️ [GenerateIncrementalSyncSubTasks] 未找到 %s 列表，尝试从 stock_basic 获取", paramKey)
		// 可以尝试从其他来源获取
	}

	// 应用数量限制
	if maxSubTasks > 0 && len(paramValues) > maxSubTasks {
		logrus.Printf("📡 [GenerateIncrementalSyncSubTasks] 限制子任务数量从 %d 到 %d", len(paramValues), maxSubTasks)
		paramValues = paramValues[:maxSubTasks]
	}

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	var subTaskInfos []map[string]interface{}
	for _, paramValue := range paramValues {
		subTaskName := fmt.Sprintf("IncrSync_%s_%s", apiName, paramValue)

		// 构建子任务参数
		subTaskParams := map[string]interface{}{
			"data_source_name": dataSourceName,
			"api_name":         apiName,
			"token":            token,
			"target_db_path":   targetDBPath,
			"checkpoint_table": checkpointTable,
			"sync_batch_id":    workflowInstanceID,
			"params": map[string]interface{}{
				paramKey: paramValue,
			},
		}

		// 添加日期范围参数
		if startDate != "" {
			subTaskParams["params"].(map[string]interface{})["start_date"] = startDate
		}
		if latestTradeDate != "" {
			subTaskParams["params"].(map[string]interface{})["end_date"] = latestTradeDate
		}

		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("增量同步 %s: %s=%s", apiName, paramKey, paramValue), taskRegistry).
			WithJobFunction("SyncAPIData", subTaskParams).
			WithTaskHandler(task.TaskStatusSuccess, "DataSyncSuccess").
			WithTaskHandler(task.TaskStatusFailed, "DataSyncFailure").
			WithCompensationFunction("CompensateSyncData").
			Build()
		if err != nil {
			logrus.Printf("❌ [GenerateIncrementalSyncSubTasks] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			logrus.Printf("❌ [GenerateIncrementalSyncSubTasks] 添加子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		generatedCount++
		subTaskInfos = append(subTaskInfos, map[string]interface{}{
			"name":       subTaskName,
			"api_name":   apiName,
			"param_key":  paramKey,
			paramKey:     paramValue,
			"start_date": startDate,
			"end_date":   latestTradeDate,
		})
		logrus.Printf("✅ [GenerateIncrementalSyncSubTasks] 子任务已添加: %s", subTaskName)
	}

	logrus.Printf("✅ [GenerateIncrementalSyncSubTasks] 共生成 %d 个子任务", generatedCount)

	return map[string]interface{}{
		"status":            "success",
		"generated":         generatedCount,
		"api_name":          apiName,
		"param_key":         paramKey,
		"start_date":        startDate,
		"end_date":          latestTradeDate,
		"sub_tasks":         subTaskInfos,
		"workflow_instance": workflowInstanceID,
	}, nil
}

// UpdateSyncCheckpointJob 更新同步检查点
// 同步完成后更新检查点表中的最后同步日期。使用 QuantDBFactory 按 target_db_path 连接 DuckDB。
//
// Input params:
//   - target_db_path: string - 目标数据库路径（DuckDB）
//   - checkpoint_table: string - 检查点表名
//   - api_names: []string - API 名称列表
//
// Output:
//   - updated: int - 更新的检查点数量
//   - checkpoints: map[string]string - 更新后的检查点
func UpdateSyncCheckpointJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()
	targetDBPath := tc.GetParamString("target_db_path")
	checkpointTable := tc.GetParamString("checkpoint_table")

	if targetDBPath == "" || checkpointTable == "" {
		return nil, fmt.Errorf("target_db_path and checkpoint_table are required")
	}

	// 获取 API 名称列表
	var apiNames []string
	if raw := tc.GetParam("api_names"); raw != nil {
		apiNames = convertToStringSlice(raw)
	}

	// 从上游任务获取最新交易日
	latestTradeDate := ""
	if cached := tc.GetParam("_cached_FetchLatestTradingDate"); cached != nil {
		if resultMap, ok := cached.(map[string]interface{}); ok {
			if date, ok := resultMap["latest_trade_date"].(string); ok {
				latestTradeDate = date
			}
		}
	}

	if latestTradeDate == "" {
		latestTradeDate = getTodayDateString()
	}

	logrus.Printf("📝 [UpdateSyncCheckpoint] 更新检查点: table=%s, date=%s, apis=%v",
		checkpointTable, latestTradeDate, apiNames)

	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("get QuantDB for target_db_path: %w", err)
	}

	// 保存旧的检查点（用于补偿）
	oldCheckpoints := make(map[string]string)
	for _, apiName := range apiNames {
		query := fmt.Sprintf(`SELECT last_sync_date FROM "%s" WHERE api_name = ?`, checkpointTable)
		rows, err := quantDB.Query(ctx, query, apiName)
		if err == nil && len(rows) > 0 {
			if v, ok := rows[0]["last_sync_date"]; ok {
				if s, ok := v.(string); ok {
					oldCheckpoints[apiName] = s
				}
			}
		}
	}

	// 更新检查点（DuckDB 兼容的 UPSERT）
	updatedCount := 0
	newCheckpoints := make(map[string]string)

	upsertSQL := fmt.Sprintf(`
		INSERT INTO "%s" (api_name, last_sync_date, last_sync_time, record_count)
		VALUES (?, ?, CURRENT_TIMESTAMP, 0)
		ON CONFLICT(api_name) DO UPDATE SET
			last_sync_date = excluded.last_sync_date,
			last_sync_time = CURRENT_TIMESTAMP
	`, checkpointTable)

	for _, apiName := range apiNames {
		if _, err := quantDB.Execute(ctx, upsertSQL, apiName, latestTradeDate); err != nil {
			logrus.Printf("⚠️ [UpdateSyncCheckpoint] 更新失败: %s, error=%v", apiName, err)
			continue
		}
		updatedCount++
		newCheckpoints[apiName] = latestTradeDate
		logrus.Printf("✅ [UpdateSyncCheckpoint] %s: %s -> %s",
			apiName, oldCheckpoints[apiName], latestTradeDate)
	}

	return map[string]interface{}{
		"updated":         updatedCount,
		"checkpoints":     newCheckpoints,
		"old_checkpoints": oldCheckpoints,
		"sync_date":       latestTradeDate,
	}, nil
}

// ==================== 辅助函数 ====================

// UpstreamParamConfig 上游参数配置
// 用于配置如何从上游任务结果中获取参数值
type UpstreamParamConfig struct {
	TaskName       string // 上游任务名称
	Field          string // 上游结果中的直接字段名
	ExtractedField string // extracted_data 中的字段名（如 "cal_dates", "ts_codes"）
	Select         string // 选择策略: "first" | "last" | "all"，默认 "last"
}

// resolveUpstreamParams 解析上游参数映射配置，从上游任务结果中获取参数值
// upstream_params 配置格式:
//
//	{
//	  "trade_date": {"task_name": "FetchTradeCal", "extracted_field": "cal_dates", "select": "last"},
//	  "ts_code": {"task_name": "FetchStockBasic", "extracted_field": "ts_codes", "select": "first"}
//	}
func resolveUpstreamParams(tc *task.TaskContext) map[string]interface{} {
	result := make(map[string]interface{})

	upstreamParamsRaw := tc.GetParam("upstream_params")
	if upstreamParamsRaw == nil {
		return result
	}

	upstreamParams, ok := upstreamParamsRaw.(map[string]interface{})
	if !ok {
		logrus.Printf("⚠️ [resolveUpstreamParams] upstream_params 格式错误，期望 map[string]interface{}")
		return result
	}

	for paramName, configRaw := range upstreamParams {
		config, ok := configRaw.(map[string]interface{})
		if !ok {
			logrus.Printf("⚠️ [resolveUpstreamParams] 参数 %s 配置格式错误", paramName)
			continue
		}

		// 解析配置
		taskName, _ := config["task_name"].(string)
		field, _ := config["field"].(string)
		extractedField, _ := config["extracted_field"].(string)
		selectStrategy, _ := config["select"].(string)
		if selectStrategy == "" {
			selectStrategy = "last" // 默认取最后一个（最新值）
		}

		if taskName == "" {
			logrus.Printf("⚠️ [resolveUpstreamParams] 参数 %s 缺少 task_name", paramName)
			continue
		}

		// 使用新 API 获取上游任务结果
		upstreamResult := tc.GetUpstreamResult(taskName)
		if upstreamResult == nil {
			logrus.Printf("⚠️ [resolveUpstreamParams] 未找到上游任务 %s 的结果", taskName)
			continue
		}

		var value interface{}

		// 优先从 extracted_data 获取
		if extractedField != "" {
			if extracted, ok := upstreamResult["extracted_data"].(map[string]interface{}); ok {
				if vals, ok := extracted[extractedField]; ok {
					value = selectFromSlice(vals, selectStrategy)
				}
			}
		}

		// 如果 extracted_data 没有，尝试直接获取字段
		if value == nil && field != "" {
			if fieldVal, ok := upstreamResult[field]; ok {
				value = selectFromSlice(fieldVal, selectStrategy)
			}
		}

		// 如果还没有，尝试从 extracted_data 的复数形式获取
		if value == nil && extractedField == "" && field != "" {
			pluralField := field + "s"
			if extracted, ok := upstreamResult["extracted_data"].(map[string]interface{}); ok {
				if vals, ok := extracted[pluralField]; ok {
					value = selectFromSlice(vals, selectStrategy)
				}
			}
		}

		if value != nil {
			result[paramName] = value
			logrus.Printf("✅ [resolveUpstreamParams] 解析参数 %s=%v (from %s)", paramName, value, taskName)
		} else {
			logrus.Printf("⚠️ [resolveUpstreamParams] 无法从任务 %s 获取参数 %s", taskName, paramName)
		}
	}

	return result
}

// selectFromSlice 根据策略从切片中选择值
// "first" - 返回第一个值
// "last" - 返回最后一个值
// "all" - 返回整个切片
func selectFromSlice(val interface{}, strategy string) interface{} {
	// 如果不是切片类型，直接返回
	switch v := val.(type) {
	case []string:
		if len(v) == 0 {
			return nil
		}
		switch strategy {
		case "first":
			return v[0]
		case "all":
			return v
		default: // "last"
			return v[len(v)-1]
		}
	case []interface{}:
		if len(v) == 0 {
			return nil
		}
		switch strategy {
		case "first":
			return v[0]
		case "all":
			return v
		default: // "last"
			return v[len(v)-1]
		}
	default:
		// 非切片类型，直接返回
		return val
	}
}

// getTodayDateString 获取今天的日期字符串（格式: "20251201"）
func getTodayDateString() string {
	return getRecentDateString(0)
}

// getRecentDateString 获取相对于今天的日期字符串
// offset: 天数偏移量，正数为未来，负数为过去
func getRecentDateString(offset int) string {
	now := time.Now()
	target := now.AddDate(0, 0, offset)
	return target.Format("20060102")
}

// getParamKeys 获取参数的所有 key（调试用）
func getParamKeys(params map[string]interface{}) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}

// extractParamValuesFromSpecificUpstream 从指定上游任务结果中提取参数值列表（使用新 API）
func extractParamValuesFromSpecificUpstream(tc *task.TaskContext, taskName, paramKey string) []string {
	upstreamResult := tc.GetUpstreamResult(taskName)
	if upstreamResult == nil {
		return nil
	}

	// 特殊处理：trade_date 从 FetchTradeCal 任务中提取时，使用 cal_dates 字段
	if paramKey == "trade_date" && taskName == "FetchTradeCal" {
		if extracted, ok := upstreamResult["extracted_data"].(map[string]interface{}); ok {
			if vals, ok := extracted["cal_dates"]; ok {
				return convertToStringSlice(vals)
			}
		}
		// 也尝试直接字段
		if vals, ok := upstreamResult["cal_dates"]; ok {
			return convertToStringSlice(vals)
		}
	}

	// 优先从 extracted_data 获取（复数形式）
	pluralKey := paramKey + "s"
	if extracted, ok := upstreamResult["extracted_data"].(map[string]interface{}); ok {
		if vals, ok := extracted[pluralKey]; ok {
			return convertToStringSlice(vals)
		}
		if vals, ok := extracted[paramKey]; ok {
			return convertToStringSlice(vals)
		}
	}

	// 直接检查字段
	if vals, ok := upstreamResult[pluralKey]; ok {
		return convertToStringSlice(vals)
	}
	if vals, ok := upstreamResult[paramKey]; ok {
		return convertToStringSlice(vals)
	}

	return nil
}

// extractParamValuesFromUpstream 从上游任务结果中提取参数值列表（遍历所有上游任务）
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

// parseGoMapString 解析 Go fmt.Sprintf 格式的 map 字符串
// 例如: "map[trade_date:20260121]" -> map[string]interface{}{"trade_date": "20260121"}
// 例如: "map[end_date:20260121 start_date:20260114]" -> map[string]interface{}{"end_date": "20260121", "start_date": "20260114"}
func parseGoMapString(s string) map[string]interface{} {
	result := make(map[string]interface{})

	// 移除 "map[" 前缀和 "]" 后缀
	s = strings.TrimPrefix(s, "map[")
	s = strings.TrimSuffix(s, "]")

	if s == "" {
		return result
	}

	// 按空格分割键值对
	pairs := strings.Fields(s)
	for _, pair := range pairs {
		// 按第一个 ":" 分割键和值
		idx := strings.Index(pair, ":")
		if idx > 0 {
			key := pair[:idx]
			value := pair[idx+1:]
			result[key] = value
		}
	}

	return result
}

// dateLayoutsForFilter 支持的日期格式，用于 normalizeDateForFilter 解析（按长度从长到短，避免短格式误匹配）
var dateLayoutsForFilter = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02",
	"2006/01/02",
	"20060102",
	"2006-01", // 仅年月，视为当月第一天
}

// normalizeDateForFilter 将日期串解析为 date 再格式化为 YYYYMMDD，用于与 trade_cal 的日期比较。
// 不假设输入格式，按 dateLayoutsForFilter 依次尝试解析，解析失败返回空字符串。
func normalizeDateForFilter(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	for _, layout := range dateLayoutsForFilter {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format("20060102")
		}
	}
	return ""
}

// dateTimeLayoutsForStkMins 供 stk_mins 使用的日期时间解析格式（输出为 yyyy-mm-dd HH:MM:SS）
var dateTimeLayoutsForStkMins = []string{
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
	"2006/01/02 15:04:05",
	"2006/01/02",
	"20060102 150405",
	"20060102",
}

// normalizeDateTimeToStkMinsFormat 将日期/日期时间规范化为 "yyyy-mm-dd HH:MM:SS"，供 stk_mins 等 API 使用。
// defaultTime 在仅解析出日期时使用，如 "09:30:00"（start_date）、"15:00:00"（end_date）。
func normalizeDateTimeToStkMinsFormat(s, defaultTime string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	for _, layout := range dateTimeLayoutsForStkMins {
		if t, err := time.Parse(layout, s); err == nil {
			if strings.Contains(layout, "15:04") || strings.Contains(layout, "150405") {
				return t.Format("2006-01-02 15:04:05")
			}
			return t.Format("2006-01-02") + " " + defaultTime
		}
	}
	return ""
}

// filterDatesByRange 根据日期范围过滤日期列表
// dates: 日期列表（格式: "20260122"）
// startDate: 开始日期（格式: "20260115"），为空则不限制开始
// endDate: 结束日期（格式: "20260122"），为空则不限制结束
func filterDatesByRange(dates []string, startDate, endDate string) []string {
	if len(dates) == 0 {
		return dates
	}

	// 如果开始和结束日期都为空，返回所有日期
	if startDate == "" && endDate == "" {
		return dates
	}

	filtered := make([]string, 0, len(dates))
	for _, date := range dates {
		// 检查是否在范围内
		if startDate != "" && date < startDate {
			continue
		}
		if endDate != "" && date > endDate {
			continue
		}
		filtered = append(filtered, date)
	}

	return filtered
}
