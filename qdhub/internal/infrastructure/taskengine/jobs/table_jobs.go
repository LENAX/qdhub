// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// ==================== 建表 Job Functions ====================

// CreateTableFromMetadataJob 根据 API Metadata 动态创建数据表
//
// 该 Job 使用 QuantDB Adapter 创建表，支持 DuckDB、ClickHouse 等多种数据库。
// QuantDB Adapter 通过 Task Engine 依赖注入获取。
//
// Input params:
//   - api_name: string - API 名称/表名（必需）
//   - data_source_id: string - 数据源 ID
//   - target_db_path: string - 目标数据库路径（用于日志和回滚）
//   - fields: []FieldMeta - 字段定义（必需，从父任务传递）
//
// Output:
//   - table_name: string - 创建的表名
//   - field_count: int - 字段数量
//   - target_db_path: string - 目标数据库路径
func CreateTableFromMetadataJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()

	// 获取参数
	apiName := tc.GetParamString("api_name")
	targetDBPath := tc.GetParamString("target_db_path")

	if apiName == "" {
		return nil, fmt.Errorf("api_name is required")
	}

	logrus.Debugf("[CreateTableFromMetadata] 开始创建表: %s", apiName)

	// 获取字段定义（必须由父任务传递）
	var fields []metadata.FieldMeta
	if fieldsRaw := tc.GetParam("fields"); fieldsRaw != nil {
		fields = convertToFieldMeta(fieldsRaw)
	}

	// 如果参数中没有字段，尝试从 MetadataRepo 获取
	// 注意：需要先检查 Registry 是否存在，避免 nil pointer panic
	if len(fields) == 0 && tc.GetRegistry() != nil {
		if repoInterface, ok := tc.GetDependency("MetadataRepo"); ok {
			repo := repoInterface.(metadata.Repository)
			dataSourceID := tc.GetParamString("data_source_id")
			if dataSourceID != "" {
				apiMetadataList, err := repo.ListAPIMetadataByDataSource(ctx, shared.ID(dataSourceID))
				if err == nil {
					for _, api := range apiMetadataList {
						if api.Name == apiName {
							fields = api.ResponseFields
							break
						}
					}
				}
			}
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no field definitions found for API: %s", apiName)
	}

	// 使用 QuantDBFactory 按 target_db_path 获取与数据存储一致的 DuckDB
	if targetDBPath == "" {
		return nil, fmt.Errorf("target_db_path is required for create table (must match Quant Data Store)")
	}
	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("get QuantDB for target_db_path: %w", err)
	}

	// 构建 TableSchema
	schema := buildTableSchema(apiName, fields)
	logrus.Debugf("[CreateTableFromMetadata] 准备创建表: %s, 字段数=%d", apiName, len(schema.Columns))

	// 使用 QuantDB Adapter 创建表
	if err := quantDB.CreateTable(ctx, schema); err != nil {
		return nil, fmt.Errorf("failed to create table %s: %w", apiName, err)
	}

	logrus.Debugf("[CreateTableFromMetadata] 表创建成功: %s", apiName)

	return map[string]interface{}{
		"table_name":     apiName,
		"field_count":    len(fields),
		"target_db_path": targetDBPath,
	}, nil
}

// buildTableSchema 从字段定义构建 TableSchema
func buildTableSchema(tableName string, fields []metadata.FieldMeta) *datastore.TableSchema {
	schema := &datastore.TableSchema{
		ID:          shared.NewID(),
		TableName:   tableName,
		Columns:     make([]datastore.ColumnDef, 0, len(fields)+2), // +2 for sync_batch_id and created_at
		PrimaryKeys: []string{},
		Indexes:     []datastore.IndexDef{},
	}

	// 去重字段名
	seenFields := make(map[string]bool)
	primaryKeys := make([]string, 0)

	for _, f := range fields {
		fieldName := strings.TrimSpace(f.Name)
		if fieldName == "" || seenFields[fieldName] {
			continue
		}
		seenFields[fieldName] = true

		col := datastore.ColumnDef{
			Name:       fieldName,
			SourceType: f.Type,
			TargetType: mapTypeToDuckDB(f.Type),
			Nullable:   !f.IsPrimary,
			Comment:    f.Description,
		}
		schema.Columns = append(schema.Columns, col)

		if f.IsPrimary {
			primaryKeys = append(primaryKeys, fieldName)
		}
		if f.IsIndex {
			// 为索引字段创建单列索引
			schema.Indexes = append(schema.Indexes, datastore.IndexDef{
				Name:    fmt.Sprintf("idx_%s_%s", tableName, fieldName),
				Columns: []string{fieldName},
				Unique:  false,
			})
		}
	}

	// 添加同步批次字段（用于回滚）
	schema.Columns = append(schema.Columns, datastore.ColumnDef{
		Name:       "sync_batch_id",
		SourceType: "string",
		TargetType: "VARCHAR",
		Nullable:   true,
		Comment:    "同步批次ID，用于数据回滚",
	})

	// 添加创建时间字段
	schema.Columns = append(schema.Columns, datastore.ColumnDef{
		Name:       "created_at",
		SourceType: "timestamp",
		TargetType: "TIMESTAMP",
		Nullable:   true,
		Comment:    "记录创建时间",
	})

	schema.PrimaryKeys = primaryKeys

	return schema
}

// mapTypeToDuckDB 将数据源字段类型映射为 DuckDB 类型
func mapTypeToDuckDB(sourceType string) string {
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	switch sourceType {
	case "int", "integer":
		return "INTEGER"
	case "bigint", "long":
		return "BIGINT"
	case "float", "number", "double", "decimal":
		return "DOUBLE"
	case "str", "string", "text", "varchar", "char":
		return "VARCHAR"
	case "date":
		return "DATE"
	case "datetime", "timestamp":
		return "TIMESTAMP"
	case "bool", "boolean":
		return "BOOLEAN"
	default:
		return "VARCHAR" // 默认使用 VARCHAR
	}
}

// CreateTablesFromCatalogJob 根据数据源的 API 元数据批量创建数据表
// 这是一个模板任务，为每个 API 生成建表子任务
//
// 注意：该 Job Function 从 MetadataRepo 获取 API 元数据（已保存的数据），
// 而不是从上游任务获取。这是因为 create_tables 工作流是独立执行的，
// 与 metadata_crawl 工作流没有直接的上下游依赖关系。
//
// Input params:
//   - data_source_id: string - 数据源 ID（必需）
//   - target_db_path: string - 目标数据库路径（必需）
//   - max_tables: int - 最大建表数量（0=不限制）
//
// Output:
//   - status: string - 操作状态
//   - generated: int - 生成的子任务数量
//   - api_count: int - 数据源中的 API 总数
func CreateTablesFromCatalogJob(tc *task.TaskContext) (interface{}, error) {
	logrus.Debugf("[CreateTablesFromCatalog] Job Function 执行")

	// 获取必需参数
	dataSourceID := tc.GetParamString("data_source_id")
	targetDBPath := tc.GetParamString("target_db_path")
	maxTables, _ := tc.GetParamInt("max_tables")

	if dataSourceID == "" {
		return nil, fmt.Errorf("data_source_id is required")
	}
	if targetDBPath == "" {
		return nil, fmt.Errorf("target_db_path is required")
	}

	logrus.Debugf("[CreateTablesFromCatalog] data_source_id=%s, max_tables=%d", dataSourceID, maxTables)

	// 获取 Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("Engine dependency not found")
	}
	eng := engineInterface.(*engine.Engine)
	taskRegistry := eng.GetRegistry()

	// 从 MetadataRepo 获取数据源的 API 元数据
	repoInterface, ok := tc.GetDependency("MetadataRepo")
	if !ok {
		return nil, fmt.Errorf("MetadataRepo dependency not found")
	}
	repo := repoInterface.(metadata.Repository)

	ctx := context.Background()
	apiMetadataList, err := repo.ListAPIMetadataByDataSource(ctx, shared.ID(dataSourceID))
	if err != nil {
		return nil, fmt.Errorf("failed to list API metadata for data source: %w", err)
	}

	if len(apiMetadataList) == 0 {
		logrus.Warnf("⚠️ [CreateTablesFromCatalog] 数据源 %s 没有 API 元数据，请先执行元数据爬取工作流", dataSourceID)
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"api_count": 0,
			"message":   fmt.Sprintf("数据源 %s 没有 API 元数据，请先执行元数据爬取工作流", dataSourceID),
		}, nil
	}

	totalAPICount := len(apiMetadataList)
	logrus.Debugf("[CreateTablesFromCatalog] 从数据源获取到 %d 个 API 元数据", totalAPICount)

	// 应用数量限制
	if maxTables > 0 && len(apiMetadataList) > maxTables {
		apiMetadataList = apiMetadataList[:maxTables]
		logrus.Debugf("[CreateTablesFromCatalog] 限制建表数量到 %d", maxTables)
	}

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	for _, apiMeta := range apiMetadataList {
		apiName := apiMeta.Name
		if apiName == "" {
			logrus.Warnf("[CreateTablesFromCatalog] API 元数据 ID=%s 名称为空，跳过", apiMeta.ID)
			continue
		}

		// 跳过没有响应字段的 API（无法建表）
		if len(apiMeta.ResponseFields) == 0 {
			logrus.Debugf("[CreateTablesFromCatalog] API %s 没有响应字段，跳过", apiName)
			continue
		}

		// 将 ResponseFields 转换为可序列化的格式
		fields := make([]map[string]interface{}, len(apiMeta.ResponseFields))
		for i, f := range apiMeta.ResponseFields {
			fields[i] = map[string]interface{}{
				"name":        f.Name,
				"type":        f.Type,
				"description": f.Description,
				"is_primary":  f.IsPrimary,
				"is_index":    f.IsIndex,
			}
		}

		subTaskName := fmt.Sprintf("CreateTable_%s", apiName)
		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("创建表: %s", apiName), taskRegistry).
			WithJobFunction("CreateTableFromMetadata", map[string]interface{}{
				"api_name":       apiName,
				"data_source_id": dataSourceID,
				"target_db_path": targetDBPath,
				"fields":         fields,
			}).
			WithTaskHandler(task.TaskStatusSuccess, "TableCreationSuccess").
			WithTaskHandler(task.TaskStatusFailed, "TableCreationFailure").
			WithCompensationFunction("CompensateCreateTable"). // SAGA 补偿
			Build()
		if err != nil {
			logrus.Warnf("[CreateTablesFromCatalog] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			logrus.Warnf("[CreateTablesFromCatalog] 添加子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		generatedCount++
	}

	logrus.Debugf("[CreateTablesFromCatalog] 共生成 %d 个建表子任务", generatedCount)

	return map[string]interface{}{
		"status":    "success",
		"generated": generatedCount,
		"api_count": totalAPICount,
	}, nil
}

// DropTableJob 删除数据表（用于回滚/SAGA 补偿）
//
// 该 Job 使用 QuantDB Adapter 删除表，支持 DuckDB、ClickHouse 等多种数据库。
//
// Input params:
//   - table_name: string - 表名（必需）
//   - target_db_path: string - 目标数据库路径（用于日志）
//
// Output:
//   - dropped: bool - 是否成功删除
//   - table_name: string - 表名
func DropTableJob(tc *task.TaskContext) (interface{}, error) {
	ctx := context.Background()

	tableName := tc.GetParamString("table_name")
	targetDBPath := tc.GetParamString("target_db_path")

	if tableName == "" {
		return nil, fmt.Errorf("table_name is required")
	}
	if targetDBPath == "" {
		return nil, fmt.Errorf("target_db_path is required for drop table (must match Quant Data Store)")
	}

	logrus.Debugf("[DropTable] 删除表: %s", tableName)

	quantDB, err := GetQuantDBForPath(tc, targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("get QuantDB for target_db_path: %w", err)
	}

	// 使用 QuantDB Adapter 删除表
	if err := quantDB.DropTable(ctx, tableName); err != nil {
		return nil, fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	logrus.Debugf("[DropTable] 表删除成功: %s", tableName)

	return map[string]interface{}{
		"dropped":    true,
		"table_name": tableName,
	}, nil
}

// ==================== 辅助函数 ====================

// extractFieldsFromUpstream 从上游任务提取字段定义（后备方法）
func extractFieldsFromUpstream(tc *task.TaskContext) []metadata.FieldMeta {
	for key, val := range tc.Params {
		if !strings.HasPrefix(key, "_cached_") {
			continue
		}

		if resultMap, ok := val.(map[string]interface{}); ok {
			if fields, ok := resultMap["response_fields"]; ok {
				return convertToFieldMeta(fields)
			}
			if apiMeta, ok := resultMap["api_metadata"].(map[string]interface{}); ok {
				if fields, ok := apiMeta["response_fields"]; ok {
					return convertToFieldMeta(fields)
				}
			}
		}
	}
	return nil
}

// extractAPIMetadataFromUpstream 从子任务结果中提取 API 元数据列表
// 优先级：(1) 当前任务的 ExtractMapsFromSubTasks (2) 上游模板任务 GetUpstreamResult("FetchAPIDetails") (3) _cached_* 参数
func extractAPIMetadataFromUpstream(tc *task.TaskContext) []map[string]interface{} {
	// 1. 当前任务若有子任务，直接提取（SaveAllMetadata 无子任务，子任务属于 FetchAPIDetails）
	apiMetadataMaps := tc.ExtractMapsFromSubTasks("api_metadata")
	if len(apiMetadataMaps) > 0 {
		logrus.Debugf("extractAPIMetadataFromUpstream: ExtractMapsFromSubTasks 提取到 %d 个 api_metadata", len(apiMetadataMaps))
		return apiMetadataMaps
	}

	// 2. 通过 TaskContext 获取上游模板任务 FetchAPIDetails 的结果（含子任务聚合时由引擎填入）
	upstream := tc.GetUpstreamResult("FetchAPIDetails")
	if upstream != nil {
		apiMetadataMaps = extractAPIMetadataFromMap(upstream)
		if len(apiMetadataMaps) > 0 {
			logrus.Debugf("extractAPIMetadataFromUpstream: GetUpstreamResult(FetchAPIDetails) 提取到 %d 个 api_metadata", len(apiMetadataMaps))
			return apiMetadataMaps
		}
		logrus.Warnf("extractAPIMetadataFromUpstream: GetUpstreamResult(FetchAPIDetails) 有值但未解析出 api_metadata，keys=%v（需含 subtask_results 或 sub_tasks 且每项有 result.api_metadata 或 api_metadata）", upstreamResultKeys(upstream))
	}

	// 3. 回退：从 _cached_* 参数中收集（引擎将上游结果注入 Params 时）
	apiMetadataMaps = extractAPIMetadataFromCachedParams(tc)
	if len(apiMetadataMaps) > 0 {
		logrus.Debugf("extractAPIMetadataFromUpstream: 从 _cached_* 回退提取到 %d 个 api_metadata", len(apiMetadataMaps))
		return apiMetadataMaps
	}

	return nil
}

// extractAPIMetadataFromMap 从单个 map 中提取 api_metadata 列表（支持 subtask_results / sub_tasks 结构）
func extractAPIMetadataFromMap(m map[string]interface{}) []map[string]interface{} {
	for _, key := range []string{"subtask_results", "sub_tasks"} {
		raw := m[key]
		if raw == nil {
			continue
		}
		if out := extractAPIMetadataFromSlice(raw); len(out) > 0 {
			return out
		}
	}
	return nil
}

// extractAPIMetadataFromSlice 从 slice 中提取 api_metadata，支持 []interface{} 与 []map[string]interface{}（引擎可能传入后者）
func extractAPIMetadataFromSlice(raw interface{}) []map[string]interface{} {
	if raw == nil {
		return nil
	}
	switch arr := raw.(type) {
	case []interface{}:
		return extractAPIMetadataFromResultEntries(arr)
	case []map[string]interface{}:
		out := make([]map[string]interface{}, 0, len(arr))
		for _, entry := range arr {
			if am := extractOneAPIMetadataFromEntry(entry); am != nil {
				out = append(out, am)
			}
		}
		return out
	default:
		return nil
	}
}

// extractOneAPIMetadataFromEntry 从单条 entry（map）中提取 api_metadata
func extractOneAPIMetadataFromEntry(entry map[string]interface{}) map[string]interface{} {
	if result, _ := entry["result"].(map[string]interface{}); result != nil {
		if am, _ := result["api_metadata"].(map[string]interface{}); am != nil {
			return am
		}
	}
	if am, _ := entry["api_metadata"].(map[string]interface{}); am != nil {
		return am
	}
	return nil
}

// upstreamResultKeys 返回 map 的 keys，用于调试日志（避免打印大 value）
func upstreamResultKeys(m map[string]interface{}) []string {
	if m == nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// extractAPIMetadataFromCachedParams 从 tc.Params 的 _cached_* 中收集 api_metadata
// 支持形态：
//   (1) 直接 _cached_* = {"api_metadata": {...}}
//   (2) 模板聚合 subtask_results[].result.api_metadata（Task Engine 注入子任务执行结果时使用）
//   (3) 兼容 sub_tasks[].result.api_metadata（部分引擎用 sub_tasks 键名且元素含 result 时）
func extractAPIMetadataFromCachedParams(tc *task.TaskContext) []map[string]interface{} {
	var out []map[string]interface{}
	for k, v := range tc.Params {
		if !strings.HasPrefix(k, "_cached_") {
			continue
		}
		m, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		am, ok := m["api_metadata"].(map[string]interface{})
		if ok && am != nil {
			out = append(out, am)
			continue
		}
		// 模板任务聚合：subtask_results / sub_tasks（支持 []interface{} 与 []map[string]interface{}）
		for _, key := range []string{"subtask_results", "sub_tasks"} {
			if raw := m[key]; raw != nil {
				extracted := extractAPIMetadataFromSlice(raw)
				if len(extracted) > 0 {
					out = append(out, extracted...)
					break // 已从本 _cached_ 取到数据，跳过同 map 的另一个 key
				}
			}
		}
	}
	return out
}

// extractAPIMetadataFromResultEntries 从子任务结果数组中提取 api_metadata 列表
// 支持两种结构：(1) []{ result: { api_metadata } } (2) []{ api_metadata }（子任务返回值直接作为元素）
func extractAPIMetadataFromResultEntries(arr []interface{}) []map[string]interface{} {
	var out []map[string]interface{}
	for _, item := range arr {
		entry, _ := item.(map[string]interface{})
		if entry == nil {
			continue
		}
		// 结构 (1): entry.result.api_metadata
		if result, _ := entry["result"].(map[string]interface{}); result != nil {
			if am2, _ := result["api_metadata"].(map[string]interface{}); am2 != nil {
				out = append(out, am2)
				continue
			}
		}
		// 结构 (2): entry.api_metadata（子任务返回值直接为元素时）
		if am2, _ := entry["api_metadata"].(map[string]interface{}); am2 != nil {
			out = append(out, am2)
		}
	}
	return out
}

// convertToFieldMeta 将接口类型转换为 FieldMeta 切片
func convertToFieldMeta(raw interface{}) []metadata.FieldMeta {
	var fields []metadata.FieldMeta

	switch v := raw.(type) {
	case []metadata.FieldMeta:
		return v
	case []interface{}:
		for _, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				field := metadata.FieldMeta{}
				if name, ok := m["name"].(string); ok {
					field.Name = name
				}
				if typ, ok := m["type"].(string); ok {
					field.Type = typ
				}
				if desc, ok := m["description"].(string); ok {
					field.Description = desc
				}
				if isPrimary, ok := m["is_primary"].(bool); ok {
					field.IsPrimary = isPrimary
				}
				if isIndex, ok := m["is_index"].(bool); ok {
					field.IsIndex = isIndex
				}
				fields = append(fields, field)
			}
		}
	case string:
		// JSON 字符串，需要反序列化
		var meta metadata.APIMetadata
		if err := meta.UnmarshalResponseFieldsJSON(v); err == nil {
			return meta.ResponseFields
		}
	}

	return fields
}
