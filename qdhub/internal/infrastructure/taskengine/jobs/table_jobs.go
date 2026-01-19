// Package jobs provides Task Engine job functions for QDHub workflows.
package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// ==================== 建表 Job Functions ====================

// CreateTableFromMetadataJob 根据 API Metadata 动态创建数据表
//
// Input params:
//   - api_metadata_id: string - API Metadata ID（从 metadata repository 获取）
//   - api_name: string - API 名称（如果不提供 metadata_id，则从 data_source 获取）
//   - data_source_id: string - 数据源 ID
//   - target_db_path: string - 目标数据库路径
//   - fields: []FieldMeta - 字段定义（可选，优先使用）
//
// Output:
//   - table_name: string - 创建的表名
//   - field_count: int - 字段数量
//   - ddl: string - 执行的 DDL 语句
func CreateTableFromMetadataJob(tc *task.TaskContext) (interface{}, error) {
	// 获取参数
	apiName := tc.GetParamString("api_name")
	targetDBPath := tc.GetParamString("target_db_path")

	if apiName == "" || targetDBPath == "" {
		return nil, fmt.Errorf("api_name and target_db_path are required")
	}

	logrus.Printf("🔨 [CreateTableFromMetadata] 开始创建表: %s", apiName)

	// 获取字段定义
	var fields []metadata.FieldMeta

	// 优先从参数中获取字段定义
	if fieldsRaw := tc.GetParam("fields"); fieldsRaw != nil {
		fields = convertToFieldMeta(fieldsRaw)
	}

	// 如果没有字段定义，尝试从 MetadataRepo 获取
	if len(fields) == 0 {
		if repoInterface, ok := tc.GetDependency("MetadataRepo"); ok {
			repo := repoInterface.(metadata.Repository)
			dataSourceID := tc.GetParamString("data_source_id")
			if dataSourceID != "" {
				// 从 repository 获取 API metadata
				ctx := context.Background()
				ds, err := repo.GetDataSource(ctx, shared.ID(dataSourceID))
				if err == nil && ds != nil {
					for _, api := range ds.APIs {
						if api.Name == apiName {
							fields = api.ResponseFields
							break
						}
					}
				}
			}
		}
	}

	// 也尝试从上游任务结果获取
	if len(fields) == 0 {
		fields = extractFieldsFromUpstream(tc)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no field definitions found for API: %s", apiName)
	}

	// 生成 DDL
	ddl := generateTableDDL(apiName, fields)
	logrus.Printf("📝 [CreateTableFromMetadata] DDL: %s", ddl)

	// 执行建表
	db, err := sql.Open("sqlite3", targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec(ddl); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	logrus.Printf("✅ [CreateTableFromMetadata] 表创建成功: %s, 字段数=%d", apiName, len(fields))

	return map[string]interface{}{
		"table_name":     apiName,
		"field_count":    len(fields),
		"ddl":            ddl,
		"target_db_path": targetDBPath,
	}, nil
}

// CreateTablesFromCatalogJob 根据爬取的目录批量创建数据表
// 这是一个模板任务，为每个 API 生成建表子任务
//
// Input params:
//   - target_db_path: string - 目标数据库路径
//   - max_tables: int - 最大建表数量（0=不限制）
//
// Output:
//   - status: string - 操作状态
//   - generated: int - 生成的子任务数量
func CreateTablesFromCatalogJob(tc *task.TaskContext) (interface{}, error) {
	logrus.Printf("📋 [CreateTablesFromCatalog] Job Function 执行")

	targetDBPath := tc.GetParamString("target_db_path")
	maxTables, _ := tc.GetParamInt("max_tables")

	if targetDBPath == "" {
		return nil, fmt.Errorf("target_db_path is required")
	}

	// 获取 Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("Engine dependency not found")
	}
	eng := engineInterface.(*engine.Engine)
	taskRegistry := eng.GetRegistry()

	// 从上游任务提取 API 元数据列表
	apiMetadataList := extractAPIMetadataFromUpstream(tc)
	if len(apiMetadataList) == 0 {
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"message":   "未找到 API 元数据，跳过建表",
		}, nil
	}

	// 应用数量限制
	if maxTables > 0 && len(apiMetadataList) > maxTables {
		apiMetadataList = apiMetadataList[:maxTables]
	}

	logrus.Printf("📡 [CreateTablesFromCatalog] 为 %d 个 API 生成建表子任务", len(apiMetadataList))

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	for _, apiMeta := range apiMetadataList {
		apiName, _ := apiMeta["api_name"].(string)
		if apiName == "" {
			continue
		}

		subTaskName := fmt.Sprintf("CreateTable_%s", apiName)
		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("创建表: %s", apiName), taskRegistry).
			WithJobFunction("CreateTableFromMetadata", map[string]interface{}{
				"api_name":       apiName,
				"target_db_path": targetDBPath,
				"fields":         apiMeta["fields"],
			}).
			WithTaskHandler(task.TaskStatusSuccess, "TableCreationSuccess").
			WithTaskHandler(task.TaskStatusFailed, "TableCreationFailure").
			WithCompensationFunction("CompensateCreateTable"). // SAGA 补偿
			Build()
		if err != nil {
			logrus.Printf("❌ [CreateTablesFromCatalog] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			logrus.Printf("❌ [CreateTablesFromCatalog] 添加子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		generatedCount++
		logrus.Printf("✅ [CreateTablesFromCatalog] 子任务已添加: %s", subTaskName)
	}

	return map[string]interface{}{
		"status":    "success",
		"generated": generatedCount,
	}, nil
}

// DropTableJob 删除数据表（用于回滚）
//
// Input params:
//   - table_name: string - 表名
//   - target_db_path: string - 目标数据库路径
//
// Output:
//   - dropped: bool - 是否成功删除
//   - table_name: string - 表名
func DropTableJob(tc *task.TaskContext) (interface{}, error) {
	tableName := tc.GetParamString("table_name")
	targetDBPath := tc.GetParamString("target_db_path")

	if tableName == "" || targetDBPath == "" {
		return nil, fmt.Errorf("table_name and target_db_path are required")
	}

	logrus.Printf("🗑️ [DropTable] 删除表: %s", tableName)

	db, err := sql.Open("sqlite3", targetDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName)
	if _, err := db.Exec(dropSQL); err != nil {
		return nil, fmt.Errorf("failed to drop table: %w", err)
	}

	logrus.Printf("✅ [DropTable] 表删除成功: %s", tableName)

	return map[string]interface{}{
		"dropped":    true,
		"table_name": tableName,
	}, nil
}

// ==================== 辅助函数 ====================

// generateTableDDL 根据字段定义生成建表 DDL
func generateTableDDL(tableName string, fields []metadata.FieldMeta) string {
	var sb strings.Builder

	// 表名用双引号包裹（避免 SQLite 保留字冲突）
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\n", tableName))
	sb.WriteString("    \"id\" INTEGER PRIMARY KEY AUTOINCREMENT,\n")

	// 去重字段名
	seenFields := make(map[string]bool)
	var uniqueFields []metadata.FieldMeta
	for _, f := range fields {
		fieldName := strings.TrimSpace(f.Name)
		if fieldName == "" || fieldName == "id" || seenFields[fieldName] {
			continue
		}
		seenFields[fieldName] = true
		uniqueFields = append(uniqueFields, f)
	}

	for i, f := range uniqueFields {
		sqlType := mapTypeToSQLite(f.Type)
		sb.WriteString(fmt.Sprintf("    \"%s\" %s", f.Name, sqlType))
		if i < len(uniqueFields)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString(",\n")
		}
	}

	// 添加同步批次字段（用于回滚）
	sb.WriteString("    \"sync_batch_id\" TEXT,\n")
	sb.WriteString("    \"created_at\" DATETIME DEFAULT CURRENT_TIMESTAMP\n")
	sb.WriteString(")")

	return sb.String()
}

// mapTypeToSQLite 将数据源字段类型映射为 SQLite 类型
func mapTypeToSQLite(sourceType string) string {
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	switch sourceType {
	case "int", "integer", "bigint":
		return "INTEGER"
	case "float", "number", "double", "decimal":
		return "REAL"
	case "str", "string", "text", "varchar", "char":
		return "TEXT"
	case "date", "datetime", "timestamp":
		return "TEXT" // SQLite 用 TEXT 存储日期
	case "bool", "boolean":
		return "INTEGER" // SQLite 用 INTEGER 存储布尔
	default:
		return "TEXT" // 默认使用 TEXT
	}
}

// extractFieldsFromUpstream 从上游任务提取字段定义
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
// 使用 Task Engine v1.0.6+ 的子任务结果聚合 API
func extractAPIMetadataFromUpstream(tc *task.TaskContext) []map[string]interface{} {
	// 使用 ExtractMapsFromSubTasks 直接提取 api_metadata 字段
	// Task Engine 会自动聚合模板任务的子任务结果
	apiMetadataMaps := tc.ExtractMapsFromSubTasks("api_metadata")
	if len(apiMetadataMaps) > 0 {
		logrus.Debugf("extractAPIMetadataFromUpstream: 提取到 %d 个 api_metadata", len(apiMetadataMaps))
		return apiMetadataMaps
	}

	return nil
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
