package jobs_test

import (
	"strings"
	"testing"

	"qdhub/internal/infrastructure/taskengine/jobs"
)

// TestCreateTableFromMetadataJob_MissingAPIName 测试缺少 api_name 参数时的错误处理
func TestCreateTableFromMetadataJob_MissingAPIName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"target_db_path": "/tmp/test.db",
	})

	_, err := jobs.CreateTableFromMetadataJob(tc)
	if err == nil {
		t.Error("expected error for missing api_name")
	}
	if !strings.Contains(err.Error(), "api_name is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestCreateTableFromMetadataJob_MissingFields 测试缺少字段定义时的错误处理
// 注意：新实现需要 QuantDB 依赖，但首先检查字段定义
func TestCreateTableFromMetadataJob_MissingFields(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"api_name":       "daily",
		"target_db_path": "/tmp/test.db",
	})

	_, err := jobs.CreateTableFromMetadataJob(tc)
	if err == nil {
		t.Error("expected error for missing fields or QuantDB dependency")
	}
	// 可能是 "no field definitions found" 或 "QuantDB dependency not found"
	errMsg := err.Error()
	if !strings.Contains(errMsg, "field") && !strings.Contains(errMsg, "QuantDB") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestDropTableJob_MissingTableName 测试缺少 table_name 参数时的错误处理
func TestDropTableJob_MissingTableName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"target_db_path": "/tmp/test.db",
	})

	_, err := jobs.DropTableJob(tc)
	if err == nil {
		t.Error("expected error for missing table_name")
	}
	if !strings.Contains(err.Error(), "table_name is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestDropTableJob_MissingQuantDB 测试缺少 QuantDB 依赖时的错误处理
func TestDropTableJob_MissingQuantDB(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"table_name":     "daily",
		"target_db_path": "/tmp/test.db",
	})

	_, err := jobs.DropTableJob(tc)
	if err == nil {
		t.Error("expected error for missing QuantDB dependency")
	}
	if !strings.Contains(err.Error(), "QuantDB") {
		t.Errorf("unexpected error message: %v", err)
	}
}
