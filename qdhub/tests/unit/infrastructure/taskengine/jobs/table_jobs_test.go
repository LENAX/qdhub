package jobs_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/jobs"
)

func TestCreateTableFromMetadataJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.CreateTableFromMetadataJob(tc)
	if err == nil {
		t.Error("expected error for missing params")
	}
	if err.Error() != "api_name and target_db_path are required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestCreateTableFromMetadataJob_MissingAPIName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"target_db_path": "/tmp/test.db",
	})

	_, err := jobs.CreateTableFromMetadataJob(tc)
	if err == nil {
		t.Error("expected error for missing api_name")
	}
}

func TestCreateTableFromMetadataJob_MissingDBPath(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"api_name": "daily",
	})

	_, err := jobs.CreateTableFromMetadataJob(tc)
	if err == nil {
		t.Error("expected error for missing target_db_path")
	}
}

func TestDropTableJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.DropTableJob(tc)
	if err == nil {
		t.Error("expected error for missing params")
	}
	if err.Error() != "table_name and target_db_path are required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDropTableJob_MissingTableName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"target_db_path": "/tmp/test.db",
	})

	_, err := jobs.DropTableJob(tc)
	if err == nil {
		t.Error("expected error for missing table_name")
	}
}

func TestDropTableJob_MissingDBPath(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"table_name": "daily",
	})

	_, err := jobs.DropTableJob(tc)
	if err == nil {
		t.Error("expected error for missing target_db_path")
	}
}
