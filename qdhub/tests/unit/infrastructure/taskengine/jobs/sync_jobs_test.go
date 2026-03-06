package jobs_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine/jobs"
)

func TestSyncAPIDataJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.SyncAPIDataJob(tc)
	if err == nil {
		t.Error("expected error for missing params")
	}
	if err.Error() != "data_source_name and api_name are required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSyncAPIDataJob_MissingDataSourceName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"api_name": "daily",
	})

	_, err := jobs.SyncAPIDataJob(tc)
	if err == nil {
		t.Error("expected error for missing data_source_name")
	}
}

func TestSyncAPIDataJob_MissingAPIName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"data_source_name": "tushare",
	})

	_, err := jobs.SyncAPIDataJob(tc)
	if err == nil {
		t.Error("expected error for missing api_name")
	}
}

func TestDeleteSyncedDataJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.DeleteSyncedDataJob(tc)
	if err == nil {
		t.Error("expected error for missing params")
	}
	if err.Error() != "api_name, target_db_path and sync_batch_id are required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDeleteSyncedDataJob_MissingAPIName(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"target_db_path": "/tmp/test.db",
		"sync_batch_id":  "batch-123",
	})

	_, err := jobs.DeleteSyncedDataJob(tc)
	if err == nil {
		t.Error("expected error for missing api_name")
	}
}

func TestDeleteSyncedDataJob_MissingSyncBatchID(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"api_name":       "daily",
		"target_db_path": "/tmp/test.db",
	})

	_, err := jobs.DeleteSyncedDataJob(tc)
	if err == nil {
		t.Error("expected error for missing sync_batch_id")
	}
}

func TestGenerateDatetimeRangeJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.GenerateDatetimeRangeJob(tc)
	if err == nil {
		t.Error("expected error for missing start/end")
	}
}

func TestGenerateDatetimeRangeJob_BasicDaily(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"start": "2025-01-01",
		"end":   "2025-01-03",
		"freq":  "D",
	})

	out, err := jobs.GenerateDatetimeRangeJob(tc)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	result, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", out)
	}
	dts, ok := result["datetimes"].([]string)
	if !ok {
		t.Fatalf("expected []string datetimes, got %T", result["datetimes"])
	}
	if len(dts) != 3 {
		t.Errorf("expected 3 datetimes, got %d", len(dts))
	}
}

func TestGenerateTimeWindowSubTasksJob_MissingParams(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{})

	_, err := jobs.GenerateTimeWindowSubTasksJob(tc)
	if err == nil {
		t.Error("expected error for missing data_source_name and api_name")
	}
}
