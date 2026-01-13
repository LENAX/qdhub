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
