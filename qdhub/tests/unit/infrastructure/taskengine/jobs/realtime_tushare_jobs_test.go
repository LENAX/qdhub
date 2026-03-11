package jobs_test

import (
	"fmt"
	"testing"
	"time"

	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/internal/infrastructure/taskengine/jobs"
)

func TestTushareTickDBBatchWriteJob_MissingTargetDBPath(t *testing.T) {
	tc := mockTaskContext(map[string]interface{}{
		"api_name": "ts_realtime_mkt_tick",
		"data": []map[string]interface{}{
			{"ts_code": "000001.SZ", "price": 10.5},
		},
	})
	_, err := jobs.TushareTickDBBatchWriteJob(tc)
	if err == nil {
		t.Fatalf("expected error when target_db_path is missing")
	}
}

func TestTushareTickFrontendPushJob_UpdatesLatestQuoteStore(t *testing.T) {
	code := fmt.Sprintf("UT_%d.SZ", time.Now().UnixNano())
	tc := mockTaskContext(map[string]interface{}{
		"data": []map[string]interface{}{
			{"ts_code": code, "price": 12.34, "target_db_path": "/tmp/ignored.duckdb"},
		},
	})

	out, err := jobs.TushareTickFrontendPushJob(tc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map result, got %T", out)
	}
	if m["updated"] != 1 {
		t.Fatalf("expected updated=1, got %v", m["updated"])
	}

	store := realtimestore.DefaultLatestQuoteStore()
	got, ok := store.Get(code)
	if !ok {
		t.Fatalf("expected quote for %s in store", code)
	}
	if got["price"] != 12.34 {
		t.Fatalf("expected price=12.34, got %v", got["price"])
	}
	if _, exists := got["target_db_path"]; exists {
		t.Fatalf("target_db_path should be removed before caching")
	}
}

