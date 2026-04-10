package realtimestore_test

import (
	"encoding/json"
	"testing"

	"qdhub/internal/infrastructure/realtimestore"
)

func TestLatestQuoteStore_BuildFullItemsJSON_CachesByVersion(t *testing.T) {
	store := realtimestore.NewLatestQuoteStore()
	store.Update("000001.SZ", map[string]interface{}{
		"ts_code": "000001.SZ",
		"price":   10.11,
	})

	b1, v1, err := store.BuildFullItemsJSON()
	if err != nil {
		t.Fatalf("BuildFullItemsJSON first call failed: %v", err)
	}
	b2, v2, err := store.BuildFullItemsJSON()
	if err != nil {
		t.Fatalf("BuildFullItemsJSON second call failed: %v", err)
	}
	if v1 != v2 {
		t.Fatalf("expected same version, got %d and %d", v1, v2)
	}
	if string(b1) != string(b2) {
		t.Fatalf("expected cached json to be reused")
	}

	store.Update("000002.SZ", map[string]interface{}{
		"ts_code": "000002.SZ",
		"price":   20.22,
	})
	b3, v3, err := store.BuildFullItemsJSON()
	if err != nil {
		t.Fatalf("BuildFullItemsJSON after update failed: %v", err)
	}
	if v3 == v2 {
		t.Fatalf("expected version bump after update")
	}
	if string(b3) == string(b2) {
		t.Fatalf("expected items json to change after update")
	}
}

func TestLatestQuoteStore_GetSubsetQuotes_ReturnsTypedSubset(t *testing.T) {
	store := realtimestore.NewLatestQuoteStore()
	store.Update("000001.SZ", map[string]interface{}{
		"ts_code":     "000001.SZ",
		"code":        "000001.SZ",
		"price":       "10.50",
		"trade_time":  "2026-04-08 09:30:00",
		"ask_price1":  10.51,
		"bid_price1":  10.50,
		"ask_volume1": 100,
		"bid_volume1": 200,
	})
	store.Update("000002.SZ", map[string]interface{}{
		"ts_code": "000002.SZ",
		"price":   20.50,
	})

	subset := store.GetSubsetQuotes([]string{"000001.SZ"})
	if len(subset) != 1 {
		t.Fatalf("expected subset size 1, got %d", len(subset))
	}
	quote, ok := subset["000001.SZ"]
	if !ok {
		t.Fatalf("expected 000001.SZ in subset")
	}
	if quote.TsCode != "000001.SZ" {
		t.Fatalf("expected TsCode=000001.SZ, got %s", quote.TsCode)
	}
	if quote.Price != 10.50 {
		t.Fatalf("expected Price=10.50, got %v", quote.Price)
	}
	if quote.AskPrice1 != 10.51 || quote.BidVolume1 != 200 {
		t.Fatalf("expected level1 fields to be mapped, got %+v", quote)
	}

	gotMap, ok := store.Get("000001.SZ")
	if !ok {
		t.Fatalf("expected Get to return existing quote")
	}
	raw, err := json.Marshal(gotMap)
	if err != nil || len(raw) == 0 {
		t.Fatalf("expected Get result to remain json serializable")
	}
}
