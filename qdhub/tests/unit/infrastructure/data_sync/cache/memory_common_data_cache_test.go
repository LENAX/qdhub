package cache_test

import (
	"context"
	"testing"
	"time"

	"qdhub/internal/infrastructure/data_sync/cache"
)

func TestMemoryCommonDataCache_GetSet(t *testing.T) {
	ctx := context.Background()
	c := cache.NewMemoryCommonDataCache(24 * time.Hour)

	key := "ds:api:hash1"
	data := []map[string]any{
		{"a": "1", "b": "2"},
		{"a": "3", "b": "4"},
	}

	// Miss
	got, ok := c.Get(ctx, key)
	if ok || got != nil {
		t.Errorf("Get() before Set: ok=%v, got=%v, want miss", ok, got)
	}

	// Set
	if err := c.Set(ctx, key, data, 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Hit
	got, ok = c.Get(ctx, key)
	if !ok {
		t.Fatal("Get() after Set: want hit")
	}
	if len(got) != 2 {
		t.Errorf("Get() len = %d, want 2", len(got))
	}
	if got[0]["a"] != "1" || got[1]["a"] != "3" {
		t.Errorf("Get() data = %v", got)
	}
}

func TestMemoryCommonDataCache_Expiry(t *testing.T) {
	ctx := context.Background()
	ttl := 20 * time.Millisecond
	c := cache.NewMemoryCommonDataCache(ttl)

	key := "expiry:key"
	data := []map[string]any{{"x": "1"}}
	if err := c.Set(ctx, key, data, 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	got, ok := c.Get(ctx, key)
	if !ok || len(got) != 1 {
		t.Errorf("Get() before expiry: ok=%v, len=%d", ok, len(got))
	}

	time.Sleep(ttl + 5*time.Millisecond)

	got, ok = c.Get(ctx, key)
	if ok || got != nil {
		t.Errorf("Get() after expiry: ok=%v, got=%v, want miss", ok, got)
	}
}
