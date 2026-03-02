package datasource_test

import (
	"testing"

	"qdhub/internal/infrastructure/datasource"
	"qdhub/internal/infrastructure/datasource/tushare"
)

func TestRegistry_RegisterAdapter(t *testing.T) {
	registry := datasource.NewRegistry()
	adapter := tushare.NewAdapter()

	err := registry.RegisterAdapter(adapter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to register again - should fail
	err = registry.RegisterAdapter(adapter)
	if err == nil {
		t.Error("expected error when registering duplicate adapter")
	}
}

func TestRegistry_GetAdapter(t *testing.T) {
	registry := datasource.NewRegistry()
	adapter := tushare.NewAdapter()
	registry.RegisterAdapter(adapter)

	// Get existing adapter
	got, err := registry.GetAdapter("tushare")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name() != "tushare" {
		t.Errorf("expected adapter name 'tushare', got '%s'", got.Name())
	}

	// Get non-existing adapter
	_, err = registry.GetAdapter("unknown")
	if err == nil {
		t.Error("expected error for unknown adapter")
	}
}

func TestRegistry_GetClient(t *testing.T) {
	registry := datasource.NewRegistry()
	adapter := tushare.NewAdapter()
	registry.RegisterAdapter(adapter)

	client, err := registry.GetClient("tushare")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.Name() != "tushare" {
		t.Errorf("expected client name 'tushare', got '%s'", client.Name())
	}
}

func TestRegistry_GetCrawler(t *testing.T) {
	registry := datasource.NewRegistry()
	adapter := tushare.NewAdapter()
	registry.RegisterAdapter(adapter)

	crawler, err := registry.GetCrawler("tushare")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if crawler.Name() != "tushare" {
		t.Errorf("expected crawler name 'tushare', got '%s'", crawler.Name())
	}
}

func TestRegistry_ListAdapters(t *testing.T) {
	registry := datasource.NewRegistry()

	// Empty registry
	adapters := registry.ListAdapters()
	if len(adapters) != 0 {
		t.Errorf("expected 0 adapters, got %d", len(adapters))
	}

	// Add adapter
	adapter := tushare.NewAdapter()
	registry.RegisterAdapter(adapter)

	adapters = registry.ListAdapters()
	if len(adapters) != 1 {
		t.Errorf("expected 1 adapter, got %d", len(adapters))
	}
	if adapters[0] != "tushare" {
		t.Errorf("expected adapter name 'tushare', got '%s'", adapters[0])
	}
}

func TestRegistry_RegisterClient_Standalone(t *testing.T) {
	registry := datasource.NewRegistry()
	client := tushare.NewClient()

	err := registry.RegisterClient(client)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to register again - should fail
	err = registry.RegisterClient(client)
	if err == nil {
		t.Error("expected error when registering duplicate client")
	}
}

func TestRegistry_RegisterCrawler_Standalone(t *testing.T) {
	registry := datasource.NewRegistry()
	crawler := tushare.NewCrawler()

	err := registry.RegisterCrawler(crawler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to register again - should fail
	err = registry.RegisterCrawler(crawler)
	if err == nil {
		t.Error("expected error when registering duplicate crawler")
	}
}

func TestGlobalRegistry(t *testing.T) {
	registry1 := datasource.GlobalRegistry()
	registry2 := datasource.GlobalRegistry()

	if registry1 != registry2 {
		t.Error("expected global registry to be singleton")
	}
}
