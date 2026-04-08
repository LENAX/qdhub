package sync_test

import (
	"testing"

	"qdhub/internal/domain/sync"
)

func TestDependencyResolver_Resolve_SimpleCase(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// Selected APIs: daily, stock_basic
	// daily depends on stock_basic.ts_code
	selectedAPIs := []string{"daily", "stock_basic"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"daily": {
			{ParamName: "ts_code", SourceAPI: "stock_basic", SourceField: "ts_code", IsList: true},
		},
		"stock_basic": {},
	}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	// Should have 2 levels: [stock_basic], [daily]
	if len(graph.Levels) != 2 {
		t.Errorf("Levels length = %d, expected 2", len(graph.Levels))
	}

	// Level 0 should contain stock_basic (no dependencies)
	if len(graph.Levels[0]) != 1 || graph.Levels[0][0] != "stock_basic" {
		t.Errorf("Level 0 should contain only stock_basic, got %v", graph.Levels[0])
	}

	// Level 1 should contain daily
	if len(graph.Levels[1]) != 1 || graph.Levels[1][0] != "daily" {
		t.Errorf("Level 1 should contain only daily, got %v", graph.Levels[1])
	}

	// ResolvedAPIs should contain both
	if len(resolvedAPIs) != 2 {
		t.Errorf("ResolvedAPIs length = %d, expected 2", len(resolvedAPIs))
	}

	// TaskConfigs should be created for each API
	if len(graph.TaskConfigs) != 2 {
		t.Errorf("TaskConfigs length = %d, expected 2", len(graph.TaskConfigs))
	}

	// daily should have template mode (has IsList=true dependency)
	if graph.TaskConfigs["daily"].SyncMode != sync.TaskSyncModeTemplate {
		t.Errorf("daily SyncMode = %s, expected template", graph.TaskConfigs["daily"].SyncMode)
	}

	// stock_basic should have direct mode (no dependencies)
	if graph.TaskConfigs["stock_basic"].SyncMode != sync.TaskSyncModeDirect {
		t.Errorf("stock_basic SyncMode = %s, expected direct", graph.TaskConfigs["stock_basic"].SyncMode)
	}
}

func TestDependencyResolver_Resolve_AutoAddDependencies(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// Selected APIs: daily only
	// daily depends on stock_basic.ts_code (not selected)
	selectedAPIs := []string{"daily"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"daily": {
			{ParamName: "ts_code", SourceAPI: "stock_basic", SourceField: "ts_code", IsList: true},
		},
		"stock_basic": {},
	}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	// Should auto-add stock_basic as dependency
	if len(resolvedAPIs) != 2 {
		t.Errorf("ResolvedAPIs length = %d, expected 2 (should auto-add stock_basic)", len(resolvedAPIs))
	}

	// Should have 2 levels
	if len(graph.Levels) != 2 {
		t.Errorf("Levels length = %d, expected 2", len(graph.Levels))
	}
}

func TestDependencyResolver_Resolve_TransitiveDependencies(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// A -> B -> C (transitive dependencies)
	// API_C has no dependencies
	// API_B depends on API_C
	// API_A depends on API_B
	selectedAPIs := []string{"api_a"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"api_a": {
			{ParamName: "param", SourceAPI: "api_b", SourceField: "field", IsList: false},
		},
		"api_b": {
			{ParamName: "param", SourceAPI: "api_c", SourceField: "field", IsList: false},
		},
		"api_c": {},
	}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	// Should have 3 APIs (all transitively resolved)
	if len(resolvedAPIs) != 3 {
		t.Errorf("ResolvedAPIs length = %d, expected 3", len(resolvedAPIs))
	}

	// Should have 3 levels: [api_c], [api_b], [api_a]
	if len(graph.Levels) != 3 {
		t.Errorf("Levels length = %d, expected 3", len(graph.Levels))
	}
}

func TestDependencyResolver_Resolve_MultipleDependencies(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// daily depends on both trade_cal and stock_basic
	selectedAPIs := []string{"daily"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"daily": {
			{ParamName: "trade_date", SourceAPI: "trade_cal", SourceField: "cal_date", IsList: true},
			{ParamName: "ts_code", SourceAPI: "stock_basic", SourceField: "ts_code", IsList: true},
		},
		"trade_cal":   {},
		"stock_basic": {},
	}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	// Should have 3 APIs
	if len(resolvedAPIs) != 3 {
		t.Errorf("ResolvedAPIs length = %d, expected 3", len(resolvedAPIs))
	}

	// Should have 2 levels: [trade_cal, stock_basic], [daily]
	if len(graph.Levels) != 2 {
		t.Errorf("Levels length = %d, expected 2", len(graph.Levels))
	}

	// Level 0 should have 2 APIs (both base APIs)
	if len(graph.Levels[0]) != 2 {
		t.Errorf("Level 0 length = %d, expected 2", len(graph.Levels[0]))
	}

	// daily should have 2 param mappings
	if len(graph.TaskConfigs["daily"].ParamMappings) != 2 {
		t.Errorf("daily ParamMappings length = %d, expected 2", len(graph.TaskConfigs["daily"].ParamMappings))
	}
}

func TestDependencyResolver_Resolve_CircularDependency(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// A -> B -> A (circular)
	selectedAPIs := []string{"api_a"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"api_a": {
			{ParamName: "param", SourceAPI: "api_b", SourceField: "field", IsList: false},
		},
		"api_b": {
			{ParamName: "param", SourceAPI: "api_a", SourceField: "field", IsList: false},
		},
	}

	_, _, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err == nil {
		t.Error("Resolve should fail for circular dependencies")
	}
}

func TestDependencyResolver_Resolve_MissingDependency(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// daily depends on non-existent API
	selectedAPIs := []string{"daily"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"daily": {
			{ParamName: "ts_code", SourceAPI: "non_existent", SourceField: "ts_code", IsList: true},
		},
	}

	graph, _, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	// Should report missing APIs
	if len(graph.MissingAPIs) == 0 {
		t.Error("MissingAPIs should not be empty")
	}
	if graph.MissingAPIs[0] != "non_existent" {
		t.Errorf("MissingAPIs[0] = %s, expected non_existent", graph.MissingAPIs[0])
	}
}

func TestDependencyResolver_Resolve_EmptySelection(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	selectedAPIs := []string{}
	allAPIDependencies := map[string][]sync.ParamDependency{}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	if len(graph.Levels) != 0 {
		t.Errorf("Levels should be empty, got %d", len(graph.Levels))
	}
	if len(resolvedAPIs) != 0 {
		t.Errorf("ResolvedAPIs should be empty, got %d", len(resolvedAPIs))
	}
}

func TestDependencyResolver_Resolve_NoDependencies(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	// APIs with no dependencies
	selectedAPIs := []string{"api_a", "api_b", "api_c"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"api_a": {},
		"api_b": {},
		"api_c": {},
	}

	graph, resolvedAPIs, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	// All APIs should be at level 0
	if len(graph.Levels) != 1 {
		t.Errorf("Levels length = %d, expected 1", len(graph.Levels))
	}
	if len(graph.Levels[0]) != 3 {
		t.Errorf("Level 0 length = %d, expected 3", len(graph.Levels[0]))
	}
	if len(resolvedAPIs) != 3 {
		t.Errorf("ResolvedAPIs length = %d, expected 3", len(resolvedAPIs))
	}

	// All should have direct mode
	for _, api := range selectedAPIs {
		if graph.TaskConfigs[api].SyncMode != sync.TaskSyncModeDirect {
			t.Errorf("%s SyncMode = %s, expected direct", api, graph.TaskConfigs[api].SyncMode)
		}
	}
}

func TestDependencyResolver_Resolve_IndexDailyDependsOnFetchIndexBasic(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	selectedAPIs := []string{"index_daily"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"index_daily": {
			{ParamName: "ts_code", SourceAPI: "index_basic", SourceField: "ts_code", IsList: true},
		},
		"index_basic": {},
	}

	graph, _, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	cfg := graph.TaskConfigs["index_daily"]
	if cfg == nil {
		t.Fatal("missing task config for index_daily")
	}
	if len(cfg.Dependencies) != 1 || cfg.Dependencies[0] != "FetchIndexBasic" {
		t.Fatalf("expected Dependencies [FetchIndexBasic], got %+v", cfg.Dependencies)
	}
	if len(cfg.ParamMappings) != 1 || cfg.ParamMappings[0].SourceTask != "FetchIndexBasic" {
		t.Fatalf("expected ParamMappings SourceTask FetchIndexBasic, got %+v", cfg.ParamMappings)
	}
}

func TestDependencyResolver_Resolve_ParamMappingGeneration(t *testing.T) {
	resolver := sync.NewDependencyResolver()

	selectedAPIs := []string{"daily"}
	allAPIDependencies := map[string][]sync.ParamDependency{
		"daily": {
			{
				ParamName:   "ts_code",
				SourceAPI:   "stock_basic",
				SourceField: "ts_code",
				IsList:      true,
				FilterField: "list_status",
				FilterValue: "L",
			},
		},
		"stock_basic": {},
	}

	graph, _, err := resolver.Resolve(selectedAPIs, allAPIDependencies)
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}

	// Check ParamMapping is correctly generated
	dailyConfig := graph.TaskConfigs["daily"]
	if len(dailyConfig.ParamMappings) != 1 {
		t.Fatalf("ParamMappings length = %d, expected 1", len(dailyConfig.ParamMappings))
	}

	mapping := dailyConfig.ParamMappings[0]
	if mapping.ParamName != "ts_code" {
		t.Errorf("ParamName = %s, expected ts_code", mapping.ParamName)
	}
	if mapping.IsList != true {
		t.Error("IsList should be true")
	}
	if mapping.FilterField != "list_status" {
		t.Errorf("FilterField = %s, expected list_status", mapping.FilterField)
	}
}
