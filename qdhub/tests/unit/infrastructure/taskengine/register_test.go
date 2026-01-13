package taskengine_test

import (
	"context"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/engine"

	"qdhub/internal/infrastructure/taskengine"
)

func setupTestEngine(t *testing.T) *engine.Engine {
	t.Helper()

	// Use simple engine configuration for testing (nil repos is allowed)
	eng, err := engine.NewEngine(5, 60, nil, nil, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	return eng
}

func TestRegisterJobFunctions(t *testing.T) {
	eng := setupTestEngine(t)
	ctx := context.Background()

	err := taskengine.RegisterJobFunctions(ctx, eng)
	if err != nil {
		t.Errorf("RegisterJobFunctions failed: %v", err)
	}

	// Verify registry is not nil
	registry := eng.GetRegistry()
	if registry == nil {
		t.Error("registry should not be nil")
	}
}

func TestRegisterTaskHandlers(t *testing.T) {
	eng := setupTestEngine(t)
	ctx := context.Background()

	err := taskengine.RegisterTaskHandlers(ctx, eng)
	if err != nil {
		t.Errorf("RegisterTaskHandlers failed: %v", err)
	}

	// Verify registry is not nil
	registry := eng.GetRegistry()
	if registry == nil {
		t.Error("registry should not be nil")
	}
}

func TestSetupDependencies(t *testing.T) {
	eng := setupTestEngine(t)

	deps := &taskengine.Dependencies{
		DataSourceRegistry: nil, // Can be nil for this test
		MetadataRepo:       nil,
	}

	// Should not panic
	taskengine.SetupDependencies(eng, deps)
}

func TestInitialize(t *testing.T) {
	eng := setupTestEngine(t)
	ctx := context.Background()

	deps := &taskengine.Dependencies{
		DataSourceRegistry: nil,
		MetadataRepo:       nil,
	}

	err := taskengine.Initialize(ctx, eng, deps)
	if err != nil {
		t.Errorf("Initialize failed: %v", err)
	}

	// Verify registry is set up
	registry := eng.GetRegistry()
	if registry == nil {
		t.Error("registry should not be nil after Initialize")
	}
}

func TestInitialize_RegistersAllComponents(t *testing.T) {
	eng := setupTestEngine(t)
	ctx := context.Background()

	deps := &taskengine.Dependencies{
		DataSourceRegistry: nil,
		MetadataRepo:       nil,
	}

	err := taskengine.Initialize(ctx, eng, deps)
	if err != nil {
		t.Errorf("Initialize failed: %v", err)
	}

	// This test verifies that Initialize completes without error,
	// which means all job functions and handlers are registered successfully
}
