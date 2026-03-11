package workflows_test

import (
	"context"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/engine"

	"qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/internal/infrastructure/taskengine/workflows"
)

func TestTushareWSStreamingBuilder_BuildSuccess(t *testing.T) {
	eng, err := engine.NewEngine(2, 30, nil, nil, nil)
	if err != nil {
		t.Fatalf("new engine failed: %v", err)
	}
	if err := taskengine.RegisterJobFunctions(context.Background(), eng); err != nil {
		t.Fatalf("register jobs failed: %v", err)
	}

	builder := workflows.NewTushareWSStreamingBuilder(
		eng.GetRegistry(),
		workflows.TushareWSStreamingParams{
			DataSourceName: "tushare",
			Token:          "test-token",
			TargetDBPath:   "/tmp/test.duckdb",
			APIName:        "ts_realtime_mkt_tick",
		},
		"tushare_ws_tick",
		&realtime.TushareWSTickCollector{
			Token:        "test-token",
			TargetDBPath: "/tmp/test.duckdb",
			Topic:        "HQ_STK_TICK",
			Codes:        []string{"000001.SZ"},
		},
	)
	wf, err := builder.Build()
	if err != nil {
		t.Fatalf("build should succeed, got error: %v", err)
	}
	if wf == nil {
		t.Fatalf("workflow should not be nil")
	}
}

func TestTushareWSStreamingBuilder_BuildValidationError(t *testing.T) {
	builder := workflows.NewTushareWSStreamingBuilder(nil, workflows.TushareWSStreamingParams{}, "", nil)
	_, err := builder.Build()
	if err == nil {
		t.Fatalf("expected validation error for empty params")
	}
}

