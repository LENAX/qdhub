package jobs_test

import (
	"context"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/infrastructure/taskengine/jobs"
)

func TestEffectiveSyncAPIDataJobTimeoutSeconds(t *testing.T) {
	belowMinWant := jobs.EffectiveSyncAPIDataJobTimeoutSeconds(30)
	tests := []struct {
		in   int
		want int
	}{
		{0, jobs.DefaultSyncAPIDataJobTimeoutSeconds},
		{-1, jobs.DefaultSyncAPIDataJobTimeoutSeconds},
		{30, belowMinWant},
		{60, belowMinWant},
		{89, belowMinWant},
		{90, belowMinWant},
		{91, 91},
		{120, 120},
		{180, 180},
	}
	for _, tt := range tests {
		if got := jobs.EffectiveSyncAPIDataJobTimeoutSeconds(tt.in); got != tt.want {
			t.Errorf("EffectiveSyncAPIDataJobTimeoutSeconds(%d)=%d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestSyncAPIDataJobTimeoutSecondsFromContext(t *testing.T) {
	wantFallback := jobs.EffectiveSyncAPIDataJobTimeoutSeconds(0)

	if got := jobs.SyncAPIDataJobTimeoutSecondsFromContext(nil); got != wantFallback {
		t.Errorf("nil TaskContext: got %d, want %d", got, wantFallback)
	}

	tc := task.NewTaskContext(context.Background(), "id", "name", "wf", "inst", nil)
	if got := jobs.SyncAPIDataJobTimeoutSecondsFromContext(tc); got != wantFallback {
		t.Errorf("TaskContext without engine deps: got %d, want %d", got, wantFallback)
	}
}
