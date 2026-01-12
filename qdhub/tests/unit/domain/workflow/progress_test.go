package workflow_test

import (
	"testing"
	"time"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
)

func TestProgressCalculator_CalculateProgress(t *testing.T) {
	calculator := workflow.NewProgressCalculator()

	tests := []struct {
		name           string
		tasks          []workflow.TaskInstance
		expectedMin    float64
		expectedMax    float64
	}{
		{
			name:           "empty tasks",
			tasks:          []workflow.TaskInstance{},
			expectedMin:    0.0,
			expectedMax:    0.0,
		},
		{
			name: "all pending",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusPending},
				{ID: shared.NewID(), Status: workflow.TaskStatusPending},
			},
			expectedMin: 0.0,
			expectedMax: 0.0,
		},
		{
			name: "all success",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
			},
			expectedMin: 100.0,
			expectedMax: 100.0,
		},
		{
			name: "half completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusPending},
			},
			expectedMin: 50.0,
			expectedMax: 50.0,
		},
		{
			name: "one third completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusRunning},
				{ID: shared.NewID(), Status: workflow.TaskStatusPending},
			},
			expectedMin: 33.0,
			expectedMax: 34.0,
		},
		{
			name: "skipped counts as completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusSkipped},
			},
			expectedMin: 100.0,
			expectedMax: 100.0,
		},
		{
			name: "failed does not count as completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusFailed},
			},
			expectedMin: 50.0,
			expectedMax: 50.0,
		},
		{
			name: "running does not count as completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusRunning},
			},
			expectedMin: 50.0,
			expectedMax: 50.0,
		},
		{
			name: "mixed statuses",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusSuccess},
				{ID: shared.NewID(), Status: workflow.TaskStatusSkipped},
				{ID: shared.NewID(), Status: workflow.TaskStatusFailed},
				{ID: shared.NewID(), Status: workflow.TaskStatusPending},
			},
			expectedMin: 60.0,
			expectedMax: 60.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			progress := calculator.CalculateProgress(tt.tasks)

			if progress < tt.expectedMin || progress > tt.expectedMax {
				t.Errorf("CalculateProgress() = %v, expected between %v and %v", 
					progress, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestProgressCalculator_EstimateRemainingTime(t *testing.T) {
	calculator := workflow.NewProgressCalculator()

	t.Run("nil instance returns nil", func(t *testing.T) {
		result := calculator.EstimateRemainingTime(nil)

		if result != nil {
			t.Errorf("EstimateRemainingTime(nil) should return nil, got %v", *result)
		}
	})

	t.Run("completed instance returns zero", func(t *testing.T) {
		instance := &workflow.WorkflowInstance{
			Status:   workflow.WfInstStatusSuccess,
			Progress: 100.0,
		}

		result := calculator.EstimateRemainingTime(instance)

		if result == nil {
			t.Fatal("EstimateRemainingTime() should not return nil for completed instance")
		}

		if *result != 0 {
			t.Errorf("EstimateRemainingTime() = %d, expected 0 for completed instance", *result)
		}
	})

	t.Run("failed instance returns zero", func(t *testing.T) {
		instance := &workflow.WorkflowInstance{
			Status: workflow.WfInstStatusFailed,
		}

		result := calculator.EstimateRemainingTime(instance)

		if result == nil {
			t.Fatal("EstimateRemainingTime() should not return nil for failed instance")
		}

		if *result != 0 {
			t.Errorf("EstimateRemainingTime() = %d, expected 0 for failed instance", *result)
		}
	})

	t.Run("cancelled instance returns zero", func(t *testing.T) {
		instance := &workflow.WorkflowInstance{
			Status: workflow.WfInstStatusCancelled,
		}

		result := calculator.EstimateRemainingTime(instance)

		if result == nil {
			t.Fatal("EstimateRemainingTime() should not return nil for cancelled instance")
		}

		if *result != 0 {
			t.Errorf("EstimateRemainingTime() = %d, expected 0 for cancelled instance", *result)
		}
	})

	t.Run("zero progress returns nil", func(t *testing.T) {
		instance := &workflow.WorkflowInstance{
			Status:    workflow.WfInstStatusRunning,
			Progress:  0.0,
			StartedAt: shared.Timestamp(time.Now().Add(-1 * time.Minute)),
		}

		result := calculator.EstimateRemainingTime(instance)

		if result != nil {
			t.Errorf("EstimateRemainingTime() should return nil for zero progress, got %v", *result)
		}
	})

	t.Run("running instance with progress", func(t *testing.T) {
		// Instance started 1 minute ago with 50% progress
		// Should estimate about 1 minute remaining
		instance := &workflow.WorkflowInstance{
			Status:    workflow.WfInstStatusRunning,
			Progress:  50.0,
			StartedAt: shared.Timestamp(time.Now().Add(-1 * time.Minute)),
		}

		result := calculator.EstimateRemainingTime(instance)

		if result == nil {
			t.Fatal("EstimateRemainingTime() should not return nil for running instance with progress")
		}

		// Allow some tolerance for timing
		if *result < 50 || *result > 70 {
			t.Errorf("EstimateRemainingTime() = %d seconds, expected around 60 seconds", *result)
		}
	})

	t.Run("paused instance", func(t *testing.T) {
		instance := &workflow.WorkflowInstance{
			Status:    workflow.WfInstStatusPaused,
			Progress:  30.0,
			StartedAt: shared.Timestamp(time.Now().Add(-1 * time.Minute)),
		}

		// Paused instance should still calculate remaining time
		result := calculator.EstimateRemainingTime(instance)

		if result == nil {
			t.Fatal("EstimateRemainingTime() should not return nil for paused instance")
		}
	})
}
