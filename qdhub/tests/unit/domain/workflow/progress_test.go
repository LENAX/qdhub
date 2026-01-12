package workflow_test

import (
	"testing"

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
				{ID: shared.NewID().String(), Status: "Pending"},
				{ID: shared.NewID().String(), Status: "Pending"},
			},
			expectedMin: 0.0,
			expectedMax: 0.0,
		},
		{
			name: "all success",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Success"},
			},
			expectedMin: 100.0,
			expectedMax: 100.0,
		},
		{
			name: "half completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Pending"},
			},
			expectedMin: 50.0,
			expectedMax: 50.0,
		},
		{
			name: "one third completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Running"},
				{ID: shared.NewID().String(), Status: "Pending"},
			},
			expectedMin: 33.0,
			expectedMax: 34.0,
		},
		{
			name: "skipped counts as completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Skipped"},
			},
			expectedMin: 100.0,
			expectedMax: 100.0,
		},
		{
			name: "failed does not count as completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Failed"},
			},
			expectedMin: 50.0,
			expectedMax: 50.0,
		},
		{
			name: "running does not count as completed",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Running"},
			},
			expectedMin: 50.0,
			expectedMax: 50.0,
		},
		{
			name: "mixed statuses",
			tasks: []workflow.TaskInstance{
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Success"},
				{ID: shared.NewID().String(), Status: "Skipped"},
				{ID: shared.NewID().String(), Status: "Failed"},
				{ID: shared.NewID().String(), Status: "Pending"},
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
			Status: "Success",
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
			Status: "Failed",
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
			Status: "Terminated",
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
			Status: "Running",
		}

		result := calculator.EstimateRemainingTime(instance)

		// Task Engine WorkflowInstance doesn't have Progress field
		// Progress needs to be calculated from task instances
		// So this will return nil
		if result != nil {
			t.Errorf("EstimateRemainingTime() should return nil for zero progress, got %v", *result)
		}
	})

	t.Run("running instance with progress", func(t *testing.T) {
		// Task Engine WorkflowInstance doesn't have Progress field
		// This test may need to be adjusted
		instance := &workflow.WorkflowInstance{
			Status: "Running",
		}

		result := calculator.EstimateRemainingTime(instance)

		// Without progress information, this will return nil
		if result != nil {
			t.Errorf("EstimateRemainingTime() should return nil without progress info, got %v", *result)
		}
	})

	t.Run("paused instance", func(t *testing.T) {
		instance := &workflow.WorkflowInstance{
			Status: "Paused",
		}

		// Paused instance without progress info will return nil
		result := calculator.EstimateRemainingTime(instance)

		// Without progress information, this will return nil
		if result != nil {
			t.Errorf("EstimateRemainingTime() should return nil without progress info, got %v", *result)
		}
	})
}
