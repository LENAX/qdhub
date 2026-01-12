package shared_test

import (
	"encoding/json"
	"testing"
	"time"

	"qdhub/internal/domain/shared"
)

func TestNewID(t *testing.T) {
	id1 := shared.NewID()
	id2 := shared.NewID()

	if id1.IsEmpty() {
		t.Error("NewID should not return empty ID")
	}

	if id1 == id2 {
		t.Error("NewID should generate unique IDs")
	}

	if len(id1.String()) != 36 { // UUID format
		t.Errorf("ID should be UUID format, got length %d", len(id1.String()))
	}
}

func TestIDIsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		id       shared.ID
		expected bool
	}{
		{"empty ID", shared.ID(""), true},
		{"non-empty ID", shared.ID("test-id"), false},
		{"generated ID", shared.NewID(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.id.IsEmpty(); got != tt.expected {
				t.Errorf("ID.IsEmpty() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestTimestamp(t *testing.T) {
	t.Run("Now", func(t *testing.T) {
		before := time.Now()
		ts := shared.Now()
		after := time.Now()

		tsTime := ts.ToTime()
		if tsTime.Before(before) || tsTime.After(after) {
			t.Error("Now() should return current time")
		}
	})

	t.Run("IsZero", func(t *testing.T) {
		var zeroTs shared.Timestamp
		if !zeroTs.IsZero() {
			t.Error("zero Timestamp should return true for IsZero")
		}

		nonZeroTs := shared.Now()
		if nonZeroTs.IsZero() {
			t.Error("non-zero Timestamp should return false for IsZero")
		}
	})

	t.Run("JSON marshaling", func(t *testing.T) {
		ts := shared.Timestamp(time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC))

		data, err := json.Marshal(ts)
		if err != nil {
			t.Fatalf("failed to marshal Timestamp: %v", err)
		}

		var unmarshaled shared.Timestamp
		if err := json.Unmarshal(data, &unmarshaled); err != nil {
			t.Fatalf("failed to unmarshal Timestamp: %v", err)
		}

		if !ts.ToTime().Equal(unmarshaled.ToTime()) {
			t.Errorf("JSON round-trip failed: got %v, expected %v", unmarshaled.ToTime(), ts.ToTime())
		}
	})
}

func TestStatus(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		tests := []struct {
			status   shared.Status
			expected string
		}{
			{shared.StatusActive, "active"},
			{shared.StatusInactive, "inactive"},
			{shared.StatusPending, "pending"},
			{shared.StatusFailed, "failed"},
			{shared.StatusSuccess, "success"},
		}

		for _, tt := range tests {
			t.Run(tt.expected, func(t *testing.T) {
				if got := tt.status.String(); got != tt.expected {
					t.Errorf("Status.String() = %v, expected %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("IsValid", func(t *testing.T) {
		validStatuses := []shared.Status{
			shared.StatusActive,
			shared.StatusInactive,
			shared.StatusPending,
			shared.StatusFailed,
			shared.StatusSuccess,
		}

		for _, status := range validStatuses {
			if !status.IsValid() {
				t.Errorf("Status %s should be valid", status)
			}
		}

		invalidStatus := shared.Status("invalid")
		if invalidStatus.IsValid() {
			t.Error("invalid status should return false for IsValid")
		}
	})
}

func TestDomainError(t *testing.T) {
	t.Run("without cause", func(t *testing.T) {
		err := shared.NewDomainError(shared.ErrCodeValidation, "test error", nil)

		if err.Code != shared.ErrCodeValidation {
			t.Errorf("expected code %s, got %s", shared.ErrCodeValidation, err.Code)
		}

		if err.Error() != "test error" {
			t.Errorf("expected message 'test error', got '%s'", err.Error())
		}
	})

	t.Run("with cause", func(t *testing.T) {
		cause := shared.NewDomainError(shared.ErrCodeNotFound, "not found", nil)
		err := shared.NewDomainError(shared.ErrCodeValidation, "validation failed", cause)

		expectedMsg := "validation failed: not found"
		if err.Error() != expectedMsg {
			t.Errorf("expected message '%s', got '%s'", expectedMsg, err.Error())
		}
	})
}
