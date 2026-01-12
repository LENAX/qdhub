package sync_test

import (
	"testing"
	"time"

	"qdhub/internal/domain/sync"
)

func TestCronScheduleCalculator_ParseCronExpression(t *testing.T) {
	calculator := sync.NewCronScheduleCalculator()

	tests := []struct {
		name     string
		cronExpr string
		wantErr  bool
	}{
		{
			name:     "valid 5-field cron",
			cronExpr: "0 9 * * *",
			wantErr:  false,
		},
		{
			name:     "valid 6-field cron",
			cronExpr: "0 0 9 * * *",
			wantErr:  false,
		},
		{
			name:     "empty expression",
			cronExpr: "",
			wantErr:  true,
		},
		{
			name:     "whitespace only",
			cronExpr: "   ",
			wantErr:  true,
		},
		{
			name:     "too few fields",
			cronExpr: "0 9 *",
			wantErr:  true,
		},
		{
			name:     "too many fields",
			cronExpr: "0 0 9 * * * * *",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := calculator.ParseCronExpression(tt.cronExpr)

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCronExpression() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCronScheduleCalculator_CalculateNextRunTime(t *testing.T) {
	calculator := sync.NewCronScheduleCalculator()

	t.Run("valid cron expression returns future time", func(t *testing.T) {
		now := time.Now()
		cronExpr := "0 9 * * *" // Every day at 9:00

		nextTime, err := calculator.CalculateNextRunTime(cronExpr, now)

		if err != nil {
			t.Fatalf("CalculateNextRunTime() error = %v", err)
		}

		if nextTime == nil {
			t.Fatal("CalculateNextRunTime() returned nil")
		}

		if !nextTime.After(now) {
			t.Errorf("next run time should be after current time, got %v", nextTime)
		}
	})

	t.Run("invalid cron expression returns error", func(t *testing.T) {
		now := time.Now()
		cronExpr := "invalid"

		_, err := calculator.CalculateNextRunTime(cronExpr, now)

		if err == nil {
			t.Error("CalculateNextRunTime() should return error for invalid cron")
		}
	})

	t.Run("empty cron expression returns error", func(t *testing.T) {
		now := time.Now()

		_, err := calculator.CalculateNextRunTime("", now)

		if err == nil {
			t.Error("CalculateNextRunTime() should return error for empty cron")
		}
	})
}
