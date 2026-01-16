package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/infrastructure/scheduler"
)

// mockJobHandler tracks job executions for testing.
type mockJobHandler struct {
	executedJobs []string
	executeCount int32
	executeCh    chan string
}

func newMockJobHandler() *mockJobHandler {
	return &mockJobHandler{
		executedJobs: make([]string, 0),
		executeCh:    make(chan string, 100),
	}
}

func (h *mockJobHandler) ExecuteScheduledJob(ctx context.Context, jobID string) error {
	atomic.AddInt32(&h.executeCount, 1)
	h.executedJobs = append(h.executedJobs, jobID)
	h.executeCh <- jobID
	return nil
}

func (h *mockJobHandler) GetExecuteCount() int {
	return int(atomic.LoadInt32(&h.executeCount))
}

// ==================== CronParser Tests ====================

func TestCronParser_Parse_ValidExpressions(t *testing.T) {
	parser := scheduler.NewCronParser()

	testCases := []struct {
		name     string
		cronExpr string
	}{
		{"every second", "* * * * * *"},
		{"every minute", "0 * * * * *"},
		{"every hour", "0 0 * * * *"},
		{"every day at 9am", "0 0 9 * * *"},
		{"every monday at 9am", "0 0 9 * * 1"},
		{"every first of month", "0 0 0 1 * *"},
		{"with descriptors", "@hourly"},
		{"with descriptors daily", "@daily"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := parser.Parse(tc.cronExpr)
			assert.NoError(t, err)
		})
	}
}

func TestCronParser_Parse_InvalidExpressions(t *testing.T) {
	parser := scheduler.NewCronParser()

	testCases := []struct {
		name     string
		cronExpr string
	}{
		{"empty", ""},
		{"invalid format", "invalid"},
		{"too few fields", "* * *"},
		{"invalid minute", "60 * * * * *"},
		{"invalid hour", "0 0 25 * * *"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := parser.Parse(tc.cronExpr)
			assert.Error(t, err)
		})
	}
}

func TestCronParser_CalculateNextRunTime(t *testing.T) {
	parser := scheduler.NewCronParser()

	// Test with a specific time
	fromTime := time.Date(2026, 1, 16, 10, 0, 0, 0, time.UTC)

	testCases := []struct {
		name         string
		cronExpr     string
		expectedHour int
	}{
		{"every hour on the hour", "0 0 * * * *", 11},
		{"specific hour", "0 0 12 * * *", 12},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nextTime, err := parser.CalculateNextRunTime(tc.cronExpr, fromTime)
			require.NoError(t, err)
			require.NotNil(t, nextTime)
			assert.Equal(t, tc.expectedHour, nextTime.Hour())
		})
	}
}

func TestCronParser_CalculateNextRunTime_InvalidExpression(t *testing.T) {
	parser := scheduler.NewCronParser()
	fromTime := time.Now()

	nextTime, err := parser.CalculateNextRunTime("invalid", fromTime)
	assert.Error(t, err)
	assert.Nil(t, nextTime)
}

// ==================== CronScheduler Tests ====================

func TestCronScheduler_ScheduleAndUnschedule(t *testing.T) {
	handler := newMockJobHandler()
	sched := scheduler.NewCronScheduler(handler)

	// Schedule a job
	err := sched.ScheduleJob("job-1", "* * * * * *") // every second
	require.NoError(t, err)

	assert.True(t, sched.IsScheduled("job-1"))
	assert.Equal(t, 1, sched.GetScheduledJobCount())

	// Unschedule
	sched.UnscheduleJob("job-1")
	assert.False(t, sched.IsScheduled("job-1"))
	assert.Equal(t, 0, sched.GetScheduledJobCount())
}

func TestCronScheduler_RescheduleJob(t *testing.T) {
	handler := newMockJobHandler()
	sched := scheduler.NewCronScheduler(handler)

	// Schedule a job
	err := sched.ScheduleJob("job-1", "0 0 9 * * *")
	require.NoError(t, err)

	// Reschedule with different expression
	err = sched.ScheduleJob("job-1", "0 0 10 * * *")
	require.NoError(t, err)

	assert.True(t, sched.IsScheduled("job-1"))
	assert.Equal(t, 1, sched.GetScheduledJobCount()) // Should still be 1, not 2
}

func TestCronScheduler_InvalidCronExpression(t *testing.T) {
	handler := newMockJobHandler()
	sched := scheduler.NewCronScheduler(handler)

	err := sched.ScheduleJob("job-1", "invalid")
	assert.Error(t, err)
	assert.False(t, sched.IsScheduled("job-1"))
}

func TestCronScheduler_GetNextRunTime(t *testing.T) {
	handler := newMockJobHandler()
	sched := scheduler.NewCronScheduler(handler)
	sched.Start()
	defer sched.Stop()

	// Schedule a job for every hour
	err := sched.ScheduleJob("job-1", "0 0 * * * *")
	require.NoError(t, err)

	nextTime := sched.GetNextRunTime("job-1")
	require.NotNil(t, nextTime)
	assert.True(t, nextTime.After(time.Now()))

	// Non-existent job
	noNext := sched.GetNextRunTime("non-existent")
	assert.Nil(t, noNext)
}

func TestCronScheduler_UnscheduleNonExistent(t *testing.T) {
	handler := newMockJobHandler()
	sched := scheduler.NewCronScheduler(handler)

	// Should not panic
	sched.UnscheduleJob("non-existent")
}

func TestCronScheduler_MultipleJobs(t *testing.T) {
	handler := newMockJobHandler()
	sched := scheduler.NewCronScheduler(handler)

	// Schedule multiple jobs
	require.NoError(t, sched.ScheduleJob("job-1", "0 0 9 * * *"))
	require.NoError(t, sched.ScheduleJob("job-2", "0 0 10 * * *"))
	require.NoError(t, sched.ScheduleJob("job-3", "0 0 11 * * *"))

	assert.Equal(t, 3, sched.GetScheduledJobCount())
	assert.True(t, sched.IsScheduled("job-1"))
	assert.True(t, sched.IsScheduled("job-2"))
	assert.True(t, sched.IsScheduled("job-3"))

	// Unschedule one
	sched.UnscheduleJob("job-2")
	assert.Equal(t, 2, sched.GetScheduledJobCount())
	assert.False(t, sched.IsScheduled("job-2"))
}

// ==================== CronSchedulerCalculatorAdapter Tests ====================

func TestCronSchedulerCalculatorAdapter_ImplementsInterface(t *testing.T) {
	adapter := scheduler.NewCronSchedulerCalculatorAdapter()

	// Test ParseCronExpression
	err := adapter.ParseCronExpression("0 0 9 * * *")
	assert.NoError(t, err)

	err = adapter.ParseCronExpression("invalid")
	assert.Error(t, err)

	// Test CalculateNextRunTime
	fromTime := time.Now()
	nextTime, err := adapter.CalculateNextRunTime("0 0 * * * *", fromTime)
	require.NoError(t, err)
	require.NotNil(t, nextTime)
	assert.True(t, nextTime.After(fromTime))
}

// ==================== Integration-like Test ====================

func TestCronScheduler_JobExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration-like test in short mode")
	}

	handler := newMockJobHandler()
	sched := scheduler.NewCronScheduler(handler)
	sched.Start()
	defer sched.Stop()

	// Schedule a job to run every second
	err := sched.ScheduleJob("fast-job", "* * * * * *")
	require.NoError(t, err)

	// Wait for at least one execution (with timeout)
	select {
	case jobID := <-handler.executeCh:
		assert.Equal(t, "fast-job", jobID)
	case <-time.After(3 * time.Second):
		t.Fatal("Job was not executed within timeout")
	}

	// Verify execution happened
	assert.GreaterOrEqual(t, handler.GetExecuteCount(), 1)
}
