//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/LENAX/task-engine/pkg/core/task"

	"qdhub/internal/infrastructure/taskengine/jobs"
)

func TestStkMinsGenerateDatetimeRangeBounds_DateOnly(t *testing.T) {
	s, e, err := jobs.StkMinsGenerateDatetimeRangeBounds("20190101", "20260320")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if s != "2019-01-01 09:30:00" {
		t.Errorf("start: want 2019-01-01 09:30:00, got %q", s)
	}
	if e != "2026-03-20 15:00:00" {
		t.Errorf("end: want 2026-03-20 15:00:00, got %q", e)
	}
}

func TestStkMinsGenerateDatetimeRangeStepSpan(t *testing.T) {
	start, end, err := jobs.StkMinsGenerateDatetimeRangeStepSpan("20190101", "20260320")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if start != "2019-01-01 00:00:00" {
		t.Errorf("step start: got %q", start)
	}
	if end != "2026-03-21 00:00:00" {
		t.Errorf("step end exclusive: want 2026-03-21 00:00:00, got %q", end)
	}
}

func TestStkMinsAPIRangeFromHalfOpenWindow(t *testing.T) {
	a, b := jobs.StkMinsAPIRangeFromHalfOpenWindow("2019-01-01 00:00:00", "2019-01-31 00:00:00")
	if a != "2019-01-01 09:30:00" {
		t.Errorf("api start: got %q", a)
	}
	if b != "2019-01-30 15:00:00" {
		t.Errorf("api end: want 2019-01-30 15:00:00, got %q", b)
	}
}

func TestGenerateDatetimeRangeJob_30DWindows(t *testing.T) {
	tc := task.NewTaskContext(
		context.Background(),
		"id", "t", "wf", "wfi",
		map[string]interface{}{
			"start":      "2019-01-01 00:00:00",
			"end":        "2019-03-20 00:00:00",
			"freq":       "30D",
			"inclusive":  "both",
			"as_windows": true,
		},
	)
	out, err := jobs.GenerateDatetimeRangeJob(tc)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	m, ok := out.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", out)
	}
	raw, ok := m["windows"]
	if !ok || raw == nil {
		t.Fatalf("expected windows in result")
	}
	arr, ok := raw.([]map[string]string)
	if !ok {
		t.Fatalf("expected []map[string]string windows, got %T", raw)
	}
	if len(arr) < 2 {
		t.Fatalf("expected at least 2 windows for ~80d span, got %d", len(arr))
	}
	if arr[0]["start"] == "" || arr[0]["end"] == "" {
		t.Fatalf("empty first window: %+v", arr[0])
	}
}

func TestParseGoMapString_DatetimeValuesWithSpaces(t *testing.T) {
	raw := "map[end_date:2025-11-30 15:00:00 freq:1min start_date:2025-11-01 09:30:00 ts_code:000027.SZ]"
	got := jobs.ParseGoMapString(raw)
	if got["end_date"] != "2025-11-30 15:00:00" {
		t.Errorf("end_date: want %q, got %v", "2025-11-30 15:00:00", got["end_date"])
	}
	if got["start_date"] != "2025-11-01 09:30:00" {
		t.Errorf("start_date: want %q, got %v", "2025-11-01 09:30:00", got["start_date"])
	}
	if got["freq"] != "1min" || got["ts_code"] != "000027.SZ" {
		t.Errorf("freq/ts_code: got %#v", got)
	}
	if _, bad := got["09"]; bad {
		t.Errorf("should not parse stray time fragment as key 09: %#v", got)
	}
	if _, bad := got["15"]; bad {
		t.Errorf("should not parse stray time fragment as key 15: %#v", got)
	}
}

func TestParseGoMapString_SimplePairs(t *testing.T) {
	got := jobs.ParseGoMapString("map[end_date:20260121 start_date:20260114]")
	if got["end_date"] != "20260121" || got["start_date"] != "20260114" {
		t.Errorf("got %#v", got)
	}
}

func TestExtractTimeWindowsFromUpstream(t *testing.T) {
	tc := task.NewTaskContext(
		context.Background(),
		"id", "t", "wf", "wfi",
		map[string]interface{}{
			"_cached_GenRange": map[string]interface{}{
				"extracted_data": map[string]interface{}{
					"windows": []map[string]interface{}{
						{"start": "2020-01-01 00:00:00", "end": "2020-01-31 00:00:00"},
						{"start": "2020-01-31 00:00:00", "end": "2020-03-01 00:00:00"},
					},
				},
			},
		},
	)
	got := jobs.ExtractTimeWindowsFromUpstream(tc, "GenRange", "windows")
	if len(got) != 2 {
		t.Fatalf("want 2 windows, got %d", len(got))
	}
	if got[0].Start != "2020-01-01 00:00:00" || got[0].End != "2020-01-31 00:00:00" {
		t.Errorf("window0: %+v", got[0])
	}
}
