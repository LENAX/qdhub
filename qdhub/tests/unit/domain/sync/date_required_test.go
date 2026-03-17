package sync_test

import (
	"testing"

	"qdhub/internal/domain/sync"
)

func TestPlanRequiresDateRange(t *testing.T) {
	tests := []struct {
		name            string
		apiNames        []string
		paramNamesByAPI map[string][]string
		want            bool
	}{
		{
			name:     "stock_basic no date params",
			apiNames: []string{"stock_basic"},
			paramNamesByAPI: map[string][]string{
				"stock_basic": {"ts_code", "name", "market", "list_date"},
			},
			want: false,
		},
		{
			name:     "daily has trade_date",
			apiNames: []string{"daily"},
			paramNamesByAPI: map[string][]string{
				"daily": {"ts_code", "trade_date", "start_date", "end_date"},
			},
			want: true,
		},
		{
			name:     "multiple APIs one has date",
			apiNames: []string{"stock_basic", "daily"},
			paramNamesByAPI: map[string][]string{
				"stock_basic": {"ts_code", "name"},
				"daily":       {"ts_code", "trade_date"},
			},
			want: true,
		},
		{
			name:     "param name contains time",
			apiNames: []string{"some_api"},
			paramNamesByAPI: map[string][]string{
				"some_api": {"start_time", "end_time"},
			},
			want: true,
		},
		{
			name:     "param name contains _dt",
			apiNames: []string{"some_api"},
			paramNamesByAPI: map[string][]string{
				"some_api": {"start_dt", "end_dt"},
			},
			want: true,
		},
		{
			name:            "empty api names",
			apiNames:        []string{},
			paramNamesByAPI: map[string][]string{"x": {"trade_date"}},
			want:            false,
		},
		{
			name:     "API not in map treated as no params",
			apiNames: []string{"unknown_api"},
			paramNamesByAPI: map[string][]string{
				"other": {"trade_date"},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sync.PlanRequiresDateRange(tt.apiNames, tt.paramNamesByAPI)
			if got != tt.want {
				t.Errorf("PlanRequiresDateRange() = %v, want %v", got, tt.want)
			}
		})
	}
}
