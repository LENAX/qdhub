package datastore_test

import (
	"testing"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/shared"
)

func TestTypeMappingService_FindBestMatchingRule(t *testing.T) {
	service := datastore.NewTypeMappingService()

	// Helper to create rules
	createRule := func(sourceType, fieldPattern string, priority int) *datastore.DataTypeMappingRule {
		rule := &datastore.DataTypeMappingRule{
			ID:             shared.NewID(),
			DataSourceType: "tushare",
			SourceType:     sourceType,
			TargetDBType:   "duckdb",
			TargetType:     "VARCHAR",
			Priority:       priority,
		}
		if fieldPattern != "" {
			rule.FieldPattern = &fieldPattern
		}
		return rule
	}

	tests := []struct {
		name       string
		rules      []*datastore.DataTypeMappingRule
		fieldName  string
		sourceType string
		wantNil    bool
		wantPrio   int
	}{
		{
			name:       "empty rules",
			rules:      []*datastore.DataTypeMappingRule{},
			fieldName:  "ts_code",
			sourceType: "str",
			wantNil:    true,
		},
		{
			name: "match by source type only",
			rules: []*datastore.DataTypeMappingRule{
				createRule("str", "", 10),
				createRule("int", "", 10),
			},
			fieldName:  "ts_code",
			sourceType: "str",
			wantNil:    false,
			wantPrio:   10,
		},
		{
			name: "prefer higher priority",
			rules: []*datastore.DataTypeMappingRule{
				createRule("str", "", 10),
				createRule("str", "", 20),
			},
			fieldName:  "ts_code",
			sourceType: "str",
			wantNil:    false,
			wantPrio:   20,
		},
		{
			name: "prefer field pattern match over source type match",
			rules: []*datastore.DataTypeMappingRule{
				createRule("str", "", 20),     // Higher priority but no pattern
				createRule("str", "^ts_", 10), // Lower priority but matches pattern
			},
			fieldName:  "ts_code",
			sourceType: "str",
			wantNil:    false,
			wantPrio:   10, // Pattern match wins
		},
		{
			name: "field pattern must match source type too",
			rules: []*datastore.DataTypeMappingRule{
				createRule("int", "^ts_", 20), // Pattern matches but source type doesn't
				createRule("str", "", 10),     // Source type matches
			},
			fieldName:  "ts_code",
			sourceType: "str",
			wantNil:    false,
			wantPrio:   10,
		},
		{
			name: "multiple pattern rules choose highest priority",
			rules: []*datastore.DataTypeMappingRule{
				createRule("str", "^ts_", 10),
				createRule("str", "^ts_code$", 30),
				createRule("str", "code$", 20),
			},
			fieldName:  "ts_code",
			sourceType: "str",
			wantNil:    false,
			wantPrio:   30,
		},
		{
			name: "no match returns nil",
			rules: []*datastore.DataTypeMappingRule{
				createRule("int", "", 10),
				createRule("float", "", 10),
			},
			fieldName:  "ts_code",
			sourceType: "str",
			wantNil:    true,
		},
		{
			name: "regex pattern match",
			rules: []*datastore.DataTypeMappingRule{
				createRule("str", ".*_date$", 20),
			},
			fieldName:  "trade_date",
			sourceType: "str",
			wantNil:    false,
			wantPrio:   20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := service.FindBestMatchingRule(tt.rules, tt.fieldName, tt.sourceType)

			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got rule with priority %d", result.Priority)
				}
				return
			}

			if result == nil {
				t.Fatal("expected non-nil result, got nil")
			}

			if result.Priority != tt.wantPrio {
				t.Errorf("expected priority %d, got %d", tt.wantPrio, result.Priority)
			}
		})
	}
}

func TestTypeMappingService_ValidateMappingRule(t *testing.T) {
	service := datastore.NewTypeMappingService()

	validRule := func() *datastore.DataTypeMappingRule {
		pattern := "^ts_"
		return &datastore.DataTypeMappingRule{
			ID:             shared.NewID(),
			DataSourceType: "tushare",
			SourceType:     "str",
			TargetDBType:   "duckdb",
			TargetType:     "VARCHAR",
			FieldPattern:   &pattern,
			Priority:       10,
		}
	}

	tests := []struct {
		name    string
		modify  func(*datastore.DataTypeMappingRule)
		wantErr bool
	}{
		{
			name:    "valid rule",
			modify:  func(r *datastore.DataTypeMappingRule) {},
			wantErr: false,
		},
		{
			name:    "nil rule",
			modify:  func(r *datastore.DataTypeMappingRule) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(r *datastore.DataTypeMappingRule) { r.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty data source type",
			modify:  func(r *datastore.DataTypeMappingRule) { r.DataSourceType = "" },
			wantErr: true,
		},
		{
			name:    "empty source type",
			modify:  func(r *datastore.DataTypeMappingRule) { r.SourceType = "" },
			wantErr: true,
		},
		{
			name:    "empty target DB type",
			modify:  func(r *datastore.DataTypeMappingRule) { r.TargetDBType = "" },
			wantErr: true,
		},
		{
			name:    "empty target type",
			modify:  func(r *datastore.DataTypeMappingRule) { r.TargetType = "" },
			wantErr: true,
		},
		{
			name:    "negative priority",
			modify:  func(r *datastore.DataTypeMappingRule) { r.Priority = -1 },
			wantErr: true,
		},
		{
			name:    "zero priority is valid",
			modify:  func(r *datastore.DataTypeMappingRule) { r.Priority = 0 },
			wantErr: false,
		},
		{
			name: "invalid regex pattern",
			modify: func(r *datastore.DataTypeMappingRule) {
				invalid := "[invalid"
				r.FieldPattern = &invalid
			},
			wantErr: true,
		},
		{
			name:    "nil field pattern is valid",
			modify:  func(r *datastore.DataTypeMappingRule) { r.FieldPattern = nil },
			wantErr: false,
		},
		{
			name: "empty field pattern is valid",
			modify: func(r *datastore.DataTypeMappingRule) {
				empty := ""
				r.FieldPattern = &empty
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rule *datastore.DataTypeMappingRule
			if tt.name == "nil rule" {
				rule = nil
			} else {
				rule = validRule()
				tt.modify(rule)
			}

			err := service.ValidateMappingRule(rule)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMappingRule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
