// Package typemap provides type mapping utilities for converting
// data source field types to target database column types.
package typemap

import (
	"regexp"
	"sync"

	"qdhub/internal/domain/datastore"
	"qdhub/internal/domain/metadata"
)

// TypeMapper defines the interface for type mapping operations.
type TypeMapper interface {
	// MapFieldType maps a single field type to target database type.
	MapFieldType(field metadata.FieldMeta, dataSourceType string, targetDB datastore.DataStoreType) string

	// MapAllFields maps all fields to column definitions.
	MapAllFields(fields []metadata.FieldMeta, dataSourceType string, targetDB datastore.DataStoreType) []datastore.ColumnDef
}

// RuleBasedTypeMapper implements TypeMapper using DataTypeMappingRule.
type RuleBasedTypeMapper struct {
	rules           []*datastore.DataTypeMappingRule
	typeMappingSvc  datastore.TypeMappingService
	compiledRegexes map[string]*regexp.Regexp
	mu              sync.RWMutex
}

// NewRuleBasedTypeMapper creates a new RuleBasedTypeMapper.
func NewRuleBasedTypeMapper(rules []*datastore.DataTypeMappingRule, typeMappingSvc datastore.TypeMappingService) *RuleBasedTypeMapper {
	mapper := &RuleBasedTypeMapper{
		rules:           rules,
		typeMappingSvc:  typeMappingSvc,
		compiledRegexes: make(map[string]*regexp.Regexp),
	}
	// Pre-compile regex patterns
	mapper.compilePatterns()
	return mapper
}

// compilePatterns pre-compiles all regex patterns from rules.
func (m *RuleBasedTypeMapper) compilePatterns() {
	for _, rule := range m.rules {
		if rule.FieldPattern != nil && *rule.FieldPattern != "" {
			if _, exists := m.compiledRegexes[*rule.FieldPattern]; !exists {
				re, err := regexp.Compile(*rule.FieldPattern)
				if err == nil {
					m.compiledRegexes[*rule.FieldPattern] = re
				}
			}
		}
	}
}

// SetRules updates the mapping rules.
func (m *RuleBasedTypeMapper) SetRules(rules []*datastore.DataTypeMappingRule) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules = rules
	m.compiledRegexes = make(map[string]*regexp.Regexp)
	m.compilePatterns()
}

// MapFieldType maps a single field type to target database type.
func (m *RuleBasedTypeMapper) MapFieldType(field metadata.FieldMeta, dataSourceType string, targetDB datastore.DataStoreType) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Filter rules by data source type and target DB type
	filteredRules := make([]*datastore.DataTypeMappingRule, 0)
	for _, rule := range m.rules {
		if rule.DataSourceType == dataSourceType && rule.TargetDBType == targetDB.String() {
			filteredRules = append(filteredRules, rule)
		}
	}

	// Use TypeMappingService to find best matching rule
	if m.typeMappingSvc != nil {
		bestRule := m.typeMappingSvc.FindBestMatchingRule(filteredRules, field.Name, field.Type)
		if bestRule != nil {
			return bestRule.TargetType
		}
	} else {
		// Fallback: use internal matching logic
		bestRule := m.findBestMatchingRule(filteredRules, field.Name, field.Type)
		if bestRule != nil {
			return bestRule.TargetType
		}
	}

	// Use default type map if no rule matches
	return getDefaultType(field.Type, targetDB)
}

// findBestMatchingRule finds the best matching rule using internal logic.
func (m *RuleBasedTypeMapper) findBestMatchingRule(rules []*datastore.DataTypeMappingRule, fieldName, sourceType string) *datastore.DataTypeMappingRule {
	var bestRule *datastore.DataTypeMappingRule
	var bestPriority int = -1

	// First pass: find rules with field pattern match
	for _, rule := range rules {
		if rule.FieldPattern != nil && *rule.FieldPattern != "" && rule.SourceType == sourceType {
			if m.matchPattern(*rule.FieldPattern, fieldName) {
				if rule.Priority > bestPriority {
					bestRule = rule
					bestPriority = rule.Priority
				}
			}
		}
	}

	// If found pattern match, return it
	if bestRule != nil {
		return bestRule
	}

	// Second pass: find rules by source type only
	bestPriority = -1
	for _, rule := range rules {
		if (rule.FieldPattern == nil || *rule.FieldPattern == "") && rule.SourceType == sourceType {
			if rule.Priority > bestPriority {
				bestRule = rule
				bestPriority = rule.Priority
			}
		}
	}

	return bestRule
}

// matchPattern matches field name against a regex pattern.
func (m *RuleBasedTypeMapper) matchPattern(pattern, fieldName string) bool {
	if re, exists := m.compiledRegexes[pattern]; exists {
		return re.MatchString(fieldName)
	}
	// Compile on demand if not pre-compiled
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(fieldName)
}

// MapAllFields maps all fields to column definitions.
func (m *RuleBasedTypeMapper) MapAllFields(fields []metadata.FieldMeta, dataSourceType string, targetDB datastore.DataStoreType) []datastore.ColumnDef {
	columns := make([]datastore.ColumnDef, len(fields))
	for i, field := range fields {
		columns[i] = datastore.ColumnDef{
			Name:       field.Name,
			SourceType: field.Type,
			TargetType: m.MapFieldType(field, dataSourceType, targetDB),
			Nullable:   !field.IsPrimary,
			Comment:    field.Description,
		}
	}
	return columns
}

// ==================== Default Type Mapper ====================

// DefaultTypeMapper uses built-in type mapping without database rules.
type DefaultTypeMapper struct{}

// NewDefaultTypeMapper creates a new DefaultTypeMapper.
func NewDefaultTypeMapper() *DefaultTypeMapper {
	return &DefaultTypeMapper{}
}

// MapFieldType maps a single field type to target database type using defaults.
func (m *DefaultTypeMapper) MapFieldType(field metadata.FieldMeta, dataSourceType string, targetDB datastore.DataStoreType) string {
	// Check field pattern rules first
	targetType := matchFieldPatternRules(field.Name, targetDB)
	if targetType != "" {
		return targetType
	}

	// Use default type mapping
	return getDefaultType(field.Type, targetDB)
}

// MapAllFields maps all fields to column definitions.
func (m *DefaultTypeMapper) MapAllFields(fields []metadata.FieldMeta, dataSourceType string, targetDB datastore.DataStoreType) []datastore.ColumnDef {
	columns := make([]datastore.ColumnDef, len(fields))
	for i, field := range fields {
		columns[i] = datastore.ColumnDef{
			Name:       field.Name,
			SourceType: field.Type,
			TargetType: m.MapFieldType(field, dataSourceType, targetDB),
			Nullable:   !field.IsPrimary,
			Comment:    field.Description,
		}
	}
	return columns
}

// ==================== Type Mapping Tables ====================

// defaultTypeMap defines default type mappings from source types to target database types.
var defaultTypeMap = map[string]map[datastore.DataStoreType]string{
	"str": {
		datastore.DataStoreTypeDuckDB:     "VARCHAR",
		datastore.DataStoreTypeClickHouse: "String",
		datastore.DataStoreTypePostgreSQL: "VARCHAR",
	},
	"string": {
		datastore.DataStoreTypeDuckDB:     "VARCHAR",
		datastore.DataStoreTypeClickHouse: "String",
		datastore.DataStoreTypePostgreSQL: "VARCHAR",
	},
	"float": {
		datastore.DataStoreTypeDuckDB:     "DOUBLE",
		datastore.DataStoreTypeClickHouse: "Float64",
		datastore.DataStoreTypePostgreSQL: "DOUBLE PRECISION",
	},
	"float64": {
		datastore.DataStoreTypeDuckDB:     "DOUBLE",
		datastore.DataStoreTypeClickHouse: "Float64",
		datastore.DataStoreTypePostgreSQL: "DOUBLE PRECISION",
	},
	"int": {
		datastore.DataStoreTypeDuckDB:     "BIGINT",
		datastore.DataStoreTypeClickHouse: "Int64",
		datastore.DataStoreTypePostgreSQL: "BIGINT",
	},
	"int64": {
		datastore.DataStoreTypeDuckDB:     "BIGINT",
		datastore.DataStoreTypeClickHouse: "Int64",
		datastore.DataStoreTypePostgreSQL: "BIGINT",
	},
	"int32": {
		datastore.DataStoreTypeDuckDB:     "INTEGER",
		datastore.DataStoreTypeClickHouse: "Int32",
		datastore.DataStoreTypePostgreSQL: "INTEGER",
	},
	"date": {
		datastore.DataStoreTypeDuckDB:     "DATE",
		datastore.DataStoreTypeClickHouse: "Date",
		datastore.DataStoreTypePostgreSQL: "DATE",
	},
	"datetime": {
		datastore.DataStoreTypeDuckDB:     "TIMESTAMP",
		datastore.DataStoreTypeClickHouse: "DateTime",
		datastore.DataStoreTypePostgreSQL: "TIMESTAMP",
	},
	"bool": {
		datastore.DataStoreTypeDuckDB:     "BOOLEAN",
		datastore.DataStoreTypeClickHouse: "Bool",
		datastore.DataStoreTypePostgreSQL: "BOOLEAN",
	},
	"boolean": {
		datastore.DataStoreTypeDuckDB:     "BOOLEAN",
		datastore.DataStoreTypeClickHouse: "Bool",
		datastore.DataStoreTypePostgreSQL: "BOOLEAN",
	},
}

// fieldPatternRules defines special field patterns and their target types.
var fieldPatternRules = []struct {
	Pattern *regexp.Regexp
	TypeMap map[datastore.DataStoreType]string
}{
	{
		Pattern: regexp.MustCompile(`^(ts_code|.*_code)$`),
		TypeMap: map[datastore.DataStoreType]string{
			datastore.DataStoreTypeDuckDB:     "VARCHAR(16)",
			datastore.DataStoreTypeClickHouse: "FixedString(16)",
			datastore.DataStoreTypePostgreSQL: "VARCHAR(16)",
		},
	},
	{
		Pattern: regexp.MustCompile(`^(.*_date|trade_date|ann_date|end_date)$`),
		TypeMap: map[datastore.DataStoreType]string{
			datastore.DataStoreTypeDuckDB:     "DATE",
			datastore.DataStoreTypeClickHouse: "Date",
			datastore.DataStoreTypePostgreSQL: "DATE",
		},
	},
	{
		Pattern: regexp.MustCompile(`^(vol|amount|.*_vol|.*_amount)$`),
		TypeMap: map[datastore.DataStoreType]string{
			datastore.DataStoreTypeDuckDB:     "DECIMAL(20,2)",
			datastore.DataStoreTypeClickHouse: "Decimal(20,2)",
			datastore.DataStoreTypePostgreSQL: "DECIMAL(20,2)",
		},
	},
	{
		Pattern: regexp.MustCompile(`^(pct_.*|.*_pct|.*_rate|.*_ratio)$`),
		TypeMap: map[datastore.DataStoreType]string{
			datastore.DataStoreTypeDuckDB:     "DECIMAL(10,4)",
			datastore.DataStoreTypeClickHouse: "Decimal(10,4)",
			datastore.DataStoreTypePostgreSQL: "DECIMAL(10,4)",
		},
	},
	{
		Pattern: regexp.MustCompile(`^(open|high|low|close|pre_close|.*_price)$`),
		TypeMap: map[datastore.DataStoreType]string{
			datastore.DataStoreTypeDuckDB:     "DECIMAL(10,2)",
			datastore.DataStoreTypeClickHouse: "Decimal(10,2)",
			datastore.DataStoreTypePostgreSQL: "DECIMAL(10,2)",
		},
	},
}

// matchFieldPatternRules checks if field name matches any pattern rule.
func matchFieldPatternRules(fieldName string, targetDB datastore.DataStoreType) string {
	for _, rule := range fieldPatternRules {
		if rule.Pattern.MatchString(fieldName) {
			if t, ok := rule.TypeMap[targetDB]; ok {
				return t
			}
		}
	}
	return ""
}

// getDefaultType returns the default target type for a source type.
func getDefaultType(sourceType string, targetDB datastore.DataStoreType) string {
	if typeMap, ok := defaultTypeMap[sourceType]; ok {
		if t, ok := typeMap[targetDB]; ok {
			return t
		}
	}
	// Fallback to VARCHAR
	switch targetDB {
	case datastore.DataStoreTypeClickHouse:
		return "String"
	default:
		return "VARCHAR"
	}
}

// ==================== Factory ====================

// NewTypeMapper creates a TypeMapper based on the configuration.
// If rules are provided, it creates a RuleBasedTypeMapper.
// Otherwise, it creates a DefaultTypeMapper.
func NewTypeMapper(rules []*datastore.DataTypeMappingRule, typeMappingSvc datastore.TypeMappingService) TypeMapper {
	if len(rules) > 0 {
		return NewRuleBasedTypeMapper(rules, typeMappingSvc)
	}
	return NewDefaultTypeMapper()
}

// Ensure implementations satisfy the TypeMapper interface
var (
	_ TypeMapper = (*RuleBasedTypeMapper)(nil)
	_ TypeMapper = (*DefaultTypeMapper)(nil)
)
