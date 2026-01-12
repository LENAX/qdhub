// Package metadata contains the metadata domain services.
package metadata

import (
	"regexp"

	"qdhub/internal/domain/shared"
)

// ==================== 领域服务接口（纯业务逻辑）====================

// MetadataValidator defines domain service for metadata validation.
// Implementation: metadata/service_impl.go
type MetadataValidator interface {
	// ValidateAPIMetadata validates API metadata completeness and correctness.
	ValidateAPIMetadata(apiMetadata *APIMetadata) error

	// ValidateDataSource validates data source configuration.
	ValidateDataSource(dataSource *DataSource) error

	// ValidateToken validates token configuration.
	ValidateToken(token *Token) error
}

// TypeMappingService defines domain service for type mapping.
// Implementation: metadata/service_impl.go
type TypeMappingService interface {
	// FindBestMatchingRule finds the best matching type mapping rule.
	// Priority: 1. Field pattern match 2. Source type match
	FindBestMatchingRule(rules []*DataTypeMappingRule, fieldName, sourceType string) *DataTypeMappingRule

	// ValidateMappingRule validates a mapping rule.
	ValidateMappingRule(rule *DataTypeMappingRule) error
}

// ==================== 仓储接口 ====================

// DataTypeMappingRuleRepository defines the repository interface for DataTypeMappingRule.
type DataTypeMappingRuleRepository interface {
	// Create creates a new mapping rule.
	Create(rule *DataTypeMappingRule) error

	// Get retrieves a mapping rule by ID.
	Get(id shared.ID) (*DataTypeMappingRule, error)

	// GetBySourceAndTarget retrieves rules by source and target types.
	// Returns rules ordered by priority (descending).
	GetBySourceAndTarget(dataSourceType, targetDBType string) ([]*DataTypeMappingRule, error)

	// SaveBatch saves multiple rules in a batch.
	SaveBatch(rules []*DataTypeMappingRule) error

	// InitDefaultRules initializes default mapping rules.
	InitDefaultRules() error

	// List retrieves all rules.
	List() ([]*DataTypeMappingRule, error)

	// Update updates an existing rule.
	Update(rule *DataTypeMappingRule) error

	// Delete deletes a rule by ID.
	Delete(id shared.ID) error
}

// ==================== 外部依赖接口（领域定义，基础设施实现）====================

// DocumentParser defines the interface for parsing data source documentation.
// Implementation: infrastructure/datasource/parser/
type DocumentParser interface {
	// ParseCatalog parses the catalog structure.
	// Returns: category list, API detail page URLs
	ParseCatalog(content string) ([]APICategory, []string, error)

	// ParseAPIDetail parses API detail information.
	ParseAPIDetail(content string) (*APIMetadata, error)

	// SupportedType returns the supported document type.
	SupportedType() DocumentType
}

// DocumentParserFactory defines the interface for creating document parsers.
// Implementation: infrastructure/datasource/parser/
type DocumentParserFactory interface {
	// GetParser returns a parser for the given document type.
	GetParser(docType DocumentType) (DocumentParser, error)

	// RegisterParser registers a parser.
	RegisterParser(parser DocumentParser)
}

// ==================== 内部工具函数 ====================

// matchFieldPattern checks if field name matches the pattern.
func matchFieldPattern(pattern, fieldName string) bool {
	if pattern == "" {
		return false
	}
	matched, err := regexp.MatchString(pattern, fieldName)
	if err != nil {
		return false
	}
	return matched
}
