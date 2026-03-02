// Package metadata contains the metadata domain services.
package metadata

import "qdhub/internal/domain/shared"

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

// ==================== 外部依赖接口（领域定义，基础设施实现）====================

// DocumentParser defines the interface for parsing data source documentation.
// Implementation: infrastructure/datasource/parser/
type DocumentParser interface {
	// ParseCatalog parses the catalog structure.
	// Returns: category list, API detail page URLs, and category ID per URL (same length as URLs; nil = no category).
	ParseCatalog(content string) ([]APICategory, []string, []*shared.ID, error)

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
