// Package parser provides document parser implementations.
package parser

import (
	"fmt"
	"sync"

	"qdhub/internal/domain/metadata"
)

// Factory implements metadata.DocumentParserFactory.
type Factory struct {
	mu      sync.RWMutex
	parsers map[metadata.DocumentType]metadata.DocumentParser
}

// NewFactory creates a new parser factory.
func NewFactory() *Factory {
	return &Factory{
		parsers: make(map[metadata.DocumentType]metadata.DocumentParser),
	}
}

// GetParser returns a parser for the given document type.
func (f *Factory) GetParser(docType metadata.DocumentType) (metadata.DocumentParser, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	parser, exists := f.parsers[docType]
	if !exists {
		return nil, fmt.Errorf("parser for document type %s not found", docType)
	}

	return parser, nil
}

// RegisterParser registers a parser for its supported document type.
func (f *Factory) RegisterParser(parser metadata.DocumentParser) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.parsers[parser.SupportedType()] = parser
}

// HasParser checks if a parser is registered for the given document type.
func (f *Factory) HasParser(docType metadata.DocumentType) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	_, exists := f.parsers[docType]
	return exists
}

// ListSupportedTypes returns all supported document types.
func (f *Factory) ListSupportedTypes() []metadata.DocumentType {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]metadata.DocumentType, 0, len(f.parsers))
	for t := range f.parsers {
		types = append(types, t)
	}
	return types
}

// Global factory instance
var globalFactory *Factory
var factoryOnce sync.Once

// GlobalFactory returns the global parser factory instance.
func GlobalFactory() *Factory {
	factoryOnce.Do(func() {
		globalFactory = NewFactory()
	})
	return globalFactory
}

// Ensure Factory implements metadata.DocumentParserFactory
var _ metadata.DocumentParserFactory = (*Factory)(nil)
