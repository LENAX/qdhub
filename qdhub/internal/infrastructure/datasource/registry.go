// Package datasource provides interfaces and implementations for data source adapters.
package datasource

import (
	"fmt"
	"sync"
)

// Registry is a registry for data source adapters.
type Registry struct {
	mu       sync.RWMutex
	adapters map[string]DataSourceAdapter
	clients  map[string]APIClient
	crawlers map[string]Crawler
	parsers  map[string]DocumentParser
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[string]DataSourceAdapter),
		clients:  make(map[string]APIClient),
		crawlers: make(map[string]Crawler),
		parsers:  make(map[string]DocumentParser),
	}
}

// RegisterAdapter registers a data source adapter.
func (r *Registry) RegisterAdapter(adapter DataSourceAdapter) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := adapter.Name()
	if _, exists := r.adapters[name]; exists {
		return fmt.Errorf("adapter %s already registered", name)
	}

	r.adapters[name] = adapter
	r.clients[name] = adapter.Client()
	r.crawlers[name] = adapter.Crawler()
	r.parsers[name] = adapter.Parser()

	return nil
}

// RegisterClient registers an API client.
func (r *Registry) RegisterClient(client APIClient) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := client.Name()
	if _, exists := r.clients[name]; exists {
		return fmt.Errorf("client %s already registered", name)
	}

	r.clients[name] = client
	return nil
}

// RegisterCrawler registers a crawler.
func (r *Registry) RegisterCrawler(crawler Crawler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := crawler.Name()
	if _, exists := r.crawlers[name]; exists {
		return fmt.Errorf("crawler %s already registered", name)
	}

	r.crawlers[name] = crawler
	return nil
}

// RegisterParser registers a document parser.
func (r *Registry) RegisterParser(name string, parser DocumentParser) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.parsers[name]; exists {
		return fmt.Errorf("parser %s already registered", name)
	}

	r.parsers[name] = parser
	return nil
}

// GetAdapter returns a data source adapter by name.
func (r *Registry) GetAdapter(name string) (DataSourceAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	adapter, exists := r.adapters[name]
	if !exists {
		return nil, fmt.Errorf("adapter %s not found", name)
	}

	return adapter, nil
}

// GetClient returns an API client by name.
func (r *Registry) GetClient(name string) (APIClient, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	client, exists := r.clients[name]
	if !exists {
		return nil, fmt.Errorf("client %s not found", name)
	}

	return client, nil
}

// GetCrawler returns a crawler by name.
func (r *Registry) GetCrawler(name string) (Crawler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	crawler, exists := r.crawlers[name]
	if !exists {
		return nil, fmt.Errorf("crawler %s not found", name)
	}

	return crawler, nil
}

// GetParser returns a document parser by name.
func (r *Registry) GetParser(name string) (DocumentParser, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	parser, exists := r.parsers[name]
	if !exists {
		return nil, fmt.Errorf("parser %s not found", name)
	}

	return parser, nil
}

// ListAdapters returns the names of all registered adapters.
func (r *Registry) ListAdapters() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		names = append(names, name)
	}
	return names
}

// ListClients returns the names of all registered clients.
func (r *Registry) ListClients() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.clients))
	for name := range r.clients {
		names = append(names, name)
	}
	return names
}

// Global registry instance
var globalRegistry *Registry
var registryOnce sync.Once

// GlobalRegistry returns the global registry instance.
func GlobalRegistry() *Registry {
	registryOnce.Do(func() {
		globalRegistry = NewRegistry()
	})
	return globalRegistry
}
