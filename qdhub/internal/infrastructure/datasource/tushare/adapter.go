// Package tushare provides Tushare data source adapter implementation.
package tushare

import (
	"qdhub/internal/infrastructure/datasource"
)

// Adapter implements datasource.DataSourceAdapter for Tushare.
type Adapter struct {
	client  *Client
	crawler *Crawler
	parser  *Parser
}

// AdapterOption is a function that configures Adapter.
type AdapterOption func(*Adapter)

// WithClient sets the API client.
func WithAdapterClient(client *Client) AdapterOption {
	return func(a *Adapter) {
		a.client = client
	}
}

// WithAdapterCrawler sets the crawler.
func WithAdapterCrawler(crawler *Crawler) AdapterOption {
	return func(a *Adapter) {
		a.crawler = crawler
	}
}

// WithAdapterParser sets the parser.
func WithAdapterParser(parser *Parser) AdapterOption {
	return func(a *Adapter) {
		a.parser = parser
	}
}

// NewAdapter creates a new Tushare adapter with default components.
func NewAdapter(opts ...AdapterOption) *Adapter {
	a := &Adapter{
		client:  NewClient(),
		crawler: NewCrawler(),
		parser:  NewParser(),
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

// Name returns the data source name.
func (a *Adapter) Name() string {
	return "tushare"
}

// Client returns the API client.
func (a *Adapter) Client() datasource.APIClient {
	return a.client
}

// Crawler returns the document crawler.
func (a *Adapter) Crawler() datasource.Crawler {
	return a.crawler
}

// Parser returns the document parser.
func (a *Adapter) Parser() datasource.DocumentParser {
	return a.parser
}

// SetToken sets the API token on the client.
func (a *Adapter) SetToken(token string) {
	a.client.SetToken(token)
}

// Ensure Adapter implements datasource.DataSourceAdapter
var _ datasource.DataSourceAdapter = (*Adapter)(nil)
