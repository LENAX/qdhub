// Package tushare provides Tushare data source adapter implementation.
package tushare

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/datasource"
)

const (
	// DefaultDocURL is the default Tushare documentation URL.
	DefaultDocURL = "https://tushare.pro/document/2"
	// DefaultCrawlerTimeout is the default crawler timeout in seconds (metadata crawl 等单次请求可能较慢).
	DefaultCrawlerTimeout = 60
)

// Crawler implements datasource.Crawler for Tushare documentation.
// Note: Rate limiting is handled by Task Engine's concurrency control,
// not by the Crawler itself. This allows for true parallel execution
// of sub-tasks while respecting overall system concurrency limits.
type Crawler struct {
	docURL     string
	httpClient *http.Client
}

// CrawlerOption is a function that configures Crawler.
type CrawlerOption func(*Crawler)

// WithDocURL sets the documentation URL.
func WithDocURL(url string) CrawlerOption {
	return func(c *Crawler) {
		c.docURL = url
	}
}

// WithCrawlerTimeout sets the crawler timeout.
func WithCrawlerTimeout(timeout time.Duration) CrawlerOption {
	return func(c *Crawler) {
		c.httpClient.Timeout = timeout
	}
}

// NewCrawler creates a new Tushare documentation crawler.
func NewCrawler(opts ...CrawlerOption) *Crawler {
	c := &Crawler{
		docURL: DefaultDocURL,
		httpClient: &http.Client{
			Timeout: DefaultCrawlerTimeout * time.Second,
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Name returns the data source name.
func (c *Crawler) Name() string {
	return "tushare"
}

// FetchCatalogPage fetches the catalog page content.
func (c *Crawler) FetchCatalogPage(ctx context.Context, dataSourceID shared.ID) (string, metadata.DocumentType, error) {
	// Fetch the main documentation page
	content, err := c.fetchPage(ctx, c.docURL)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch catalog page: %w", err)
	}

	return content, metadata.DocumentTypeHTML, nil
}

// FetchAPIDetailPage fetches an API detail page content.
func (c *Crawler) FetchAPIDetailPage(ctx context.Context, apiURL string) (string, metadata.DocumentType, error) {
	// Resolve relative URL
	fullURL, err := c.resolveURL(apiURL)
	if err != nil {
		return "", "", fmt.Errorf("failed to resolve URL: %w", err)
	}

	// Fetch the API detail page
	content, err := c.fetchPage(ctx, fullURL)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch API detail page: %w", err)
	}

	return content, metadata.DocumentTypeHTML, nil
}

// fetchPage fetches a web page and returns its content.
func (c *Crawler) fetchPage(ctx context.Context, pageURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers to mimic a browser
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", datasource.NewDataSourceError(
			datasource.ErrCodeNetworkError,
			"network error",
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", datasource.NewDataSourceError(
			datasource.ErrCodeNetworkError,
			fmt.Sprintf("unexpected status code: %d", resp.StatusCode),
			nil,
		)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	return string(body), nil
}

// resolveURL resolves a relative URL against the documentation base URL.
func (c *Crawler) resolveURL(relativeURL string) (string, error) {
	// If it's already an absolute URL, return it
	if strings.HasPrefix(relativeURL, "http://") || strings.HasPrefix(relativeURL, "https://") {
		return relativeURL, nil
	}

	base, err := url.Parse(c.docURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse base URL: %w", err)
	}

	ref, err := url.Parse(relativeURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse relative URL: %w", err)
	}

	return base.ResolveReference(ref).String(), nil
}

// GetDocURL returns the documentation URL.
func (c *Crawler) GetDocURL() string {
	return c.docURL
}

// Ensure Crawler implements datasource.Crawler
var _ datasource.Crawler = (*Crawler)(nil)
