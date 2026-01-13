// Package parser provides document parser implementations.
package parser

import (
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// HTMLParser implements metadata.DocumentParser for HTML documents.
// This is a base HTML parser that can be extended by specific data source parsers.
type HTMLParser struct {
	// dataSourceID is the current data source being parsed.
	dataSourceID shared.ID
	// catalogSelector is the CSS selector for the catalog container.
	catalogSelector string
	// apiDetailSelector is the CSS selector for the API detail container.
	apiDetailSelector string
}

// HTMLParserOption is a function that configures HTMLParser.
type HTMLParserOption func(*HTMLParser)

// WithCatalogSelector sets the catalog container selector.
func WithCatalogSelector(selector string) HTMLParserOption {
	return func(p *HTMLParser) {
		p.catalogSelector = selector
	}
}

// WithAPIDetailSelector sets the API detail container selector.
func WithAPIDetailSelector(selector string) HTMLParserOption {
	return func(p *HTMLParser) {
		p.apiDetailSelector = selector
	}
}

// NewHTMLParser creates a new HTMLParser with options.
func NewHTMLParser(opts ...HTMLParserOption) *HTMLParser {
	p := &HTMLParser{
		catalogSelector:   "body",
		apiDetailSelector: "body",
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// SetDataSourceID sets the current data source ID.
func (p *HTMLParser) SetDataSourceID(id shared.ID) {
	p.dataSourceID = id
}

// SupportedType returns the supported document type.
func (p *HTMLParser) SupportedType() metadata.DocumentType {
	return metadata.DocumentTypeHTML
}

// ParseCatalog parses the catalog structure from HTML content.
// Returns: category list, API detail page URLs
func (p *HTMLParser) ParseCatalog(content string) ([]metadata.APICategory, []string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	var categories []metadata.APICategory
	var apiURLs []string

	// Find the catalog container
	container := doc.Find(p.catalogSelector)
	if container.Length() == 0 {
		return nil, nil, fmt.Errorf("catalog container not found with selector: %s", p.catalogSelector)
	}

	// Parse categories and API URLs - this is a generic implementation
	// Specific data source parsers should override this method
	container.Find("a").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists && href != "" && !strings.HasPrefix(href, "#") {
			apiURLs = append(apiURLs, href)
		}
	})

	return categories, apiURLs, nil
}

// ParseAPIDetail parses API detail information from HTML content.
func (p *HTMLParser) ParseAPIDetail(content string) (*metadata.APIMetadata, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	// Find the API detail container
	container := doc.Find(p.apiDetailSelector)
	if container.Length() == 0 {
		return nil, fmt.Errorf("API detail container not found with selector: %s", p.apiDetailSelector)
	}

	// Extract API metadata - this is a generic implementation
	// Specific data source parsers should override this method
	api := metadata.NewAPIMetadata(p.dataSourceID, "", "", "", "")

	// Try to find API name from heading
	heading := container.Find("h1, h2, h3").First()
	if heading.Length() > 0 {
		api.DisplayName = strings.TrimSpace(heading.Text())
	}

	// Try to find description from first paragraph
	description := container.Find("p").First()
	if description.Length() > 0 {
		api.Description = strings.TrimSpace(description.Text())
	}

	return api, nil
}

// ParseTable parses a table from HTML content and returns rows as maps.
// This is a utility method for parsing parameter and field tables.
func (p *HTMLParser) ParseTable(content string, tableSelector string) ([]map[string]string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	table := doc.Find(tableSelector)
	if table.Length() == 0 {
		return nil, fmt.Errorf("table not found with selector: %s", tableSelector)
	}

	var rows []map[string]string
	var headers []string

	// Extract headers
	table.Find("thead th, thead td").Each(func(i int, s *goquery.Selection) {
		headers = append(headers, strings.TrimSpace(s.Text()))
	})

	// If no thead, try first row
	if len(headers) == 0 {
		table.Find("tr").First().Find("th, td").Each(func(i int, s *goquery.Selection) {
			headers = append(headers, strings.TrimSpace(s.Text()))
		})
	}

	// Extract data rows
	table.Find("tbody tr, tr").Each(func(rowIdx int, tr *goquery.Selection) {
		// Skip header row if no thead
		if rowIdx == 0 && len(headers) > 0 {
			firstCell := tr.Find("th, td").First()
			if firstCell.Text() == headers[0] {
				return
			}
		}

		row := make(map[string]string)
		tr.Find("td").Each(func(colIdx int, td *goquery.Selection) {
			if colIdx < len(headers) {
				row[headers[colIdx]] = strings.TrimSpace(td.Text())
			}
		})

		if len(row) > 0 {
			rows = append(rows, row)
		}
	})

	return rows, nil
}

// ExtractText extracts text content from a CSS selector.
func (p *HTMLParser) ExtractText(content string, selector string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return "", fmt.Errorf("failed to parse HTML: %w", err)
	}

	element := doc.Find(selector)
	if element.Length() == 0 {
		return "", nil
	}

	return strings.TrimSpace(element.Text()), nil
}

// ExtractAttribute extracts an attribute value from a CSS selector.
func (p *HTMLParser) ExtractAttribute(content string, selector string, attr string) (string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return "", fmt.Errorf("failed to parse HTML: %w", err)
	}

	element := doc.Find(selector)
	if element.Length() == 0 {
		return "", nil
	}

	value, _ := element.Attr(attr)
	return strings.TrimSpace(value), nil
}

// ExtractLinks extracts all links from a CSS selector.
func (p *HTMLParser) ExtractLinks(content string, selector string) ([]string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	var links []string
	doc.Find(selector).Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists && href != "" {
			links = append(links, href)
		}
	})

	return links, nil
}

// Ensure HTMLParser implements metadata.DocumentParser
var _ metadata.DocumentParser = (*HTMLParser)(nil)
