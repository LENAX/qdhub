// Package tushare provides Tushare data source adapter implementation.
package tushare

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
)

// Parser implements metadata.DocumentParser for Tushare HTML documentation.
type Parser struct {
	dataSourceID shared.ID
}

// NewParser creates a new Tushare documentation parser.
func NewParser() *Parser {
	return &Parser{}
}

// SetDataSourceID sets the current data source ID.
func (p *Parser) SetDataSourceID(id shared.ID) {
	p.dataSourceID = id
}

// SupportedType returns the supported document type.
func (p *Parser) SupportedType() metadata.DocumentType {
	return metadata.DocumentTypeHTML
}

// ParseCatalog parses the catalog structure from Tushare documentation HTML.
// Returns: category list, API detail page URLs
func (p *Parser) ParseCatalog(content string) ([]metadata.APICategory, []string, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	var categories []metadata.APICategory
	var apiURLs []string
	categoryMap := make(map[string]*metadata.APICategory)

	// Find the sidebar menu (Tushare uses a sidebar for navigation)
	// The sidebar typically contains nested lists of categories and APIs
	doc.Find(".sidebar-menu, .doc-menu, .menu-list, nav.menu").Each(func(i int, menu *goquery.Selection) {
		p.parseMenu(menu, nil, categoryMap, &categories, &apiURLs)
	})

	// If no sidebar found, try to find links in the main content
	if len(apiURLs) == 0 {
		doc.Find("a[href*='/document/']").Each(func(i int, link *goquery.Selection) {
			href, exists := link.Attr("href")
			if exists && href != "" {
				// Filter out non-API links
				if isAPIDocLink(href) {
					apiURLs = append(apiURLs, href)
				}
			}
		})
	}

	return categories, apiURLs, nil
}

// parseMenu recursively parses a menu structure.
func (p *Parser) parseMenu(menu *goquery.Selection, parentID *shared.ID, categoryMap map[string]*metadata.APICategory, categories *[]metadata.APICategory, apiURLs *[]string) {
	menu.ChildrenFiltered("li, .menu-item").Each(func(i int, item *goquery.Selection) {
		// Check if this item has a submenu (category) or is a leaf (API)
		submenu := item.Find("ul, .submenu").First()
		link := item.ChildrenFiltered("a, .menu-link").First()

		if submenu.Length() > 0 {
			// This is a category
			categoryName := strings.TrimSpace(link.Text())
		if categoryName == "" {
			categoryName = strings.TrimSpace(item.ChildrenFiltered("span, .menu-title").First().Text())
		}

			if categoryName != "" {
				category := metadata.NewAPICategory(
					p.dataSourceID,
					categoryName,
					"",
					"",
					parentID,
					i,
				)
				*categories = append(*categories, *category)
				categoryMap[categoryName] = category

				// Recursively parse submenu
				categoryID := category.ID
				p.parseMenu(submenu, &categoryID, categoryMap, categories, apiURLs)
			}
		} else if link.Length() > 0 {
			// This is an API link
			href, exists := link.Attr("href")
			if exists && href != "" && isAPIDocLink(href) {
				*apiURLs = append(*apiURLs, href)
			}
		}
	})
}

// isAPIDocLink checks if a URL is an API documentation link.
func isAPIDocLink(href string) bool {
	// Tushare API doc links typically match patterns like:
	// /document/2?doc_id=xxx
	// /document/xxx
	if strings.Contains(href, "/document/") {
		return true
	}
	return false
}

// ParseAPIDetail parses API detail information from Tushare HTML content.
func (p *Parser) ParseAPIDetail(content string) (*metadata.APIMetadata, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	api := metadata.NewAPIMetadata(p.dataSourceID, "", "", "", "")

	// Parse API name and display name from title/heading
	// Tushare typically has format: "接口名称：xxx" or heading text
	doc.Find("h1, h2, .doc-title, .api-title").Each(func(i int, heading *goquery.Selection) {
		text := strings.TrimSpace(heading.Text())
		if text != "" && api.DisplayName == "" {
			api.DisplayName = text
			// Extract API name (usually in format "接口：api_name")
			if match := regexp.MustCompile(`接口[：:]\s*(\w+)`).FindStringSubmatch(text); len(match) > 1 {
				api.Name = match[1]
			}
		}
	})

	// Parse description
	doc.Find(".doc-description, .api-description, .content > p").First().Each(func(i int, desc *goquery.Selection) {
		api.Description = strings.TrimSpace(desc.Text())
	})

	// Parse API endpoint/name from code blocks or specific elements
	doc.Find("code, .api-name, .endpoint").Each(func(i int, code *goquery.Selection) {
		text := strings.TrimSpace(code.Text())
		// Look for API name pattern (e.g., "pro.query('daily', ...)")
		if match := regexp.MustCompile(`(?:pro\.)?query\s*\(\s*['""](\w+)['""]\s*`).FindStringSubmatch(text); len(match) > 1 {
			if api.Name == "" {
				api.Name = match[1]
			}
		}
		// Also check for direct API name
		if api.Name == "" && regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString(text) {
			api.Name = text
		}
	})

	// Set endpoint (Tushare uses API name as endpoint)
	if api.Name != "" {
		api.Endpoint = api.Name
	}

	// Parse request parameters table
	params := p.parseParamsTable(doc)
	api.SetRequestParams(params)

	// Parse response fields table
	fields := p.parseFieldsTable(doc)
	api.SetResponseFields(fields)

	// Parse rate limit information
	rateLimit := p.parseRateLimit(doc)
	if rateLimit != nil {
		api.SetRateLimit(rateLimit)
	}

	// Parse permission/积分 requirement
	doc.Find(".permission, .points, .require-points").Each(func(i int, perm *goquery.Selection) {
		text := strings.TrimSpace(perm.Text())
		if text != "" {
			api.Permission = text
		}
	})

	return api, nil
}

// parseParamsTable parses the input parameters table.
func (p *Parser) parseParamsTable(doc *goquery.Document) []metadata.ParamMeta {
	var params []metadata.ParamMeta

	// Look for input parameters table
	// Tushare typically labels it as "输入参数" or similar
	doc.Find("table").Each(func(i int, table *goquery.Selection) {
		// Check if this is a parameters table
		prevText := strings.ToLower(table.Prev().Text())
		if !strings.Contains(prevText, "输入") && !strings.Contains(prevText, "参数") && !strings.Contains(prevText, "input") {
			// Also check table caption
			caption := strings.ToLower(table.Find("caption").Text())
			if !strings.Contains(caption, "输入") && !strings.Contains(caption, "参数") {
				return
			}
		}

		// Parse table headers
		var headers []string
		table.Find("thead th, thead td, tr:first-child th, tr:first-child td").Each(func(j int, th *goquery.Selection) {
			headers = append(headers, strings.TrimSpace(strings.ToLower(th.Text())))
		})

		// Map column indices
		nameIdx, typeIdx, requiredIdx, defaultIdx, descIdx := -1, -1, -1, -1, -1
		for idx, header := range headers {
			switch {
			case strings.Contains(header, "名称") || strings.Contains(header, "name"):
				nameIdx = idx
			case strings.Contains(header, "类型") || strings.Contains(header, "type"):
				typeIdx = idx
			case strings.Contains(header, "必选") || strings.Contains(header, "required"):
				requiredIdx = idx
			case strings.Contains(header, "默认") || strings.Contains(header, "default"):
				defaultIdx = idx
			case strings.Contains(header, "描述") || strings.Contains(header, "说明") || strings.Contains(header, "description"):
				descIdx = idx
			}
		}

		// Parse rows
		table.Find("tbody tr, tr").Each(func(rowIdx int, tr *goquery.Selection) {
			if rowIdx == 0 && len(headers) > 0 {
				// Skip header row
				return
			}

			var cells []string
			tr.Find("td").Each(func(cellIdx int, td *goquery.Selection) {
				cells = append(cells, strings.TrimSpace(td.Text()))
			})

			if len(cells) == 0 {
				return
			}

			param := metadata.ParamMeta{}

			if nameIdx >= 0 && nameIdx < len(cells) {
				param.Name = cells[nameIdx]
			}
			if typeIdx >= 0 && typeIdx < len(cells) {
				param.Type = cells[typeIdx]
			}
			if requiredIdx >= 0 && requiredIdx < len(cells) {
				reqText := strings.ToLower(cells[requiredIdx])
				param.Required = reqText == "y" || reqText == "是" || reqText == "true" || reqText == "必选"
			}
			if defaultIdx >= 0 && defaultIdx < len(cells) {
				if cells[defaultIdx] != "" && cells[defaultIdx] != "-" {
					defaultVal := cells[defaultIdx]
					param.Default = &defaultVal
				}
			}
			if descIdx >= 0 && descIdx < len(cells) {
				param.Description = cells[descIdx]
			}

			if param.Name != "" {
				params = append(params, param)
			}
		})
	})

	return params
}

// parseFieldsTable parses the output fields table.
func (p *Parser) parseFieldsTable(doc *goquery.Document) []metadata.FieldMeta {
	var fields []metadata.FieldMeta

	// Look for output fields table
	// Tushare typically labels it as "输出参数" or "输出字段"
	doc.Find("table").Each(func(i int, table *goquery.Selection) {
		// Check if this is a fields table
		prevText := strings.ToLower(table.Prev().Text())
		if !strings.Contains(prevText, "输出") && !strings.Contains(prevText, "返回") && !strings.Contains(prevText, "output") {
			// Also check table caption
			caption := strings.ToLower(table.Find("caption").Text())
			if !strings.Contains(caption, "输出") && !strings.Contains(caption, "返回") {
				return
			}
		}

		// Parse table headers
		var headers []string
		table.Find("thead th, thead td, tr:first-child th, tr:first-child td").Each(func(j int, th *goquery.Selection) {
			headers = append(headers, strings.TrimSpace(strings.ToLower(th.Text())))
		})

		// Map column indices
		nameIdx, typeIdx, descIdx := -1, -1, -1
		for idx, header := range headers {
			switch {
			case strings.Contains(header, "名称") || strings.Contains(header, "name") || strings.Contains(header, "字段"):
				nameIdx = idx
			case strings.Contains(header, "类型") || strings.Contains(header, "type"):
				typeIdx = idx
			case strings.Contains(header, "描述") || strings.Contains(header, "说明") || strings.Contains(header, "description"):
				descIdx = idx
			}
		}

		// Parse rows
		table.Find("tbody tr, tr").Each(func(rowIdx int, tr *goquery.Selection) {
			if rowIdx == 0 && len(headers) > 0 {
				// Skip header row
				return
			}

			var cells []string
			tr.Find("td").Each(func(cellIdx int, td *goquery.Selection) {
				cells = append(cells, strings.TrimSpace(td.Text()))
			})

			if len(cells) == 0 {
				return
			}

			field := metadata.FieldMeta{}

			if nameIdx >= 0 && nameIdx < len(cells) {
				field.Name = cells[nameIdx]
			}
			if typeIdx >= 0 && typeIdx < len(cells) {
				field.Type = cells[typeIdx]
			}
			if descIdx >= 0 && descIdx < len(cells) {
				field.Description = cells[descIdx]
			}

			// Detect primary key fields (common patterns in Tushare)
			if field.Name != "" {
				nameLower := strings.ToLower(field.Name)
				if nameLower == "ts_code" || nameLower == "trade_date" || strings.HasSuffix(nameLower, "_code") {
					field.IsPrimary = true
					field.IsIndex = true
				} else if strings.HasSuffix(nameLower, "_date") || strings.HasSuffix(nameLower, "_time") {
					field.IsIndex = true
				}
			}

			if field.Name != "" {
				fields = append(fields, field)
			}
		})
	})

	return fields
}

// parseRateLimit parses rate limit information from the document.
func (p *Parser) parseRateLimit(doc *goquery.Document) *metadata.RateLimit {
	var rateLimit *metadata.RateLimit

	// Look for rate limit information
	doc.Find(".rate-limit, .limit-info, p, div").Each(func(i int, elem *goquery.Selection) {
		text := strings.ToLower(elem.Text())

		// Look for patterns like "每分钟xxx次" or "xxx requests per minute"
		if match := regexp.MustCompile(`每分钟\s*(\d+)\s*次`).FindStringSubmatch(text); len(match) > 1 {
			if rateLimit == nil {
				rateLimit = &metadata.RateLimit{}
			}
			fmt.Sscanf(match[1], "%d", &rateLimit.RequestsPerMinute)
		}

		// Look for points requirement
		if match := regexp.MustCompile(`(\d+)\s*积分`).FindStringSubmatch(text); len(match) > 1 {
			if rateLimit == nil {
				rateLimit = &metadata.RateLimit{}
			}
			fmt.Sscanf(match[1], "%d", &rateLimit.PointsRequired)
		}
	})

	return rateLimit
}

// Ensure Parser implements metadata.DocumentParser
var _ metadata.DocumentParser = (*Parser)(nil)
