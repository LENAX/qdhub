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
// Returns: category list, API detail page URLs, and category ID per URL (same length; nil = no category).
func (p *Parser) ParseCatalog(content string) ([]metadata.APICategory, []string, []*shared.ID, error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(content))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse HTML: %w", err)
	}

	var categories []metadata.APICategory
	var apiURLs []string
	var apiCategoryIDs []*shared.ID
	categoryMap := make(map[string]*metadata.APICategory)

	// Tushare navigation structure: #document > div > div > nav
	nav := doc.Find("#document > div > div > nav")
	if nav.Length() == 0 {
		nav = doc.Find("nav")
	}

	nav.Find("ul").First().Each(func(i int, menu *goquery.Selection) {
		p.parseMenu(menu, nil, categoryMap, &categories, &apiURLs, &apiCategoryIDs)
	})

	if len(apiURLs) == 0 {
		nav.Find("li > a").Each(func(i int, link *goquery.Selection) {
			href, exists := link.Attr("href")
			if exists && href != "" && isAPIDocLink(href) {
				apiURLs = append(apiURLs, href)
				apiCategoryIDs = append(apiCategoryIDs, nil)
			}
		})
	}

	if len(apiURLs) == 0 {
		doc.Find("a[href*='/document/']").Each(func(i int, link *goquery.Selection) {
			href, exists := link.Attr("href")
			if exists && href != "" && isAPIDocLink(href) {
				apiURLs = append(apiURLs, href)
				apiCategoryIDs = append(apiCategoryIDs, nil)
			}
		})
	}

	return categories, apiURLs, apiCategoryIDs, nil
}

// parseMenu recursively parses a menu structure. parentID is the current category (nil at top level).
func (p *Parser) parseMenu(menu *goquery.Selection, parentID *shared.ID, categoryMap map[string]*metadata.APICategory, categories *[]metadata.APICategory, apiURLs *[]string, apiCategoryIDs *[]*shared.ID) {
	menu.ChildrenFiltered("li, .menu-item").Each(func(i int, item *goquery.Selection) {
		submenu := item.Find("ul, .submenu").First()
		link := item.ChildrenFiltered("a, .menu-link").First()

		if submenu.Length() > 0 {
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

				categoryID := category.ID
				p.parseMenu(submenu, &categoryID, categoryMap, categories, apiURLs, apiCategoryIDs)
			}
		} else if link.Length() > 0 {
			href, exists := link.Attr("href")
			if exists && href != "" && isAPIDocLink(href) {
				*apiURLs = append(*apiURLs, href)
				*apiCategoryIDs = append(*apiCategoryIDs, parentID)
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
	// Tushare page title format: "API显示名称"
	doc.Find("h1, h2, .doc-title, .api-title").Each(func(i int, heading *goquery.Selection) {
		text := strings.TrimSpace(heading.Text())
		if text != "" && api.DisplayName == "" {
			api.DisplayName = text
		}
	})

	// Parse the main content area for API info
	// Tushare format: "接口：api_name，可以通过数据工具调试和查看数据。描述：xxx权限：xxx积分起提示：xxx"
	doc.Find(".content, .doc-content, article, #document").Each(func(i int, content *goquery.Selection) {
		text := content.Text()
		p.parseAPIInfoFromText(text, api)
	})

	// Also try parsing from first paragraph
	if api.Name == "" {
		doc.Find("p").Each(func(i int, para *goquery.Selection) {
			if api.Name == "" {
				text := strings.TrimSpace(para.Text())
				p.parseAPIInfoFromText(text, api)
			}
		})
	}

	// Parse API endpoint/name from code blocks or specific elements
	doc.Find("code, .api-name, .endpoint").Each(func(i int, code *goquery.Selection) {
		text := strings.TrimSpace(code.Text())
		// Look for API name pattern (e.g., "pro.query('daily', ...)")
		if match := regexp.MustCompile(`(?:pro\.)?query\s*\(\s*['"](\w+)['"]\s*`).FindStringSubmatch(text); len(match) > 1 {
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

// parseAPIInfoFromText extracts API name, description, and permission from Tushare text.
// Tushare format: "接口：api_name，可以通过数据工具调试和查看数据。描述：xxx权限：xxx积分起提示：xxx"
// Or: "接口：api_name 描述：xxx权限：xxx"
func (p *Parser) parseAPIInfoFromText(text string, api *metadata.APIMetadata) {
	// Extract API name: "接口：api_name" or "接口：api_name，"
	if api.Name == "" {
		if match := regexp.MustCompile(`接口[：:]\s*([a-zA-Z_][a-zA-Z0-9_]*)`).FindStringSubmatch(text); len(match) > 1 {
			api.Name = match[1]
		}
	}

	// Extract description: "描述：xxx" until next field or end
	// Look for pattern: 描述：xxx权限： or 描述：xxx积分 or 描述：xxx提示：
	if api.Description == "" {
		if match := regexp.MustCompile(`描述[：:]\s*([^权积提]+)`).FindStringSubmatch(text); len(match) > 1 {
			desc := strings.TrimSpace(match[1])
			// Clean up common endings
			desc = regexp.MustCompile(`[，,。]$`).ReplaceAllString(desc, "")
			api.Description = desc
		}
	}

	// Extract permission: "权限：xxx积分起" or "xxx积分"
	if api.Permission == "" {
		if match := regexp.MustCompile(`权限[：:]\s*(\d+\s*积分[起]?)`).FindStringSubmatch(text); len(match) > 1 {
			api.Permission = strings.TrimSpace(match[1])
		} else if match := regexp.MustCompile(`[需要]?\s*(\d+)\s*积分`).FindStringSubmatch(text); len(match) > 1 {
			api.Permission = match[1] + "积分"
		}
	}
}

// parseParamsTable parses the input parameters table.
// 策略：
//  1. 优先查找前一个元素包含"输入参数"文本的表格
//  2. 降级：使用 #document 下第一个表格
func (p *Parser) parseParamsTable(doc *goquery.Document) []metadata.ParamMeta {
	tables := doc.Find("#document table")
	if tables.Length() == 0 {
		tables = doc.Find("table")
	}
	if tables.Length() == 0 {
		return nil
	}

	// 策略 1：查找前一个元素包含"输入参数"的表格
	var inputTable *goquery.Selection
	tables.Each(func(i int, table *goquery.Selection) {
		if inputTable != nil {
			return
		}
		// 检查表格前面的兄弟元素文本
		prevText := p.getPrevElementText(table)
		if strings.Contains(prevText, "输入参数") {
			inputTable = table
		}
	})

	// 策略 2：降级使用第一个表格
	if inputTable == nil {
		inputTable = tables.First()
	}

	return p.parseTableAsParams(inputTable)
}

// getPrevElementText 获取元素前一个兄弟元素的文本
func (p *Parser) getPrevElementText(elem *goquery.Selection) string {
	prev := elem.Prev()
	if prev.Length() > 0 {
		return strings.TrimSpace(prev.Text())
	}
	return ""
}

// parseTableAsParams parses a table as parameter definitions.
func (p *Parser) parseTableAsParams(table *goquery.Selection) []metadata.ParamMeta {
	var params []metadata.ParamMeta

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

	return params
}

// parseFieldsTable parses the output fields table.
// 策略：
//  1. 优先查找前一个元素包含"输出参数"文本的表格
//  2. 降级：使用 #document 下第二个表格
func (p *Parser) parseFieldsTable(doc *goquery.Document) []metadata.FieldMeta {
	tables := doc.Find("#document table")
	if tables.Length() == 0 {
		tables = doc.Find("table")
	}
	if tables.Length() == 0 {
		return nil
	}

	// 策略 1：查找前一个元素包含"输出参数"的表格
	var outputTable *goquery.Selection
	tables.Each(func(i int, table *goquery.Selection) {
		if outputTable != nil {
			return
		}
		// 检查表格前面的兄弟元素文本
		prevText := p.getPrevElementText(table)
		if strings.Contains(prevText, "输出参数") {
			outputTable = table
		}
	})

	// 策略 2：降级使用第二个表格
	if outputTable == nil && tables.Length() >= 2 {
		outputTable = tables.Eq(1)
	}

	if outputTable == nil {
		return nil
	}

	return p.parseTableAsFields(outputTable)
}

// parseTableAsFields parses a table as field definitions.
func (p *Parser) parseTableAsFields(table *goquery.Selection) []metadata.FieldMeta {
	var fields []metadata.FieldMeta

	// Parse table headers
	var headers []string
	table.Find("thead th, thead td, tr:first-child th, tr:first-child td").Each(func(j int, th *goquery.Selection) {
		headers = append(headers, strings.TrimSpace(strings.ToLower(th.Text())))
	})

	// Map column indices
	nameIdx, typeIdx, defaultOutputIdx, descIdx := -1, -1, -1, -1
	for idx, header := range headers {
		switch {
		case strings.Contains(header, "名称") || strings.Contains(header, "name") || strings.Contains(header, "字段"):
			nameIdx = idx
		case strings.Contains(header, "类型") || strings.Contains(header, "type"):
			typeIdx = idx
		case strings.Contains(header, "默认输出") || strings.Contains(header, "default"):
			defaultOutputIdx = idx
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
		// Also consider "默认输出" column - Y means it's a commonly used field
		if field.Name != "" {
			nameLower := strings.ToLower(field.Name)
			// ts_code + trade_date/end_date/ann_date + report_type/comp_type for financial reports
			// ann_date+end_date+ts_code 常用于财报类 API 的联合主键
			if nameLower == "ts_code" || nameLower == "trade_date" || nameLower == "end_date" ||
				nameLower == "ann_date" || nameLower == "report_type" || nameLower == "comp_type" ||
				strings.HasSuffix(nameLower, "_code") {
				field.IsPrimary = true
				field.IsIndex = true
			} else if strings.HasSuffix(nameLower, "_date") || strings.HasSuffix(nameLower, "_time") {
				field.IsIndex = true
			}

			// Check "默认输出" column for primary/important fields
			if defaultOutputIdx >= 0 && defaultOutputIdx < len(cells) {
				defaultOutput := strings.ToLower(cells[defaultOutputIdx])
				if defaultOutput == "y" || defaultOutput == "是" {
					// Fields with default output Y are commonly used
					if !field.IsPrimary && !field.IsIndex {
						field.IsIndex = true // Mark as index since it's a commonly used field
					}
				}
			}
		}

		if field.Name != "" {
			fields = append(fields, field)
		}
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
