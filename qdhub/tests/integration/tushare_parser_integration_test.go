//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/datasource/tushare"
)

// TestTushareParser_ParseCatalog_Real tests parsing the real Tushare catalog page.
// This test requires network access to tushare.pro.
func TestTushareParser_ParseCatalog_Real(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create crawler and parser
	crawler := tushare.NewCrawler()
	parser := tushare.NewParser()
	parser.SetDataSourceID(shared.NewID())

	// Fetch real catalog page
	content, docType, err := crawler.FetchCatalogPage(ctx, shared.NewID())
	if err != nil {
		t.Fatalf("failed to fetch catalog page: %v", err)
	}

	t.Logf("Fetched catalog page, docType: %s, content length: %d", docType, len(content))

	// Parse catalog
	categories, apiURLs, _, err := parser.ParseCatalog(content)
	if err != nil {
		t.Fatalf("failed to parse catalog: %v", err)
	}

	// Verify we got some API URLs (Tushare has many APIs)
	if len(apiURLs) == 0 {
		t.Error("expected to find API URLs in catalog, got 0")
		// Print a snippet of content for debugging
		if len(content) > 2000 {
			t.Logf("Content snippet (first 2000 chars):\n%s", content[:2000])
		} else {
			t.Logf("Content:\n%s", content)
		}
		return
	}

	// Print summary
	t.Logf("\n%s", strings.Repeat("=", 80))
	t.Logf("CATALOG PARSING RESULTS")
	t.Logf("%s", strings.Repeat("=", 80))

	// Print categories table
	t.Logf("\n[Categories] Total: %d", len(categories))
	printCategoriesTable(t, categories)

	// Print API URLs table
	t.Logf("\n[API URLs] Total: %d", len(apiURLs))
	printAPIURLsTable(t, apiURLs, 20) // Show first 20
}

// TestTushareParser_ParseAPIDetail_Real tests parsing a real Tushare API detail page.
// This test requires network access to tushare.pro.
func TestTushareParser_ParseAPIDetail_Real(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create crawler and parser
	crawler := tushare.NewCrawler()
	parser := tushare.NewParser()
	parser.SetDataSourceID(shared.NewID())

	// Test with a known API URL (daily - 日线行情)
	// doc_id=27 is the daily API
	apiURL := "/document/2?doc_id=27"

	// Fetch real API detail page
	content, docType, err := crawler.FetchAPIDetailPage(ctx, apiURL)
	if err != nil {
		t.Fatalf("failed to fetch API detail page: %v", err)
	}

	t.Logf("Fetched API detail page, docType: %s, content length: %d", docType, len(content))

	// Parse API detail
	apiMeta, err := parser.ParseAPIDetail(content)
	if err != nil {
		t.Fatalf("failed to parse API detail: %v", err)
	}

	// Verify basic parsing worked
	if apiMeta.DisplayName == "" {
		t.Error("expected DisplayName to be set")
	}

	// Print API detail in table format
	printAPIDetailTable(t, apiMeta)
}

// TestTushareParser_FullCrawlFlow_Real tests the full crawl flow:
// 1. Fetch catalog
// 2. Parse catalog to get API URLs
// 3. Fetch and parse first 3 API details
func TestTushareParser_FullCrawlFlow_Real(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Create crawler and parser
	crawler := tushare.NewCrawler()
	parser := tushare.NewParser()
	dataSourceID := shared.NewID()
	parser.SetDataSourceID(dataSourceID)

	// Step 1: Fetch catalog
	catalogContent, _, err := crawler.FetchCatalogPage(ctx, dataSourceID)
	if err != nil {
		t.Fatalf("failed to fetch catalog page: %v", err)
	}

	// Step 2: Parse catalog
	categories, apiURLs, _, err := parser.ParseCatalog(catalogContent)
	if err != nil {
		t.Fatalf("failed to parse catalog: %v", err)
	}

	if len(apiURLs) == 0 {
		t.Fatal("no API URLs found in catalog")
	}

	// Print summary
	t.Logf("\n%s", strings.Repeat("=", 100))
	t.Logf("FULL CRAWL FLOW TEST RESULTS")
	t.Logf("%s", strings.Repeat("=", 100))

	// Print categories
	t.Logf("\n[Step 1: Categories] Total: %d", len(categories))
	printCategoriesTable(t, categories)

	// Print API URLs
	t.Logf("\n[Step 2: API URLs] Total: %d", len(apiURLs))
	printAPIURLsTable(t, apiURLs, 15)

	// Step 3: Fetch and parse first 3 API details
	t.Logf("\n[Step 3: API Details] Parsing first 3 APIs...")
	t.Logf("%s", strings.Repeat("-", 100))

	maxAPIs := 3
	if len(apiURLs) < maxAPIs {
		maxAPIs = len(apiURLs)
	}

	for i := 0; i < maxAPIs; i++ {
		apiURL := apiURLs[i]
		t.Logf("\n>>> Fetching API %d: %s", i+1, apiURL)

		apiContent, _, err := crawler.FetchAPIDetailPage(ctx, apiURL)
		if err != nil {
			t.Logf("  ERROR: failed to fetch: %v", err)
			continue
		}

		apiMeta, err := parser.ParseAPIDetail(apiContent)
		if err != nil {
			t.Logf("  ERROR: failed to parse: %v", err)
			continue
		}

		printAPIDetailTable(t, apiMeta)

		// Rate limit between requests
		time.Sleep(500 * time.Millisecond)
	}

	t.Logf("\n%s", strings.Repeat("=", 100))
	t.Logf("CRAWL COMPLETED SUCCESSFULLY")
	t.Logf("%s", strings.Repeat("=", 100))
}

// ============================================================================
// Table Printing Helpers
// ============================================================================

// printCategoriesTable prints categories in a table format.
func printCategoriesTable(t *testing.T, categories []metadata.APICategory) {
	if len(categories) == 0 {
		t.Log("  (No categories found)")
		return
	}

	// Table header
	t.Logf("  +-----+----------------------+----------------------+-------+")
	t.Logf("  | %-3s | %-20s | %-20s | %-5s |", "#", "Name", "Parent", "Order")
	t.Logf("  +-----+----------------------+----------------------+-------+")

	for i, cat := range categories {
		parentName := "(root)"
		if cat.ParentID != nil {
			// Find parent name
			for _, c := range categories {
				if c.ID == *cat.ParentID {
					parentName = truncateString(c.Name, 18)
					break
				}
			}
			if parentName == "(root)" {
				parentName = "(parent)"
			}
		}
		t.Logf("  | %-3d | %-20s | %-20s | %-5d |",
			i+1,
			truncateString(cat.Name, 20),
			parentName,
			cat.SortOrder)
	}
	t.Logf("  +-----+----------------------+----------------------+-------+")
}

// printAPIURLsTable prints API URLs in a table format.
func printAPIURLsTable(t *testing.T, apiURLs []string, limit int) {
	if len(apiURLs) == 0 {
		t.Log("  (No API URLs found)")
		return
	}

	// Table header
	t.Logf("  +-----+----------------------------------------------------------+")
	t.Logf("  | %-3s | %-56s |", "#", "API URL")
	t.Logf("  +-----+----------------------------------------------------------+")

	displayed := 0
	for i, url := range apiURLs {
		if displayed >= limit {
			t.Logf("  | ... | %-56s |", fmt.Sprintf("(and %d more URLs)", len(apiURLs)-limit))
			break
		}
		t.Logf("  | %-3d | %-56s |", i+1, truncateString(url, 56))
		displayed++
	}
	t.Logf("  +-----+----------------------------------------------------------+")
}

// printAPIDetailTable prints API metadata in a table format.
func printAPIDetailTable(t *testing.T, api *metadata.APIMetadata) {
	t.Logf("\n  %s", strings.Repeat("-", 80))
	t.Logf("  API METADATA")
	t.Logf("  %s", strings.Repeat("-", 80))

	// Basic info table
	t.Logf("  | %-15s | %-58s |", "Field", "Value")
	t.Logf("  +-----------------+------------------------------------------------------------+")
	t.Logf("  | %-15s | %-58s |", "Name", api.Name)
	t.Logf("  | %-15s | %-58s |", "DisplayName", truncateString(api.DisplayName, 58))
	t.Logf("  | %-15s | %-58s |", "Endpoint", api.Endpoint)
	t.Logf("  | %-15s | %-58s |", "Description", truncateString(api.Description, 58))
	t.Logf("  | %-15s | %-58s |", "Permission", api.Permission)
	t.Logf("  +-----------------+------------------------------------------------------------+")

	// Request params table
	t.Logf("\n  [Request Params] Count: %d", len(api.RequestParams))
	if len(api.RequestParams) > 0 {
		t.Logf("  +-----+--------------------+----------+----------+--------------------------------+")
		t.Logf("  | %-3s | %-18s | %-8s | %-8s | %-30s |", "#", "Name", "Type", "Required", "Description")
		t.Logf("  +-----+--------------------+----------+----------+--------------------------------+")
		for i, p := range api.RequestParams {
			required := "N"
			if p.Required {
				required = "Y"
			}
			t.Logf("  | %-3d | %-18s | %-8s | %-8s | %-30s |",
				i+1,
				truncateString(p.Name, 18),
				truncateString(p.Type, 8),
				required,
				truncateString(p.Description, 30))
		}
		t.Logf("  +-----+--------------------+----------+----------+--------------------------------+")
	}

	// Response fields table
	t.Logf("\n  [Response Fields] Count: %d", len(api.ResponseFields))
	if len(api.ResponseFields) > 0 {
		t.Logf("  +-----+--------------------+----------+---------+-------+----------------------------+")
		t.Logf("  | %-3s | %-18s | %-8s | %-7s | %-5s | %-26s |", "#", "Name", "Type", "Primary", "Index", "Description")
		t.Logf("  +-----+--------------------+----------+---------+-------+----------------------------+")
		for i, f := range api.ResponseFields {
			primary := ""
			if f.IsPrimary {
				primary = "Y"
			}
			index := ""
			if f.IsIndex {
				index = "Y"
			}
			t.Logf("  | %-3d | %-18s | %-8s | %-7s | %-5s | %-26s |",
				i+1,
				truncateString(f.Name, 18),
				truncateString(f.Type, 8),
				primary,
				index,
				truncateString(f.Description, 26))
		}
		t.Logf("  +-----+--------------------+----------+---------+-------+----------------------------+")
	}
}

// truncateString truncates a string to maxLen and adds ellipsis if needed.
func truncateString(s string, maxLen int) string {
	// Remove newlines
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")

	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen-3]) + "..."
}
