package tushare_test

import (
	"testing"

	"qdhub/internal/domain/metadata"
	"qdhub/internal/domain/shared"
	"qdhub/internal/infrastructure/datasource/tushare"
)

func TestParser_SupportedType(t *testing.T) {
	parser := tushare.NewParser()
	if parser.SupportedType() != metadata.DocumentTypeHTML {
		t.Errorf("expected supported type to be 'html', got '%s'", parser.SupportedType())
	}
}

func TestParser_ParseAPIDetail_Basic(t *testing.T) {
	parser := tushare.NewParser()
	parser.SetDataSourceID(shared.NewID())

	html := `
<!DOCTYPE html>
<html>
<head><title>Tushare API</title></head>
<body>
<h1>接口：daily - 日线行情</h1>
<p>获取A股日线行情数据，复权数据</p>

<h3>输入参数</h3>
<table>
<thead>
<tr><th>名称</th><th>类型</th><th>必选</th><th>描述</th></tr>
</thead>
<tbody>
<tr><td>ts_code</td><td>str</td><td>N</td><td>股票代码</td></tr>
<tr><td>trade_date</td><td>str</td><td>N</td><td>交易日期</td></tr>
<tr><td>start_date</td><td>str</td><td>N</td><td>开始日期</td></tr>
<tr><td>end_date</td><td>str</td><td>N</td><td>结束日期</td></tr>
</tbody>
</table>

<h3>输出参数</h3>
<table>
<thead>
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
</thead>
<tbody>
<tr><td>ts_code</td><td>str</td><td>股票代码</td></tr>
<tr><td>trade_date</td><td>str</td><td>交易日期</td></tr>
<tr><td>open</td><td>float</td><td>开盘价</td></tr>
<tr><td>high</td><td>float</td><td>最高价</td></tr>
<tr><td>low</td><td>float</td><td>最低价</td></tr>
<tr><td>close</td><td>float</td><td>收盘价</td></tr>
<tr><td>vol</td><td>float</td><td>成交量</td></tr>
</tbody>
</table>
</body>
</html>
`

	apiMeta, err := parser.ParseAPIDetail(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify display name
	if apiMeta.DisplayName == "" {
		t.Error("expected display name to be set")
	}

	// Verify request params
	if len(apiMeta.RequestParams) == 0 {
		t.Error("expected request params to be parsed")
	}

	// Verify response fields
	if len(apiMeta.ResponseFields) == 0 {
		t.Error("expected response fields to be parsed")
	}

	// Check if ts_code is marked as primary/index
	for _, field := range apiMeta.ResponseFields {
		if field.Name == "ts_code" {
			if !field.IsPrimary {
				t.Error("expected ts_code to be marked as primary")
			}
			if !field.IsIndex {
				t.Error("expected ts_code to be marked as index")
			}
		}
		if field.Name == "trade_date" {
			if !field.IsIndex {
				t.Error("expected trade_date to be marked as index")
			}
		}
	}
}

func TestParser_ParseAPIDetail_ExtractAPIName(t *testing.T) {
	parser := tushare.NewParser()
	parser.SetDataSourceID(shared.NewID())

	html := `
<!DOCTYPE html>
<html>
<body>
<h1>接口：stock_basic - 股票列表</h1>
<p>获取基础信息数据</p>
<code>pro.query('stock_basic', exchange='', list_status='L')</code>
</body>
</html>
`

	apiMeta, err := parser.ParseAPIDetail(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if apiMeta.Name != "stock_basic" {
		t.Errorf("expected API name 'stock_basic', got '%s'", apiMeta.Name)
	}
}

func TestParser_ParseCatalog_Basic(t *testing.T) {
	parser := tushare.NewParser()
	parser.SetDataSourceID(shared.NewID())

	html := `
<!DOCTYPE html>
<html>
<body>
<nav class="menu">
<ul>
<li><a href="/document/2?doc_id=25">日线行情</a></li>
<li><a href="/document/2?doc_id=26">周线行情</a></li>
<li><a href="/document/2?doc_id=27">月线行情</a></li>
</ul>
</nav>
</body>
</html>
`

	categories, apiURLs, _, err := parser.ParseCatalog(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Categories may be empty for simple HTML
	_ = categories

	// Should have found API URLs
	if len(apiURLs) != 3 {
		t.Errorf("expected 3 API URLs, got %d", len(apiURLs))
	}
}

func TestParser_ParseAPIDetail_EmptyHTML(t *testing.T) {
	parser := tushare.NewParser()
	parser.SetDataSourceID(shared.NewID())

	html := `<!DOCTYPE html><html><body></body></html>`

	apiMeta, err := parser.ParseAPIDetail(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return empty API metadata
	if apiMeta == nil {
		t.Error("expected non-nil API metadata")
	}
}

func TestParser_ParseAPIDetail_NoTables(t *testing.T) {
	parser := tushare.NewParser()
	parser.SetDataSourceID(shared.NewID())

	html := `
<!DOCTYPE html>
<html>
<body>
<h1>Some API</h1>
<div class="doc-description">Description here</div>
</body>
</html>
`

	apiMeta, err := parser.ParseAPIDetail(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have display name
	if apiMeta.DisplayName == "" {
		t.Error("expected display name from h1")
	}

	// Description may or may not be parsed depending on selector
	// The important thing is no error and display name is parsed

	// Should have empty params and fields
	if len(apiMeta.RequestParams) != 0 {
		t.Errorf("expected 0 request params, got %d", len(apiMeta.RequestParams))
	}
	if len(apiMeta.ResponseFields) != 0 {
		t.Errorf("expected 0 response fields, got %d", len(apiMeta.ResponseFields))
	}
}
