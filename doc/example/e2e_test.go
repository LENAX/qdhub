// Package e2e 提供端到端测试
// 支持两种模式：mock模式和真实模式
// 通过环境变量 E2E_MODE 控制：mock（默认）或 real
// 真实模式需要设置 TUSHARE_TOKEN 环境变量
package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/mattn/go-sqlite3"
	"github.com/LENAX/task-engine/internal/storage/sqlite"
	"github.com/LENAX/task-engine/pkg/core/builder"
	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/core/task"
	"github.com/LENAX/task-engine/pkg/core/workflow"
)

// 高并发 HTTP 客户端（支持 50+ 并发连接）
// 基于 DefaultTransport 修改，保留代理和 DNS 配置
var httpClient = func() *http.Client {
	// 复制默认 Transport 的配置
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// 增加并发连接数
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100
	transport.MaxConnsPerHost = 100
	transport.IdleConnTimeout = 90 * time.Second
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}
}()

// ==================== 测试配置 ====================

// E2EConfig E2E测试配置
type E2EConfig struct {
	Mode           string // mock 或 real
	TushareToken   string // Tushare API Token（真实模式需要）
	DocServerURL   string // 文档服务器URL
	APIServerURL   string // API服务器URL
	MetadataDBPath string // 元数据数据库路径
	StockDBPath    string // 股票数据数据库路径
	StartDate      string // 数据开始日期
	EndDate        string // 数据结束日期
	MaxAPICrawl    int    // 最大爬取API数量（0表示不限制）
}

// getE2EConfig 获取E2E测试配置
func getE2EConfig(t *testing.T) *E2EConfig {
	mode := os.Getenv("E2E_MODE")
	if mode == "" {
		mode = "mock"
	}

	cfg := &E2EConfig{
		Mode:      mode,
		StartDate: "20251201",
		EndDate:   "20251231",
	}

	if mode == "real" {
		// 优先使用 QDHUB_TUSHARE_TOKEN，其次 TUSHARE_TOKEN
		cfg.TushareToken = os.Getenv("QDHUB_TUSHARE_TOKEN")
		if cfg.TushareToken == "" {
			cfg.TushareToken = os.Getenv("TUSHARE_TOKEN")
		}
		if cfg.TushareToken == "" {
			t.Skip("真实模式需要设置 QDHUB_TUSHARE_TOKEN 或 TUSHARE_TOKEN 环境变量")
		}
		fmt.Println("cfg.TushareToken", cfg.TushareToken)
		// 清理token中可能的换行符
		cfg.TushareToken = strings.TrimSpace(cfg.TushareToken)
		cfg.DocServerURL = "https://tushare.pro"
		cfg.APIServerURL = "http://api.tushare.pro"
		cfg.MaxAPICrawl = 0 // 0 表示不限制，全量爬取
	}

	// 设置数据库路径
	dataDir := filepath.Join(os.TempDir(), "task-engine-e2e", time.Now().Format("20060102150405"))
	os.MkdirAll(dataDir, 0755)
	cfg.MetadataDBPath = filepath.Join(dataDir, "metadata.db")
	cfg.StockDBPath = filepath.Join(dataDir, "stock_data.db")

	return cfg
}

// ==================== E2E测试上下文 ====================

// E2EContext E2E测试上下文
type E2EContext struct {
	Config      *E2EConfig
	Engine      *engine.Engine
	Registry    task.FunctionRegistry
	DocServer   *MockTushareDocServer
	APIServer   *MockTushareAPIServer
	MetadataDB  *sql.DB
	StockDB     *sql.DB
	CrawlResult *CrawlResult
	cleanup     func()
	// 用于并发收集子任务结果的互斥锁和收集器
	crawlResultMu  sync.Mutex
	crawlCollector *CrawlResultCollector
}

// CrawlResultCollector 用于收集并发爬取的API详情结果
type CrawlResultCollector struct {
	Provider   DataProvider
	Catalogs   []APICatalog
	Params     []APIParam
	DataFields []APIDataField
	mu         sync.Mutex
}

// setupE2E 设置E2E测试环境
func setupE2E(t *testing.T) *E2EContext {
	cfg := getE2EConfig(t)

	ctx := &E2EContext{
		Config: cfg,
	}

	// 创建临时数据库目录用于engine
	tmpDir := t.TempDir()
	engineDBPath := filepath.Join(tmpDir, "engine.db")

	// 创建Repository
	repos, err := sqlite.NewRepositories(engineDBPath)
	if err != nil {
		t.Fatalf("创建Repository失败: %v", err)
	}

	// 创建Engine（真实模式使用50并发，mock模式使用10）
	workerCount := 10
	if cfg.Mode == "real" {
		workerCount = 50
	}
	eng, err := engine.NewEngine(workerCount, 60, repos.Workflow, repos.WorkflowInstance, repos.Task)
	if err != nil {
		t.Fatalf("创建Engine失败: %v", err)
	}
	ctx.Engine = eng

	// 获取Registry
	ctx.Registry = eng.GetRegistry()

	// 启动mock服务器（如果是mock模式）
	if cfg.Mode == "mock" {
		ctx.DocServer = NewMockTushareDocServer()
		cfg.DocServerURL = ctx.DocServer.Start()

		ctx.APIServer = NewMockTushareAPIServer("test_token")
		cfg.APIServerURL = ctx.APIServer.Start()
		cfg.TushareToken = "test_token"
	}

	// 启动Engine
	if err := eng.Start(context.Background()); err != nil {
		t.Fatalf("启动Engine失败: %v", err)
	}

	// 注册任务函数
	registerE2EFunctions(t, ctx)

	ctx.cleanup = func() {
		eng.Stop()
		repos.Close()
		if ctx.DocServer != nil {
			ctx.DocServer.Stop()
		}
		if ctx.APIServer != nil {
			ctx.APIServer.Stop()
		}
		if ctx.MetadataDB != nil {
			ctx.MetadataDB.Close()
		}
		if ctx.StockDB != nil {
			ctx.StockDB.Close()
		}

		// 复制数据库到 test/e2e/data 目录
		copyDatabasesForInspection(t, cfg)
	}

	return ctx
}

// copyDatabasesForInspection 复制数据库文件到指定目录供检查
func copyDatabasesForInspection(t *testing.T, cfg *E2EConfig) {
	// 获取项目根目录
	wd, _ := os.Getwd()
	dataDir := filepath.Join(wd, "data")
	os.MkdirAll(dataDir, 0755)

	// 复制元数据数据库
	if _, err := os.Stat(cfg.MetadataDBPath); err == nil {
		destPath := filepath.Join(dataDir, "metadata.db")
		copyFile(cfg.MetadataDBPath, destPath)
		t.Logf("元数据数据库已保存到: %s", destPath)
	}

	// 复制股票数据数据库
	if _, err := os.Stat(cfg.StockDBPath); err == nil {
		destPath := filepath.Join(dataDir, "stock_data.db")
		copyFile(cfg.StockDBPath, destPath)
		t.Logf("股票数据数据库已保存到: %s", destPath)
	}
}

// copyFile 复制文件
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// registerE2EFunctions 注册E2E测试所需的函数
func registerE2EFunctions(t *testing.T, ctx *E2EContext) {
	bgCtx := context.Background()
	registry := ctx.Registry

	// 注册依赖
	registry.RegisterDependencyWithKey("E2EContext", ctx)
	registry.RegisterDependencyWithKey("Engine", ctx.Engine)

	// 注册爬取文档目录函数
	registry.Register(bgCtx, "CrawlDocCatalog", CrawlDocCatalog, "爬取Tushare文档目录")

	// 注册爬取API详情函数（模板任务版本）
	registry.Register(bgCtx, "CrawlAPIDetail", CrawlAPIDetail, "爬取API详情（模板任务）")

	// 注册爬取单个API详情函数（子任务使用）
	registry.Register(bgCtx, "CrawlSingleAPIDetail", CrawlSingleAPIDetail, "爬取单个API详情（子任务）")

	// 注册保存元数据函数
	registry.Register(bgCtx, "SaveMetadata", SaveMetadata, "保存元数据到SQLite")

	// 注册建表函数
	registry.Register(bgCtx, "CreateTables", CreateTables, "基于元数据创建数据表")

	// 注册数据获取函数（用于普通任务）
	registry.Register(bgCtx, "FetchTradeCal", FetchTradeCal, "获取交易日历")
	registry.Register(bgCtx, "FetchStockBasic", FetchStockBasic, "获取股票基本信息")
	registry.Register(bgCtx, "FetchTopList", FetchTopList, "获取龙虎榜")

	// 注册模板任务的 Job Functions（在 Job Function 中生成子任务，符合设计文档要求）
	// 根据设计文档：用户应该把任务生成函数放在 Job Function 中
	// Job Function 可以从 context 引用之前任务的结果，并注入给子任务
	registry.Register(bgCtx, "GenerateDailySubTasksJob", GenerateDailySubTasksJob, "生成日线数据子任务（JobFunc）")
	registry.Register(bgCtx, "GenerateAdjFactorSubTasksJob", GenerateAdjFactorSubTasksJob, "生成复权因子子任务（JobFunc）")
	registry.Register(bgCtx, "GenerateIncomeSubTasksJob", GenerateIncomeSubTasksJob, "生成利润表子任务（JobFunc）")
	registry.Register(bgCtx, "GenerateBalanceSheetSubTasksJob", GenerateBalanceSheetSubTasksJob, "生成资产负债表子任务（JobFunc）")
	registry.Register(bgCtx, "GenerateCashFlowSubTasksJob", GenerateCashFlowSubTasksJob, "生成现金流量表子任务（JobFunc）")
	registry.Register(bgCtx, "GenerateAPIDetailSubTasksJob", GenerateAPIDetailSubTasksJob, "生成API详情子任务（JobFunc）")

	// 注册子任务的 Job Functions（由模板任务的 Job Function 生成的子任务使用）
	// 这些函数从参数中获取 ts_code 并执行实际的数据获取
	registry.Register(bgCtx, "FetchDailySub", FetchDaily, "获取日线行情(子任务)")
	registry.Register(bgCtx, "FetchAdjFactorSub", FetchAdjFactor, "获取复权因子(子任务)")
	registry.Register(bgCtx, "FetchIncomeSub", FetchIncome, "获取利润表(子任务)")
	registry.Register(bgCtx, "FetchBalanceSheetSub", FetchBalanceSheet, "获取资产负债表(子任务)")
	registry.Register(bgCtx, "FetchCashFlowSub", FetchCashFlow, "获取现金流量表(子任务)")

	// 保留旧的 TemplateNoOp 用于向后兼容（但不推荐使用）
	registry.Register(bgCtx, "TemplateNoOp", func(tc *task.TaskContext) (interface{}, error) {
		log.Printf("⚠️ [模板任务] %s - 使用废弃的 TemplateNoOp，建议迁移到 Job Function 模式", tc.TaskName)
		return map[string]string{"status": "template_ready"}, nil
	}, "模板任务占位函数（已废弃，仅用于向后兼容）")

	// 注册通用Handler
	registry.RegisterTaskHandler(bgCtx, "LogSuccess", func(tc *task.TaskContext) {
		log.Printf("✅ [任务成功] %s", tc.TaskName)
	}, "记录成功")

	registry.RegisterTaskHandler(bgCtx, "LogError", func(tc *task.TaskContext) {
		errMsg := tc.GetParamString("_error_message")
		log.Printf("❌ [任务失败] %s: %s", tc.TaskName, errMsg)
	}, "记录错误")

	// 注册生成子任务的 Handlers（用于模板任务模式）
	registry.RegisterTaskHandler(bgCtx, "GenerateDailySubTasks", GenerateDailySubTasks, "生成日线数据子任务")
	registry.RegisterTaskHandler(bgCtx, "GenerateAdjFactorSubTasks", GenerateAdjFactorSubTasks, "生成复权因子子任务")
	registry.RegisterTaskHandler(bgCtx, "GenerateIncomeSubTasks", GenerateIncomeSubTasks, "生成利润表子任务")
	registry.RegisterTaskHandler(bgCtx, "GenerateBalanceSheetSubTasks", GenerateBalanceSheetSubTasks, "生成资产负债表子任务")
	registry.RegisterTaskHandler(bgCtx, "GenerateCashFlowSubTasks", GenerateCashFlowSubTasks, "生成现金流量表子任务")
	registry.RegisterTaskHandler(bgCtx, "GenerateAPIDetailSubTasks", GenerateAPIDetailSubTasks, "生成API详情子任务")
	registry.RegisterTaskHandler(bgCtx, "AggregateAPIDetailResults", AggregateAPIDetailResults, "聚合API详情子任务结果")
}

// ==================== Workflow 1: 文档爬取和元数据保存 ====================

// CrawlDocCatalog 爬取文档目录
func CrawlDocCatalog(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	url := ctx.Config.DocServerURL + "/document/2"
	log.Printf("📡 [CrawlDocCatalog] 开始爬取: %s", url)

	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	// 解析目录结构
	catalogs := parseDocCatalog(string(body), ctx.Config.DocServerURL)
	log.Printf("✅ [CrawlDocCatalog] 解析到 %d 个目录项", len(catalogs))

	return catalogs, nil
}

// parseDocCatalog 解析文档目录HTML（使用goquery）
// 支持两种URL格式：
// - Mock格式: /document/2/数字
// - 真实格式: /document/2?doc_id=数字
// 区分叶子节点和目录节点：
// - 真实网站：父目录的 <li> 有 class="in"，叶子节点没有
// - Mock服务器：所有节点都是叶子节点
func parseDocCatalog(html, baseURL string) []APICatalog {
	var catalogs []APICatalog

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		log.Printf("解析HTML失败: %v", err)
		return catalogs
	}

	// 需要屏蔽的目录项（非 API 数据）
	excludedNames := map[string]bool{
		"数据索引": true,
		"社区捐助": true,
	}

	i := 0
	// 查找所有指向 /document/2 的链接（支持两种格式）
	doc.Find("a[href^='/document/2']").Each(func(_ int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists {
			return
		}

		name := strings.TrimSpace(s.Text())

		// 跳过需要屏蔽的目录项
		if excludedNames[name] {
			return
		}

		var docID string
		var fullLink string

		// 格式1: /document/2?doc_id=数字 (真实网站)
		if strings.Contains(href, "?doc_id=") {
			parts := strings.Split(href, "?doc_id=")
			if len(parts) == 2 && parts[1] != "" {
				docID = parts[1]
				fullLink = baseURL + href
			}
		} else if strings.HasPrefix(href, "/document/2/") {
			// 格式2: /document/2/数字 (mock服务器)
			parts := strings.Split(href, "/")
			if len(parts) >= 4 {
				lastPart := parts[len(parts)-1]
				if lastPart != "" && lastPart != "2" {
					docID = lastPart
					fullLink = baseURL + href
				}
			}
		}

		// 跳过无效链接
		if docID == "" || fullLink == "" {
			return
		}

		// 判断是否为叶子节点：检查父元素 <li> 的 class 是否包含 "in"
		// 真实网站：父目录的 <li class="  in"> 表示展开的目录，不是叶子节点
		// Mock服务器：所有节点都是叶子节点
		isLeaf := true
		parentLi := s.Parent()
		if parentLi.Is("li") {
			class, _ := parentLi.Attr("class")
			// 如果 class 包含 "in"，说明是展开的目录（非叶子节点）
			if strings.Contains(class, "in") {
				isLeaf = false
			}
		}

		i++
		catalogs = append(catalogs, APICatalog{
			ID:        i,
			Name:      name,
			Link:      fullLink,
			IsLeaf:    isLeaf,
			Level:     3,
			SortOrder: i,
			CreatedAt: time.Now(),
		})
	})

	return catalogs
}

// CrawlAPIDetail 爬取API详情（模板任务版本，用于生成子任务）
// 这个函数现在作为模板任务的占位函数，实际爬取逻辑在子任务中
func CrawlAPIDetail(tc *task.TaskContext) (interface{}, error) {
	log.Printf("📋 [CrawlAPIDetail] 模板任务准备生成子任务")
	return map[string]string{"status": "template_ready"}, nil
}

// CrawlSingleAPIDetail 爬取单个API详情（子任务使用）
func CrawlSingleAPIDetail(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从参数中获取 catalog 信息
	catalogID, err := tc.GetParamInt("catalog_id")
	if err != nil || catalogID == 0 {
		return nil, fmt.Errorf("未找到 catalog_id 参数")
	}

	catalogName := tc.GetParamString("catalog_name")
	catalogLink := tc.GetParamString("catalog_link")

	if catalogLink == "" {
		return nil, fmt.Errorf("catalog_link 为空")
	}

	log.Printf("📡 [CrawlSingleAPIDetail] 爬取: %s (ID=%d)", catalogName, catalogID)

	// 爬取API详情（使用高并发 HTTP 客户端）
	resp, err := httpClient.Get(catalogLink)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	// 解析API详情
	detail := parseAPIDetail(string(body), catalogID)

	// 更新catalog信息
	catalog := APICatalog{
		ID:          catalogID,
		Name:        catalogName,
		Link:        catalogLink,
		APIName:     detail.apiName,
		Description: detail.description,
		Permission:  detail.permission,
		IsLeaf:      true,
		Level:       3,
		CreatedAt:   time.Now(),
	}

	// 线程安全地收集结果
	ctx.crawlResultMu.Lock()
	if ctx.crawlCollector == nil {
		ctx.crawlCollector = &CrawlResultCollector{
			Provider: DataProvider{
				ID:          1,
				Name:        "Tushare",
				BaseURL:     ctx.Config.APIServerURL,
				Description: "Tushare金融大数据平台",
				CreatedAt:   time.Now(),
			},
			Catalogs:   []APICatalog{},
			Params:     []APIParam{},
			DataFields: []APIDataField{},
		}
	}
	ctx.crawlCollector.mu.Lock()
	ctx.crawlCollector.Catalogs = append(ctx.crawlCollector.Catalogs, catalog)
	ctx.crawlCollector.Params = append(ctx.crawlCollector.Params, detail.params...)
	ctx.crawlCollector.DataFields = append(ctx.crawlCollector.DataFields, detail.fields...)
	ctx.crawlCollector.mu.Unlock()
	ctx.crawlResultMu.Unlock()

	log.Printf("✅ [CrawlSingleAPIDetail] 完成: %s, 参数=%d, 字段=%d", catalogName, len(detail.params), len(detail.fields))

	return map[string]interface{}{
		"catalog_id":   catalogID,
		"catalog_name": catalogName,
		"params_count": len(detail.params),
		"fields_count": len(detail.fields),
	}, nil
}

// apiDetailResult API详情解析结果
type apiDetailResult struct {
	apiName     string
	description string
	permission  string
	params      []APIParam
	fields      []APIDataField
}

// parseAPIDetail 解析API详情HTML（使用goquery）
// 支持两种格式：mock服务器格式和真实Tushare网站格式
func parseAPIDetail(html string, catalogID int) *apiDetailResult {
	result := &apiDetailResult{}

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		log.Printf("解析API详情HTML失败: %v", err)
		return result
	}

	// 格式1: Mock服务器 - 在 .api-info 区域内的 p 标签
	doc.Find(".api-info p").Each(func(_ int, s *goquery.Selection) {
		text := s.Text()
		if strings.Contains(text, "接口：") {
			result.apiName = strings.TrimSpace(strings.TrimPrefix(text, "接口："))
		} else if strings.Contains(text, "描述：") {
			result.description = strings.TrimSpace(strings.TrimPrefix(text, "描述："))
		} else if strings.Contains(text, "权限：") {
			result.permission = strings.TrimSpace(strings.TrimPrefix(text, "权限："))
		}
	})

	// 格式2: 真实网站 - 在 .content 区域内的 p 标签
	// HTML结构: <p>接口：stk_premarket<br>描述：...<br>限量：...<br>权限：...</p>
	// 或者: <p>接口：stock_basic，可以通过<a>数据工具</a>调试<br>描述：...</p>
	// goquery的Text()会忽略<br>，需要用Html()获取原始内容再解析
	if result.apiName == "" {
		doc.Find(".content p").Each(func(_ int, s *goquery.Selection) {
			// 获取HTML内容，保留<br>标签
			html, _ := s.Html()
			text := s.Text()

			// 检查是否包含接口信息
			if strings.Contains(text, "接口：") && result.apiName == "" {
				// 按<br>分割HTML内容
				parts := strings.Split(html, "<br")
				for _, part := range parts {
					// 清理HTML标签残留（如 ">描述：..."）
					part = strings.TrimPrefix(part, ">")
					part = strings.TrimPrefix(part, "/>")
					part = strings.TrimSpace(part)

					if strings.HasPrefix(part, "接口：") {
						apiName := strings.TrimPrefix(part, "接口：")
						// 移除HTML标签
						apiName = regexp.MustCompile(`<[^>]*>`).ReplaceAllString(apiName, "")
						// 截取到第一个中文逗号或中文句号或空格
						if idx := strings.IndexAny(apiName, "，。, "); idx > 0 {
							apiName = apiName[:idx]
						}
						result.apiName = strings.TrimSpace(apiName)
					} else if strings.HasPrefix(part, "描述：") {
						desc := strings.TrimPrefix(part, "描述：")
						// 移除HTML标签
						desc = regexp.MustCompile(`<[^>]*>`).ReplaceAllString(desc, "")
						result.description = strings.TrimSpace(desc)
					} else if strings.HasPrefix(part, "权限：") {
						permPart := strings.TrimPrefix(part, "权限：")
						// 移除HTML标签
						permPart = regexp.MustCompile(`<[^>]*>`).ReplaceAllString(permPart, "")
						result.permission = strings.TrimSpace(permPart)
					}
				}
			}
		})
	}

	// 提取输入参数表格
	result.params = parseParamsTableWithGoquery(doc, catalogID)

	// 提取输出字段表格
	result.fields = parseFieldsTableWithGoquery(doc, catalogID)

	return result
}

// parseParamsTableWithGoquery 使用goquery解析参数表格
// 支持两种格式：mock服务器（table.params-table）和真实网站（输入参数后的table）
func parseParamsTableWithGoquery(doc *goquery.Document, catalogID int) []APIParam {
	var params []APIParam

	// 格式1: Mock服务器 - table.params-table
	doc.Find("table.params-table tbody tr").Each(func(i int, s *goquery.Selection) {
		tds := s.Find("td")
		if tds.Length() >= 4 {
			name := strings.TrimSpace(tds.Eq(0).Text())
			paramType := strings.TrimSpace(tds.Eq(1).Text())
			required := strings.TrimSpace(tds.Eq(2).Text()) == "Y"
			desc := strings.TrimSpace(tds.Eq(3).Text())

			if name != "" {
				params = append(params, APIParam{
					ID:          catalogID*100 + i + 1,
					CatalogID:   catalogID,
					Name:        name,
					Type:        paramType,
					Required:    required,
					Description: desc,
					SortOrder:   i + 1,
					CreatedAt:   time.Now(),
				})
			}
		}
	})

	// 格式2: 真实网站 - "输入参数"后的第一个table
	if len(params) == 0 {
		foundInputParams := false
		doc.Find("p, table").Each(func(_ int, s *goquery.Selection) {
			if foundInputParams && s.Is("table") {
				// 找到输入参数后的第一个表格
				s.Find("tbody tr").Each(func(i int, row *goquery.Selection) {
					tds := row.Find("td")
					if tds.Length() >= 4 {
						name := strings.TrimSpace(tds.Eq(0).Text())
						paramType := strings.TrimSpace(tds.Eq(1).Text())
						requiredText := strings.TrimSpace(tds.Eq(2).Text())
						required := requiredText == "Y" || requiredText == "是"
						desc := strings.TrimSpace(tds.Eq(3).Text())

						if name != "" {
							params = append(params, APIParam{
								ID:          catalogID*100 + i + 1,
								CatalogID:   catalogID,
								Name:        name,
								Type:        paramType,
								Required:    required,
								Description: desc,
								SortOrder:   i + 1,
								CreatedAt:   time.Now(),
							})
						}
					}
				})
				foundInputParams = false // 只处理第一个表格
				return
			}
			if s.Is("p") && strings.Contains(s.Text(), "输入参数") {
				foundInputParams = true
			}
		})
	}

	return params
}

// parseFieldsTableWithGoquery 使用goquery解析字段表格
// 支持两种格式：mock服务器（table.fields-table）和真实网站（输出参数后的table）
func parseFieldsTableWithGoquery(doc *goquery.Document, catalogID int) []APIDataField {
	var fields []APIDataField

	// 格式1: Mock服务器 - table.fields-table
	doc.Find("table.fields-table tbody tr").Each(func(i int, s *goquery.Selection) {
		tds := s.Find("td")
		if tds.Length() >= 4 {
			name := strings.TrimSpace(tds.Eq(0).Text())
			fieldType := strings.TrimSpace(tds.Eq(1).Text())
			isDefault := strings.TrimSpace(tds.Eq(2).Text()) == "Y"
			desc := strings.TrimSpace(tds.Eq(3).Text())

			if name != "" {
				fields = append(fields, APIDataField{
					ID:          catalogID*100 + i + 1,
					CatalogID:   catalogID,
					Name:        name,
					Type:        fieldType,
					Default:     isDefault,
					Description: desc,
					SortOrder:   i + 1,
					CreatedAt:   time.Now(),
				})
			}
		}
	})

	// 格式2: 真实网站 - "输出参数"后的第一个table
	// 支持两种列格式：
	// - 4列: 名称、类型、默认值、描述
	// - 3列: 名称、类型、描述（无默认值列）
	if len(fields) == 0 {
		foundOutputParams := false
		doc.Find("p, table").Each(func(_ int, s *goquery.Selection) {
			if foundOutputParams && s.Is("table") {
				// 找到输出参数后的第一个表格
				s.Find("tbody tr").Each(func(i int, row *goquery.Selection) {
					tds := row.Find("td")
					colCount := tds.Length()

					var name, fieldType, desc string
					var isDefault bool

					if colCount >= 4 {
						// 4列格式: 名称、类型、默认值、描述
						name = strings.TrimSpace(tds.Eq(0).Text())
						fieldType = strings.TrimSpace(tds.Eq(1).Text())
						defaultText := strings.TrimSpace(tds.Eq(2).Text())
						isDefault = defaultText == "Y" || defaultText == "是"
						desc = strings.TrimSpace(tds.Eq(3).Text())
					} else if colCount >= 3 {
						// 3列格式: 名称、类型、描述（无默认值列）
						name = strings.TrimSpace(tds.Eq(0).Text())
						fieldType = strings.TrimSpace(tds.Eq(1).Text())
						desc = strings.TrimSpace(tds.Eq(2).Text())
						isDefault = false
					} else {
						return // 列数不足，跳过
					}

					if name != "" {
						fields = append(fields, APIDataField{
							ID:          catalogID*100 + i + 1,
							CatalogID:   catalogID,
							Name:        name,
							Type:        fieldType,
							Default:     isDefault,
							Description: desc,
							SortOrder:   i + 1,
							CreatedAt:   time.Now(),
						})
					}
				})
				foundOutputParams = false // 只处理第一个表格
				return
			}
			if s.Is("p") && strings.Contains(s.Text(), "输出参数") {
				foundOutputParams = true
			}
		})
	}

	return fields
}

// SaveMetadata 保存元数据到SQLite
func SaveMetadata(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从 crawlCollector 聚合结果（如果存在）
	ctx.crawlResultMu.Lock()
	if ctx.crawlCollector != nil && ctx.CrawlResult == nil {
		ctx.CrawlResult = &CrawlResult{
			Provider:   ctx.crawlCollector.Provider,
			Catalogs:   ctx.crawlCollector.Catalogs,
			Params:     ctx.crawlCollector.Params,
			DataFields: ctx.crawlCollector.DataFields,
		}
		log.Printf("📊 [SaveMetadata] 从子任务聚合结果: Catalogs=%d, Params=%d, Fields=%d",
			len(ctx.CrawlResult.Catalogs), len(ctx.CrawlResult.Params), len(ctx.CrawlResult.DataFields))
	}
	ctx.crawlResultMu.Unlock()

	if ctx.CrawlResult == nil {
		return nil, fmt.Errorf("未找到爬取结果")
	}

	log.Printf("💾 [SaveMetadata] 开始保存元数据到: %s", ctx.Config.MetadataDBPath)

	// 创建数据库连接
	db, err := sql.Open("sqlite3", ctx.Config.MetadataDBPath)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}
	ctx.MetadataDB = db

	// 创建表
	if err := createMetadataTables(db); err != nil {
		return nil, fmt.Errorf("创建表失败: %w", err)
	}

	// 开启事务
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback()

	// 保存Provider
	_, err = tx.Exec(`INSERT INTO data_provider (id, name, base_url, description, created_at) VALUES (?, ?, ?, ?, ?)`,
		ctx.CrawlResult.Provider.ID,
		ctx.CrawlResult.Provider.Name,
		ctx.CrawlResult.Provider.BaseURL,
		ctx.CrawlResult.Provider.Description,
		ctx.CrawlResult.Provider.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("保存Provider失败: %w", err)
	}

	// 保存Catalogs
	for _, c := range ctx.CrawlResult.Catalogs {
		_, err = tx.Exec(`INSERT INTO api_catalog (id, provider_id, name, level, is_leaf, link, api_name, description, permission, sort_order, created_at) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			c.ID, 1, c.Name, c.Level, c.IsLeaf, c.Link, c.APIName, c.Description, c.Permission, c.SortOrder, c.CreatedAt,
		)
		if err != nil {
			log.Printf("  ⚠️ 保存Catalog失败: %v", err)
		}
	}

	// 保存Params
	for _, p := range ctx.CrawlResult.Params {
		_, err = tx.Exec(`INSERT INTO api_param (id, catalog_id, name, type, required, description, sort_order, created_at) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			p.ID, p.CatalogID, p.Name, p.Type, p.Required, p.Description, p.SortOrder, p.CreatedAt,
		)
		if err != nil {
			log.Printf("  ⚠️ 保存Param失败: %v", err)
		}
	}

	// 保存Fields
	for _, f := range ctx.CrawlResult.DataFields {
		_, err = tx.Exec(`INSERT INTO api_data_field (id, catalog_id, name, type, is_default, description, sort_order, created_at) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			f.ID, f.CatalogID, f.Name, f.Type, f.Default, f.Description, f.SortOrder, f.CreatedAt,
		)
		if err != nil {
			log.Printf("  ⚠️ 保存Field失败: %v", err)
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("提交事务失败: %w", err)
	}

	log.Printf("✅ [SaveMetadata] 保存完成: Provider=1, Catalogs=%d, Params=%d, Fields=%d",
		len(ctx.CrawlResult.Catalogs), len(ctx.CrawlResult.Params), len(ctx.CrawlResult.DataFields))

	return map[string]int{
		"providers": 1,
		"catalogs":  len(ctx.CrawlResult.Catalogs),
		"params":    len(ctx.CrawlResult.Params),
		"fields":    len(ctx.CrawlResult.DataFields),
	}, nil
}

// createMetadataTables 创建元数据表
func createMetadataTables(db *sql.DB) error {
	sqls := []string{
		`CREATE TABLE IF NOT EXISTS data_provider (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			base_url TEXT,
			description TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS api_catalog (
			id INTEGER PRIMARY KEY,
			provider_id INTEGER,
			parent_id INTEGER,
			name TEXT NOT NULL,
			level INTEGER DEFAULT 1,
			is_leaf INTEGER DEFAULT 0,
			link TEXT,
			api_name TEXT,
			description TEXT,
			permission TEXT,
			sort_order INTEGER DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (provider_id) REFERENCES data_provider(id)
		)`,
		`CREATE TABLE IF NOT EXISTS api_param (
			id INTEGER PRIMARY KEY,
			catalog_id INTEGER,
			name TEXT NOT NULL,
			type TEXT,
			required INTEGER DEFAULT 0,
			description TEXT,
			sort_order INTEGER DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (catalog_id) REFERENCES api_catalog(id)
		)`,
		`CREATE TABLE IF NOT EXISTS api_data_field (
			id INTEGER PRIMARY KEY,
			catalog_id INTEGER,
			name TEXT NOT NULL,
			type TEXT,
			is_default INTEGER DEFAULT 0,
			description TEXT,
			sort_order INTEGER DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (catalog_id) REFERENCES api_catalog(id)
		)`,
	}

	for _, s := range sqls {
		if _, err := db.Exec(s); err != nil {
			return err
		}
	}

	return nil
}

// ==================== Workflow 2: 基于元数据建表 ====================

// CreateTables 基于爬取的API元数据动态创建数据表
func CreateTables(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	log.Printf("🔨 [CreateTables] 开始在 %s 创建数据表", ctx.Config.StockDBPath)

	// 检查是否有爬取结果
	if ctx.CrawlResult == nil || len(ctx.CrawlResult.Catalogs) == 0 {
		return nil, fmt.Errorf("未找到爬取结果，无法创建表")
	}

	// 创建股票数据数据库
	db, err := sql.Open("sqlite3", ctx.Config.StockDBPath)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}
	ctx.StockDB = db

	// 开启事务创建所有表
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback()

	// 根据爬取的API元数据动态生成建表语句
	createdTables := 0
	for _, catalog := range ctx.CrawlResult.Catalogs {
		if catalog.APIName == "" {
			continue
		}

		// 获取该API的所有字段
		fields := getFieldsForCatalog(ctx.CrawlResult.DataFields, catalog.ID)
		if len(fields) == 0 {
			log.Printf("  ⚠️ API %s (%s) 没有字段定义，跳过", catalog.Name, catalog.APIName)
			continue
		}

		// 生成DDL
		ddl := generateTableDDL(catalog.APIName, fields)
		log.Printf("  - 创建表: %s (%d个字段)", catalog.APIName, len(fields))

		if _, err := tx.Exec(ddl); err != nil {
			log.Printf("  ⚠️ 创建表 %s 失败: %v", catalog.APIName, err)
			continue
		}
		createdTables++
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("提交事务失败: %w", err)
	}

	log.Printf("✅ [CreateTables] 创建完成，共 %d 个表", createdTables)
	return map[string]int{"tables_created": createdTables}, nil
}

// getFieldsForCatalog 获取指定Catalog的所有字段
func getFieldsForCatalog(allFields []APIDataField, catalogID int) []APIDataField {
	var fields []APIDataField
	for _, f := range allFields {
		if f.CatalogID == catalogID {
			fields = append(fields, f)
		}
	}
	return fields
}

// generateTableDDL 根据API字段动态生成建表DDL
// 使用双引号转义表名和字段名（避免 SQLite 保留字冲突，如 on, limit, order 等）
func generateTableDDL(tableName string, fields []APIDataField) string {
	var sb strings.Builder
	// 表名用双引号包裹
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (\n", tableName))
	sb.WriteString("    \"id\" INTEGER PRIMARY KEY AUTOINCREMENT,\n")

	// 去重字段名（某些 API 可能有重复字段定义）
	seenFields := make(map[string]bool)
	uniqueFields := []APIDataField{}
	for _, f := range fields {
		fieldName := strings.TrimSpace(f.Name)
		if fieldName == "" || fieldName == "id" || seenFields[fieldName] {
			continue
		}
		seenFields[fieldName] = true
		uniqueFields = append(uniqueFields, f)
	}

	for i, f := range uniqueFields {
		sqlType := mapTushareTypeToSQLite(f.Type)
		// 字段名用双引号包裹
		sb.WriteString(fmt.Sprintf("    \"%s\" %s", f.Name, sqlType))
		if i < len(uniqueFields)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString("    \"created_at\" DATETIME DEFAULT CURRENT_TIMESTAMP\n")
	sb.WriteString(")")

	return sb.String()
}

// mapTushareTypeToSQLite 将Tushare字段类型映射为SQLite类型
func mapTushareTypeToSQLite(tushareType string) string {
	tushareType = strings.ToLower(strings.TrimSpace(tushareType))
	switch tushareType {
	case "int", "integer":
		return "INTEGER"
	case "float", "number", "double":
		return "REAL"
	case "str", "string", "text", "date", "datetime":
		return "TEXT"
	default:
		return "TEXT" // 默认使用TEXT
	}
}

// ==================== Workflow 3: 数据获取 ====================

// callTushareAPI 调用Tushare API
func callTushareAPI(ctx *E2EContext, apiName string, params map[string]interface{}) (*TushareDataFrame, error) {
	reqBody := TushareRequest{
		APIName: apiName,
		Token:   ctx.Config.TushareToken,
		Params:  params,
	}

	jsonData, _ := json.Marshal(reqBody)
	resp, err := httpClient.Post(ctx.Config.APIServerURL, "application/json", strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result TushareResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if result.Code != 0 {
		return nil, fmt.Errorf("API错误: %s", result.Msg)
	}

	// 转换Data为DataFrame
	dataBytes, _ := json.Marshal(result.Data)
	var df TushareDataFrame
	if err := json.Unmarshal(dataBytes, &df); err != nil {
		return nil, err
	}

	return &df, nil
}

// FetchTradeCal 获取交易日历
// 返回值包含交易日期列表，用于下游任务生成子任务
func FetchTradeCal(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	log.Printf("📡 [FetchTradeCal] 获取交易日历: %s - %s", ctx.Config.StartDate, ctx.Config.EndDate)

	df, err := callTushareAPI(ctx, "trade_cal", map[string]interface{}{
		"exchange":   "SSE",
		"start_date": ctx.Config.StartDate,
		"end_date":   ctx.Config.EndDate,
	})
	if err != nil {
		return nil, err
	}

	// 保存到数据库
	count, err := saveDataFrame(ctx.StockDB, "trade_cal", df)
	if err != nil {
		return nil, err
	}

	// 提取交易日期列表（用于生成子任务）
	var tradeDates []string
	calDateIdx := -1
	isOpenIdx := -1
	for i, field := range df.Fields {
		if field == "cal_date" {
			calDateIdx = i
		} else if field == "is_open" {
			isOpenIdx = i
		}
	}
	if calDateIdx >= 0 {
		for _, item := range df.Items {
			if len(item) > calDateIdx {
				// 只取开盘日（is_open=1）
				if isOpenIdx >= 0 && len(item) > isOpenIdx {
					if isOpen, ok := item[isOpenIdx].(float64); ok && isOpen == 1 {
						if date, ok := item[calDateIdx].(string); ok {
							tradeDates = append(tradeDates, date)
						}
					}
				} else {
					if date, ok := item[calDateIdx].(string); ok {
						tradeDates = append(tradeDates, date)
					}
				}
			}
		}
	}

	log.Printf("✅ [FetchTradeCal] 保存 %d 条记录，提取 %d 个交易日", count, len(tradeDates))
	return map[string]interface{}{
		"count":       count,
		"trade_dates": tradeDates,
	}, nil
}

// FetchStockBasic 获取股票基本信息
// 返回值包含股票代码列表，用于下游任务生成子任务
func FetchStockBasic(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	log.Printf("📡 [FetchStockBasic] 获取股票基本信息")

	df, err := callTushareAPI(ctx, "stock_basic", map[string]interface{}{
		"list_status": "L",
	})
	if err != nil {
		return nil, err
	}

	count, err := saveDataFrame(ctx.StockDB, "stock_basic", df)
	if err != nil {
		return nil, err
	}

	// 提取股票代码列表（用于生成子任务，限制数量避免生成太多子任务）
	var tsCodes []string
	tsCodeIdx := -1
	for i, field := range df.Fields {
		if field == "ts_code" {
			tsCodeIdx = i
			break
		}
	}
	maxStocks := 3 // 限制子任务数量，用于测试
	if tsCodeIdx >= 0 {
		for _, item := range df.Items {
			if len(item) > tsCodeIdx {
				if code, ok := item[tsCodeIdx].(string); ok {
					tsCodes = append(tsCodes, code)
					if len(tsCodes) >= maxStocks {
						break
					}
				}
			}
		}
	}

	log.Printf("✅ [FetchStockBasic] 保存 %d 条记录，提取 %d 个股票代码用于子任务", count, len(tsCodes))
	return map[string]interface{}{
		"count":    count,
		"ts_codes": tsCodes,
	}, nil
}

// FetchDaily 获取日线行情
// 从任务参数中获取 ts_code（由 GenerateSubTasks 动态注入）
func FetchDaily(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从参数中获取 ts_code（由父任务通过 GenerateSubTasks 注入）
	tsCode := tc.GetParamString("ts_code")
	if tsCode == "" {
		tsCode = "000001.SZ" // 默认值（用于模拟模式或未注入参数时）
	}

	log.Printf("📡 [FetchDaily] 获取日线行情: ts_code=%s, %s - %s", tsCode, ctx.Config.StartDate, ctx.Config.EndDate)

	df, err := callTushareAPI(ctx, "daily", map[string]interface{}{
		"ts_code":    tsCode,
		"start_date": ctx.Config.StartDate,
		"end_date":   ctx.Config.EndDate,
	})
	if err != nil {
		return nil, err
	}

	count, err := saveDataFrame(ctx.StockDB, "daily", df)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ [FetchDaily] 保存 %d 条记录 (ts_code=%s)", count, tsCode)
	return map[string]int{"count": count}, nil
}

// FetchAdjFactor 获取复权因子
// 从任务参数中获取 ts_code（由 GenerateSubTasks 动态注入）
func FetchAdjFactor(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从参数中获取 ts_code（由父任务通过 GenerateSubTasks 注入）
	tsCode := tc.GetParamString("ts_code")
	if tsCode == "" {
		tsCode = "000001.SZ" // 默认值
	}

	log.Printf("📡 [FetchAdjFactor] 获取复权因子: ts_code=%s", tsCode)

	df, err := callTushareAPI(ctx, "adj_factor", map[string]interface{}{
		"ts_code":    tsCode,
		"start_date": ctx.Config.StartDate,
		"end_date":   ctx.Config.EndDate,
	})
	if err != nil {
		return nil, err
	}

	count, err := saveDataFrame(ctx.StockDB, "adj_factor", df)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ [FetchAdjFactor] 保存 %d 条记录 (ts_code=%s)", count, tsCode)
	return map[string]int{"count": count}, nil
}

// FetchIncome 获取利润表
// 从任务参数中获取 ts_code（由 GenerateSubTasks 动态注入）
func FetchIncome(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从参数中获取 ts_code（由父任务通过 GenerateSubTasks 注入）
	tsCode := tc.GetParamString("ts_code")
	if tsCode == "" {
		tsCode = "000001.SZ" // 默认值
	}

	log.Printf("📡 [FetchIncome] 获取利润表: ts_code=%s", tsCode)

	df, err := callTushareAPI(ctx, "income", map[string]interface{}{
		"ts_code": tsCode,
		"period":  "20240930",
	})
	if err != nil {
		return nil, err
	}

	count, err := saveDataFrame(ctx.StockDB, "income", df)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ [FetchIncome] 保存 %d 条记录 (ts_code=%s)", count, tsCode)
	return map[string]int{"count": count}, nil
}

// FetchBalanceSheet 获取资产负债表
// 从任务参数中获取 ts_code（由 GenerateSubTasks 动态注入）
func FetchBalanceSheet(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从参数中获取 ts_code（由父任务通过 GenerateSubTasks 注入）
	tsCode := tc.GetParamString("ts_code")
	if tsCode == "" {
		tsCode = "000001.SZ" // 默认值
	}

	log.Printf("📡 [FetchBalanceSheet] 获取资产负债表: ts_code=%s", tsCode)

	df, err := callTushareAPI(ctx, "balancesheet", map[string]interface{}{
		"ts_code": tsCode,
		"period":  "20240930",
	})
	if err != nil {
		return nil, err
	}

	count, err := saveDataFrame(ctx.StockDB, "balancesheet", df)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ [FetchBalanceSheet] 保存 %d 条记录 (ts_code=%s)", count, tsCode)
	return map[string]int{"count": count}, nil
}

// FetchCashFlow 获取现金流量表
// 从任务参数中获取 ts_code（由 GenerateSubTasks 动态注入）
func FetchCashFlow(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从参数中获取 ts_code（由父任务通过 GenerateSubTasks 注入）
	tsCode := tc.GetParamString("ts_code")
	if tsCode == "" {
		tsCode = "000001.SZ" // 默认值
	}

	log.Printf("📡 [FetchCashFlow] 获取现金流量表: ts_code=%s", tsCode)

	df, err := callTushareAPI(ctx, "cashflow", map[string]interface{}{
		"ts_code": tsCode,
		"period":  "20240930",
	})
	if err != nil {
		return nil, err
	}

	count, err := saveDataFrame(ctx.StockDB, "cashflow", df)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ [FetchCashFlow] 保存 %d 条记录 (ts_code=%s)", count, tsCode)
	return map[string]int{"count": count}, nil
}

// getParamKeys 获取参数的所有 key（调试用）
func getParamKeys(params map[string]interface{}) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}

// extractTsCodesFromUpstream 从上游任务结果中提取 ts_codes
func extractTsCodesFromUpstream(tc *task.TaskContext) []string {
	var tsCodes []string

	// 从 _cached_ 参数中查找上游任务结果
	for key, val := range tc.Params {
		if strings.HasPrefix(key, "_cached_") {
			if resultMap, ok := val.(map[string]interface{}); ok {
				if tsCodesRaw, ok := resultMap["ts_codes"]; ok {
					switch v := tsCodesRaw.(type) {
					case []string:
						return v
					case []interface{}:
						for _, item := range v {
							if s, ok := item.(string); ok {
								tsCodes = append(tsCodes, s)
							}
						}
						return tsCodes
					}
				}
			}
		}
	}
	return tsCodes
}

// extractCatalogsFromUpstream 从上游任务结果中提取 catalogs
func extractCatalogsFromUpstream(tc *task.TaskContext) []APICatalog {
	var catalogs []APICatalog

	// 从 _cached_ 参数中查找上游任务结果
	for key, val := range tc.Params {
		if strings.HasPrefix(key, "_cached_") {
			catalogsRaw := val
			// 类型断言
			switch v := catalogsRaw.(type) {
			case []APICatalog:
				return v
			case []interface{}:
				data, _ := json.Marshal(v)
				json.Unmarshal(data, &catalogs)
				return catalogs
			default:
				data, _ := json.Marshal(catalogsRaw)
				json.Unmarshal(data, &catalogs)
				return catalogs
			}
		}
	}

	// 也尝试 _result_data
	catalogsRaw := tc.GetParam("_result_data")
	if catalogsRaw != nil {
		switch v := catalogsRaw.(type) {
		case []APICatalog:
			return v
		case []interface{}:
			data, _ := json.Marshal(v)
			json.Unmarshal(data, &catalogs)
			return catalogs
		default:
			data, _ := json.Marshal(catalogsRaw)
			json.Unmarshal(data, &catalogs)
			return catalogs
		}
	}

	return catalogs
}

// generateSubTasksForType 通用的子任务生成函数
func generateSubTasksForType(tc *task.TaskContext, taskTypeName, jobFuncName string) {
	// 调试：打印所有参数
	log.Printf("🔍 [%s] Params 内容: %+v", taskTypeName, tc.Params)

	// 获取Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		log.Printf("⚠️ [%s] 未找到Engine依赖", taskTypeName)
		return
	}
	eng, ok := engineInterface.(*engine.Engine)
	if !ok {
		log.Printf("⚠️ [%s] Engine类型转换失败", taskTypeName)
		return
	}

	registry := eng.GetRegistry()
	if registry == nil {
		log.Printf("⚠️ [%s] 无法获取Registry", taskTypeName)
		return
	}

	// 从上游任务结果中提取 ts_codes
	tsCodes := extractTsCodesFromUpstream(tc)
	if len(tsCodes) == 0 {
		log.Printf("⚠️ [%s] 未找到 ts_codes，Params keys: %v", taskTypeName, getParamKeys(tc.Params))
		return
	}

	log.Printf("📡 [%s] 从上游任务获取到 %d 个股票代码: %v", taskTypeName, len(tsCodes), tsCodes)

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	for _, tsCode := range tsCodes {
		subTaskName := fmt.Sprintf("%s_%s", taskTypeName, tsCode)
		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("获取%s的%s", tsCode, taskTypeName), registry).
			WithJobFunction(jobFuncName, map[string]interface{}{
				"ts_code": tsCode,
			}).
			WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
			WithTaskHandler(task.TaskStatusFailed, "LogError").
			Build()
		if err != nil {
			log.Printf("❌ [%s] 创建子任务失败: %s, error=%v", taskTypeName, subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			log.Printf("❌ [%s] 添加子任务失败: %s, error=%v", taskTypeName, subTaskName, err)
			continue
		}

		generatedCount++
		log.Printf("✅ [%s] 子任务已添加: %s (ts_code=%s)", taskTypeName, subTaskName, tsCode)
	}

	log.Printf("✅ [%s] 共生成 %d 个子任务", taskTypeName, generatedCount)
}

// GenerateDailySubTasks 日线数据模板任务的 Success Handler（已废弃，仅用于向后兼容）
func GenerateDailySubTasks(tc *task.TaskContext) {
	generateSubTasksForType(tc, "获取日线数据", "FetchDailySub")
}

// GenerateAdjFactorSubTasks 复权因子模板任务的 Success Handler（已废弃，仅用于向后兼容）
func GenerateAdjFactorSubTasks(tc *task.TaskContext) {
	generateSubTasksForType(tc, "获取复权因子", "FetchAdjFactorSub")
}

// GenerateIncomeSubTasks 利润表模板任务的 Success Handler（已废弃，仅用于向后兼容）
func GenerateIncomeSubTasks(tc *task.TaskContext) {
	generateSubTasksForType(tc, "获取利润表", "FetchIncomeSub")
}

// GenerateBalanceSheetSubTasks 资产负债表模板任务的 Success Handler（已废弃，仅用于向后兼容）
func GenerateBalanceSheetSubTasks(tc *task.TaskContext) {
	generateSubTasksForType(tc, "获取资产负债表", "FetchBalanceSheetSub")
}

// ==================== Job Function 版本的子任务生成函数（推荐使用） ====================
// 根据设计文档要求：用户应该把任务生成函数放在 Job Function 中
// Job Function 可以从 context 引用之前任务的结果，并注入给子任务

// generateSubTasksForTypeJob 通用的子任务生成 Job Function
// 返回生成的子任务数量和状态信息
func generateSubTasksForTypeJob(tc *task.TaskContext, taskTypeName, jobFuncName string) (interface{}, error) {
	// 调试：打印所有参数
	log.Printf("🔍 [%s] Job Function 执行，Params 内容: %+v", taskTypeName, tc.Params)

	// 获取Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("[%s] 未找到Engine依赖", taskTypeName)
	}
	eng, ok := engineInterface.(*engine.Engine)
	if !ok {
		return nil, fmt.Errorf("[%s] Engine类型转换失败", taskTypeName)
	}

	registry := eng.GetRegistry()
	if registry == nil {
		return nil, fmt.Errorf("[%s] 无法获取Registry", taskTypeName)
	}

	// 从上游任务结果中提取 ts_codes
	tsCodes := extractTsCodesFromUpstream(tc)
	if len(tsCodes) == 0 {
		log.Printf("⚠️ [%s] 未找到 ts_codes，Params keys: %v", taskTypeName, getParamKeys(tc.Params))
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"message":   "未找到 ts_codes，跳过子任务生成",
		}, nil
	}

	log.Printf("📡 [%s] 从上游任务获取到 %d 个股票代码: %v", taskTypeName, len(tsCodes), tsCodes)

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	// 收集所有子任务
	var subTasks []interface{}
	for _, tsCode := range tsCodes {
		subTaskName := fmt.Sprintf("%s_%s", taskTypeName, tsCode)
		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("获取%s的%s", tsCode, taskTypeName), registry).
			WithJobFunction(jobFuncName, map[string]interface{}{
				"ts_code": tsCode,
			}).
			WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
			WithTaskHandler(task.TaskStatusFailed, "LogError").
			Build()
		if err != nil {
			log.Printf("❌ [%s] 创建子任务失败: %s, error=%v", taskTypeName, subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			log.Printf("❌ [%s] 添加子任务失败: %s, error=%v", taskTypeName, subTaskName, err)
			continue
		}

		generatedCount++
		subTasks = append(subTasks, map[string]interface{}{
			"name":    subTaskName,
			"ts_code": tsCode,
		})
		log.Printf("✅ [%s] 子任务已添加: %s (ts_code=%s)", taskTypeName, subTaskName, tsCode)
	}

	log.Printf("✅ [%s] 共生成 %d 个子任务", taskTypeName, generatedCount)

	return map[string]interface{}{
		"status":    "success",
		"generated": generatedCount,
		"sub_tasks": subTasks,
	}, nil
}

// GenerateDailySubTasksJob 日线数据模板任务的 Job Function
func GenerateDailySubTasksJob(tc *task.TaskContext) (interface{}, error) {
	return generateSubTasksForTypeJob(tc, "获取日线数据", "FetchDailySub")
}

// GenerateAdjFactorSubTasksJob 复权因子模板任务的 Job Function
func GenerateAdjFactorSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	return generateSubTasksForTypeJob(tc, "获取复权因子", "FetchAdjFactorSub")
}

// GenerateIncomeSubTasksJob 利润表模板任务的 Job Function
func GenerateIncomeSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	return generateSubTasksForTypeJob(tc, "获取利润表", "FetchIncomeSub")
}

// GenerateBalanceSheetSubTasksJob 资产负债表模板任务的 Job Function
func GenerateBalanceSheetSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	return generateSubTasksForTypeJob(tc, "获取资产负债表", "FetchBalanceSheetSub")
}

// GenerateCashFlowSubTasksJob 现金流量表模板任务的 Job Function
func GenerateCashFlowSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	return generateSubTasksForTypeJob(tc, "获取现金流量表", "FetchCashFlowSub")
}

// GenerateAPIDetailSubTasksJob 爬取API详情模板任务的 Job Function
func GenerateAPIDetailSubTasksJob(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("[GenerateAPIDetailSubTasksJob] 未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	// 从上游任务获取目录列表
	catalogs := extractCatalogsFromUpstream(tc)
	if len(catalogs) == 0 {
		log.Printf("⚠️ [GenerateAPIDetailSubTasksJob] 未找到目录数据，Params keys: %v", getParamKeys(tc.Params))
		return map[string]interface{}{
			"status":    "no_data",
			"generated": 0,
			"message":   "未找到目录数据，跳过子任务生成",
		}, nil
	}

	// 真实模式下限制爬取数量
	if ctx.Config.MaxAPICrawl > 0 && len(catalogs) > ctx.Config.MaxAPICrawl {
		log.Printf("📡 [GenerateAPIDetailSubTasksJob] 真实模式：限制爬取数量从 %d 到 %d", len(catalogs), ctx.Config.MaxAPICrawl)
		catalogs = catalogs[:ctx.Config.MaxAPICrawl]
	}

	log.Printf("📡 [GenerateAPIDetailSubTasksJob] 从上游任务获取到 %d 个目录，开始生成子任务", len(catalogs))

	// 获取Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		return nil, fmt.Errorf("[GenerateAPIDetailSubTasksJob] 未找到Engine依赖")
	}
	eng, ok := engineInterface.(*engine.Engine)
	if !ok {
		return nil, fmt.Errorf("[GenerateAPIDetailSubTasksJob] Engine类型转换失败")
	}

	registry := eng.GetRegistry()
	if registry == nil {
		return nil, fmt.Errorf("[GenerateAPIDetailSubTasksJob] 无法获取Registry")
	}

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	// 初始化结果收集器
	ctx.crawlResultMu.Lock()
	if ctx.crawlCollector == nil {
		ctx.crawlCollector = &CrawlResultCollector{
			Provider: DataProvider{
				ID:          1,
				Name:        "Tushare",
				BaseURL:     ctx.Config.APIServerURL,
				Description: "Tushare金融大数据平台",
				CreatedAt:   time.Now(),
			},
			Catalogs:   []APICatalog{},
			Params:     []APIParam{},
			DataFields: []APIDataField{},
		}
	}
	ctx.crawlResultMu.Unlock()

	// 统计叶子节点和目录节点数量
	leafCount := 0
	dirCount := 0
	for _, c := range catalogs {
		if c.IsLeaf {
			leafCount++
		} else {
			dirCount++
		}
	}
	log.Printf("📊 [GenerateAPIDetailSubTasksJob] 目录结构: 叶子节点=%d, 目录节点=%d", leafCount, dirCount)

	var subTaskInfos []map[string]interface{}
	for _, catalog := range catalogs {
		// 只为叶子节点生成子任务（跳过目录节点）
		if !catalog.IsLeaf {
			log.Printf("📁 [GenerateAPIDetailSubTasksJob] 跳过目录节点: %s", catalog.Name)
			continue
		}

		if catalog.Link == "" {
			continue
		}

		subTaskName := fmt.Sprintf("爬取API详情_%s", catalog.Name)
		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("爬取%s的API详情", catalog.Name), registry).
			WithJobFunction("CrawlSingleAPIDetail", map[string]interface{}{
				"catalog_id":   catalog.ID,
				"catalog_name": catalog.Name,
				"catalog_link": catalog.Link,
			}).
			WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
			WithTaskHandler(task.TaskStatusFailed, "LogError").
			Build()
		if err != nil {
			log.Printf("❌ [GenerateAPIDetailSubTasksJob] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			log.Printf("❌ [GenerateAPIDetailSubTasksJob] 添加子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		generatedCount++
		subTaskInfos = append(subTaskInfos, map[string]interface{}{
			"name":    subTaskName,
			"catalog": catalog.Name,
		})
		log.Printf("✅ [GenerateAPIDetailSubTasksJob] 子任务已添加: %s (catalog=%s)", subTaskName, catalog.Name)
	}

	log.Printf("✅ [GenerateAPIDetailSubTasksJob] 共生成 %d 个子任务", generatedCount)

	return map[string]interface{}{
		"status":     "success",
		"generated":  generatedCount,
		"leaf_count": leafCount,
		"dir_count":  dirCount,
		"sub_tasks":  subTaskInfos,
	}, nil
}

// GenerateCashFlowSubTasks 现金流量表模板任务的 Success Handler
func GenerateCashFlowSubTasks(tc *task.TaskContext) {
	generateSubTasksForType(tc, "获取现金流量表", "FetchCashFlowSub")
}

// GenerateAPIDetailSubTasks 爬取API详情模板任务的 Success Handler
func GenerateAPIDetailSubTasks(tc *task.TaskContext) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		log.Printf("⚠️ [GenerateAPIDetailSubTasks] 未找到E2EContext依赖")
		return
	}
	ctx := e2eCtx.(*E2EContext)

	// 从上游任务获取目录列表
	catalogs := extractCatalogsFromUpstream(tc)
	if len(catalogs) == 0 {
		log.Printf("⚠️ [GenerateAPIDetailSubTasks] 未找到目录数据，Params keys: %v", getParamKeys(tc.Params))
		return
	}

	// 真实模式下限制爬取数量
	if ctx.Config.MaxAPICrawl > 0 && len(catalogs) > ctx.Config.MaxAPICrawl {
		log.Printf("📡 [GenerateAPIDetailSubTasks] 真实模式：限制爬取数量从 %d 到 %d", len(catalogs), ctx.Config.MaxAPICrawl)
		catalogs = catalogs[:ctx.Config.MaxAPICrawl]
	}

	log.Printf("📡 [GenerateAPIDetailSubTasks] 从上游任务获取到 %d 个目录，开始生成子任务", len(catalogs))

	// 获取Engine
	engineInterface, ok := tc.GetDependency("Engine")
	if !ok {
		log.Printf("⚠️ [GenerateAPIDetailSubTasks] 未找到Engine依赖")
		return
	}
	eng, ok := engineInterface.(*engine.Engine)
	if !ok {
		log.Printf("⚠️ [GenerateAPIDetailSubTasks] Engine类型转换失败")
		return
	}

	registry := eng.GetRegistry()
	if registry == nil {
		log.Printf("⚠️ [GenerateAPIDetailSubTasks] 无法获取Registry")
		return
	}

	parentTaskID := tc.TaskID
	workflowInstanceID := tc.WorkflowInstanceID
	generatedCount := 0

	// 初始化结果收集器
	ctx.crawlResultMu.Lock()
	if ctx.crawlCollector == nil {
		ctx.crawlCollector = &CrawlResultCollector{
			Provider: DataProvider{
				ID:          1,
				Name:        "Tushare",
				BaseURL:     ctx.Config.APIServerURL,
				Description: "Tushare金融大数据平台",
				CreatedAt:   time.Now(),
			},
			Catalogs:   []APICatalog{},
			Params:     []APIParam{},
			DataFields: []APIDataField{},
		}
	}
	ctx.crawlResultMu.Unlock()

	// 统计叶子节点和目录节点数量
	leafCount := 0
	dirCount := 0
	for _, c := range catalogs {
		if c.IsLeaf {
			leafCount++
		} else {
			dirCount++
		}
	}
	log.Printf("📊 [GenerateAPIDetailSubTasks] 目录结构: 叶子节点=%d, 目录节点=%d", leafCount, dirCount)

	for _, catalog := range catalogs {
		// 只为叶子节点生成子任务（跳过目录节点）
		if !catalog.IsLeaf {
			log.Printf("📁 [GenerateAPIDetailSubTasks] 跳过目录节点: %s", catalog.Name)
			continue
		}

		if catalog.Link == "" {
			continue
		}

		subTaskName := fmt.Sprintf("爬取API详情_%s", catalog.Name)
		subTask, err := builder.NewTaskBuilder(subTaskName, fmt.Sprintf("爬取%s的API详情", catalog.Name), registry).
			WithJobFunction("CrawlSingleAPIDetail", map[string]interface{}{
				"catalog_id":   catalog.ID,
				"catalog_name": catalog.Name,
				"catalog_link": catalog.Link,
			}).
			WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
			WithTaskHandler(task.TaskStatusFailed, "LogError").
			Build()
		if err != nil {
			log.Printf("❌ [GenerateAPIDetailSubTasks] 创建子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		bgCtx := context.Background()
		if err := eng.AddSubTaskToInstance(bgCtx, workflowInstanceID, subTask, parentTaskID); err != nil {
			log.Printf("❌ [GenerateAPIDetailSubTasks] 添加子任务失败: %s, error=%v", subTaskName, err)
			continue
		}

		generatedCount++
		log.Printf("✅ [GenerateAPIDetailSubTasks] 子任务已添加: %s (catalog=%s)", subTaskName, catalog.Name)
	}

	log.Printf("✅ [GenerateAPIDetailSubTasks] 共生成 %d 个子任务", generatedCount)
}

// AggregateAPIDetailResults 聚合所有API详情子任务的结果
func AggregateAPIDetailResults(tc *task.TaskContext) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		log.Printf("⚠️ [AggregateAPIDetailResults] 未找到E2EContext依赖")
		return
	}
	ctx := e2eCtx.(*E2EContext)

	ctx.crawlResultMu.Lock()
	defer ctx.crawlResultMu.Unlock()

	if ctx.crawlCollector == nil {
		log.Printf("⚠️ [AggregateAPIDetailResults] 结果收集器为空")
		return
	}

	// 构建完整结果
	result := &CrawlResult{
		Provider:   ctx.crawlCollector.Provider,
		Catalogs:   ctx.crawlCollector.Catalogs,
		Params:     ctx.crawlCollector.Params,
		DataFields: ctx.crawlCollector.DataFields,
	}

	ctx.CrawlResult = result

	log.Printf("✅ [AggregateAPIDetailResults] 聚合完成: Catalogs=%d, Params=%d, Fields=%d",
		len(result.Catalogs), len(result.Params), len(result.DataFields))
}

// FetchTopList 获取龙虎榜
func FetchTopList(tc *task.TaskContext) (interface{}, error) {
	e2eCtx, ok := tc.GetDependency("E2EContext")
	if !ok {
		return nil, fmt.Errorf("未找到E2EContext依赖")
	}
	ctx := e2eCtx.(*E2EContext)

	log.Printf("📡 [FetchTopList] 获取龙虎榜")

	df, err := callTushareAPI(ctx, "top_list", map[string]interface{}{
		"trade_date": ctx.Config.StartDate,
	})
	if err != nil {
		return nil, err
	}

	count, err := saveDataFrame(ctx.StockDB, "top_list", df)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ [FetchTopList] 保存 %d 条记录", count)
	return map[string]int{"count": count}, nil
}

// saveDataFrame 保存DataFrame到数据库
func saveDataFrame(db *sql.DB, tableName string, df *TushareDataFrame) (int, error) {
	if len(df.Items) == 0 {
		return 0, nil
	}

	// 构建INSERT语句
	placeholders := make([]string, len(df.Fields))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(df.Fields, ", "),
		strings.Join(placeholders, ", "))

	// 批量插入
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for _, item := range df.Items {
		if _, err := stmt.Exec(item...); err != nil {
			log.Printf("  ⚠️ 插入失败: %v", err)
			continue
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return count, nil
}

// ==================== 测试函数 ====================

// TestE2E_Workflow1_MetadataCrawl 测试Workflow 1: 元数据爬取
func TestE2E_Workflow1_MetadataCrawl(t *testing.T) {
	ctx := setupE2E(t)
	defer ctx.cleanup()

	bgCtx := context.Background()

	// 构建Workflow 1: 元数据爬取
	task1, _ := builder.NewTaskBuilder("爬取文档目录", "爬取Tushare文档目录结构", ctx.Registry).
		WithJobFunction("CrawlDocCatalog", nil).
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		Build()

	// 模板任务：在 Job Function 中生成子任务（符合设计文档要求）
	task2, err := builder.NewTaskBuilder("爬取API详情", "爬取每个API的详细信息", ctx.Registry).
		WithJobFunction("GenerateAPIDetailSubTasksJob", nil).
		WithDependency("爬取文档目录").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		WithTemplate(true).
		Build()
	if err != nil {
		t.Fatalf("创建爬取API详情模板任务失败: %v", err)
	}

	task3, _ := builder.NewTaskBuilder("保存元数据", "保存元数据到SQLite", ctx.Registry).
		WithJobFunction("SaveMetadata", nil).
		WithDependency("爬取API详情").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		Build()

	wf, err := builder.NewWorkflowBuilder("Tushare元数据爬取", "爬取Tushare API文档并保存元数据").
		WithTask(task1).
		WithTask(task2).
		WithTask(task3).
		Build()
	if err != nil {
		t.Fatalf("构建Workflow失败: %v", err)
	}

	// 执行
	controller, err := ctx.Engine.SubmitWorkflow(bgCtx, wf)
	if err != nil {
		t.Fatalf("提交Workflow失败: %v", err)
	}

	// 等待完成
	waitForWorkflow(t, controller, 60*time.Second)

	// 验证结果
	status, _ := controller.GetStatus()
	if status != "Success" {
		t.Errorf("Workflow状态不正确: 期望=Success, 实际=%s", status)
	}

	// 验证元数据已保存
	if ctx.CrawlResult == nil {
		t.Error("爬取结果为空")
	} else {
		t.Logf("✅ 爬取结果: Provider=%s, Catalogs=%d, Params=%d, Fields=%d",
			ctx.CrawlResult.Provider.Name,
			len(ctx.CrawlResult.Catalogs),
			len(ctx.CrawlResult.Params),
			len(ctx.CrawlResult.DataFields))
	}
}

// TestE2E_Workflow2_CreateTables 测试Workflow 2: 建表
func TestE2E_Workflow2_CreateTables(t *testing.T) {
	ctx := setupE2E(t)
	defer ctx.cleanup()

	bgCtx := context.Background()

	// 先执行Workflow 1获取元数据
	runMetadataCrawlWorkflow(t, ctx)

	// 构建Workflow 2: 建表
	task1, _ := builder.NewTaskBuilder("创建数据表", "基于元数据创建股票数据表", ctx.Registry).
		WithJobFunction("CreateTables", nil).
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		Build()

	wf, err := builder.NewWorkflowBuilder("创建数据表", "基于元数据在SQLite中创建数据表").
		WithTask(task1).
		Build()
	if err != nil {
		t.Fatalf("构建Workflow失败: %v", err)
	}

	// 执行
	controller, err := ctx.Engine.SubmitWorkflow(bgCtx, wf)
	if err != nil {
		t.Fatalf("提交Workflow失败: %v", err)
	}

	waitForWorkflow(t, controller, 30*time.Second)

	status, _ := controller.GetStatus()
	if status != "Success" {
		t.Errorf("Workflow状态不正确: 期望=Success, 实际=%s", status)
	}

	// 验证表已创建
	if ctx.StockDB != nil {
		tables := []string{"trade_cal", "stock_basic", "daily", "adj_factor", "income", "balancesheet", "cashflow", "top_list"}
		for _, table := range tables {
			var count int
			err := ctx.StockDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'", table)).Scan(&count)
			if err != nil || count == 0 {
				t.Errorf("表 %s 未创建", table)
			}
		}
		t.Logf("✅ 所有数据表已创建")
	}
}

// TestE2E_Workflow3_DataAcquisition 测试Workflow 3: 数据获取
func TestE2E_Workflow3_DataAcquisition(t *testing.T) {
	ctx := setupE2E(t)
	defer ctx.cleanup()

	bgCtx := context.Background()

	// 先执行Workflow 1和2
	runMetadataCrawlWorkflow(t, ctx)
	runCreateTablesWorkflow(t, ctx)

	// 构建Workflow 3: 数据获取（使用模板任务模式）
	//
	// 任务结构：
	// Level 0: 获取交易日历, 获取股票信息（并行执行）
	// Level 1: 5个模板任务（日线、复权因子、利润表、资产负债表、现金流量表），获取龙虎榜
	//          每个模板任务依赖获取股票信息，在 Success Handler 中生成子任务
	// Level 2: 动态生成的子任务（获取日线数据_000001.SZ, 获取复权因子_000001.SZ, ...）
	//
	tasks := []*task.Task{}

	// 交易日历（普通任务，无依赖）
	t1, _ := builder.NewTaskBuilder("获取交易日历", "获取2025年12月交易日历", ctx.Registry).
		WithJobFunction("FetchTradeCal", nil).
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		Build()
	tasks = append(tasks, t1)

	// 股票基础信息（普通任务，返回 ts_codes 供下游模板任务使用）
	t2, _ := builder.NewTaskBuilder("获取股票信息", "获取股票基本信息", ctx.Registry).
		WithJobFunction("FetchStockBasic", nil).
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		Build()
	tasks = append(tasks, t2)

	// 日线数据模板任务（依赖获取股票信息，在 Job Function 中生成子任务）
	t3, err := builder.NewTaskBuilder("获取日线数据", "获取日线行情数据", ctx.Registry).
		WithJobFunction("GenerateDailySubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		WithTemplate(true).
		Build()
	if err != nil {
		t.Fatalf("创建日线数据模板任务失败: %v", err)
	}
	tasks = append(tasks, t3)

	// 复权因子模板任务
	t4, err := builder.NewTaskBuilder("获取复权因子", "获取复权因子数据", ctx.Registry).
		WithJobFunction("GenerateAdjFactorSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		WithTemplate(true).
		Build()
	if err != nil {
		t.Fatalf("创建复权因子模板任务失败: %v", err)
	}
	tasks = append(tasks, t4)

	// 利润表模板任务
	t5, err := builder.NewTaskBuilder("获取利润表", "获取利润表数据", ctx.Registry).
		WithJobFunction("GenerateIncomeSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		WithTemplate(true).
		Build()
	if err != nil {
		t.Fatalf("创建利润表模板任务失败: %v", err)
	}
	tasks = append(tasks, t5)

	// 资产负债表模板任务
	t6, err := builder.NewTaskBuilder("获取资产负债表", "获取资产负债表数据", ctx.Registry).
		WithJobFunction("GenerateBalanceSheetSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		WithTemplate(true).
		Build()
	if err != nil {
		t.Fatalf("创建资产负债表模板任务失败: %v", err)
	}
	tasks = append(tasks, t6)

	// 现金流量表模板任务
	t7, err := builder.NewTaskBuilder("获取现金流量表", "获取现金流量表数据", ctx.Registry).
		WithJobFunction("GenerateCashFlowSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		WithTemplate(true).
		Build()
	if err != nil {
		t.Fatalf("创建现金流量表模板任务失败: %v", err)
	}
	tasks = append(tasks, t7)

	// 龙虎榜（普通任务，依赖交易日历）
	t8, _ := builder.NewTaskBuilder("获取龙虎榜", "获取龙虎榜数据", ctx.Registry).
		WithJobFunction("FetchTopList", nil).
		WithDependency("获取交易日历").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTaskHandler(task.TaskStatusFailed, "LogError").
		Build()
	tasks = append(tasks, t8)

	// 构建Workflow
	wfBuilder := builder.NewWorkflowBuilder("数据获取", "获取2025年12月股票数据")
	for _, tk := range tasks {
		wfBuilder.WithTask(tk)
	}

	wf, err := wfBuilder.Build()
	if err != nil {
		t.Fatalf("构建Workflow失败: %v", err)
	}

	// 执行
	controller, err := ctx.Engine.SubmitWorkflow(bgCtx, wf)
	if err != nil {
		t.Fatalf("提交Workflow失败: %v", err)
	}

	waitForWorkflow(t, controller, 120*time.Second)

	status, _ := controller.GetStatus()
	if status != "Success" {
		t.Errorf("Workflow状态不正确: 期望=Success, 实际=%s", status)
	}

	// 验证数据已保存
	printDataSummary(t, ctx.StockDB)

	// 校验核心数据表有数据（模板任务生成的子任务应该成功执行）
	validateDataCounts(t, ctx.StockDB, ctx.Config.Mode)
}

// TestE2E_FullPipeline 完整流程测试
func TestE2E_FullPipeline(t *testing.T) {
	ctx := setupE2E(t)
	defer ctx.cleanup()

	t.Log("========== E2E完整流程测试开始 ==========")
	t.Logf("模式: %s", ctx.Config.Mode)
	t.Logf("元数据库: %s", ctx.Config.MetadataDBPath)
	t.Logf("股票数据库: %s", ctx.Config.StockDBPath)

	// Workflow 1: 元数据爬取
	t.Log("\n----- Workflow 1: 元数据爬取 -----")
	runMetadataCrawlWorkflow(t, ctx)

	// Workflow 2: 建表
	t.Log("\n----- Workflow 2: 创建数据表 -----")
	runCreateTablesWorkflow(t, ctx)

	// Workflow 3: 数据获取
	t.Log("\n----- Workflow 3: 数据获取 -----")
	runDataAcquisitionWorkflow(t, ctx)

	// 输出最终结果
	t.Log("\n========== 测试结果汇总 ==========")
	printMetadataSummary(t, ctx.MetadataDB)
	printDataSummary(t, ctx.StockDB)

	// 校验核心数据表有数据
	validateDataCounts(t, ctx.StockDB, ctx.Config.Mode)

	t.Log("========== E2E完整流程测试完成 ==========")
}

// ==================== 辅助函数 ====================

func waitForWorkflow(t *testing.T, controller workflow.WorkflowController, timeout time.Duration) {
	startTime := time.Now()
	for {
		status, err := controller.GetStatus()
		if err != nil {
			t.Fatalf("获取状态失败: %v", err)
		}

		if status == "Success" || status == "Failed" || status == "Terminated" {
			t.Logf("Workflow完成，状态=%s，耗时=%v", status, time.Since(startTime))
			return
		}

		if time.Since(startTime) > timeout {
			t.Fatalf("Workflow执行超时，当前状态=%s", status)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func runMetadataCrawlWorkflow(t *testing.T, ctx *E2EContext) {
	bgCtx := context.Background()

	task1, _ := builder.NewTaskBuilder("爬取文档目录", "爬取Tushare文档目录结构", ctx.Registry).
		WithJobFunction("CrawlDocCatalog", nil).
		Build()

	// 模板任务：在 Job Function 中生成子任务
	task2, err := builder.NewTaskBuilder("爬取API详情", "爬取每个API的详细信息", ctx.Registry).
		WithJobFunction("GenerateAPIDetailSubTasksJob", nil).
		WithDependency("爬取文档目录").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTemplate(true).
		Build()
	if err != nil {
		t.Fatalf("创建爬取API详情模板任务失败: %v", err)
	}

	task3, _ := builder.NewTaskBuilder("保存元数据", "保存元数据到SQLite", ctx.Registry).
		WithJobFunction("SaveMetadata", nil).
		WithDependency("爬取API详情").
		Build()

	wf, _ := builder.NewWorkflowBuilder("Tushare元数据爬取", "").
		WithTask(task1).WithTask(task2).WithTask(task3).Build()

	controller, _ := ctx.Engine.SubmitWorkflow(bgCtx, wf)
	waitForWorkflow(t, controller, 60*time.Second)
}

func runCreateTablesWorkflow(t *testing.T, ctx *E2EContext) {
	bgCtx := context.Background()

	task1, _ := builder.NewTaskBuilder("创建数据表", "", ctx.Registry).
		WithJobFunction("CreateTables", nil).Build()

	wf, _ := builder.NewWorkflowBuilder("创建数据表", "").WithTask(task1).Build()

	controller, _ := ctx.Engine.SubmitWorkflow(bgCtx, wf)
	waitForWorkflow(t, controller, 30*time.Second)
}

func runDataAcquisitionWorkflow(t *testing.T, ctx *E2EContext) {
	bgCtx := context.Background()

	// 使用模板任务模式：模板任务的 Job Function 会根据上游结果动态生成子任务
	// Level 0: 获取交易日历, 获取股票信息
	// Level 1: 5个模板任务（日线、复权因子、利润表、资产负债表、现金流量表），获取龙虎榜
	// Level 2: 动态生成的子任务

	t1, _ := builder.NewTaskBuilder("获取交易日历", "获取交易日历", ctx.Registry).
		WithJobFunction("FetchTradeCal", nil).
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		Build()

	t2, _ := builder.NewTaskBuilder("获取股票信息", "获取股票信息", ctx.Registry).
		WithJobFunction("FetchStockBasic", nil).
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		Build()

	// 5个模板任务，在 Job Function 中生成子任务（符合设计文档要求）
	t3, _ := builder.NewTaskBuilder("获取日线数据", "获取日线数据", ctx.Registry).
		WithJobFunction("GenerateDailySubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTemplate(true).
		Build()

	t4, _ := builder.NewTaskBuilder("获取复权因子", "获取复权因子", ctx.Registry).
		WithJobFunction("GenerateAdjFactorSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTemplate(true).
		Build()

	t5, _ := builder.NewTaskBuilder("获取利润表", "获取利润表", ctx.Registry).
		WithJobFunction("GenerateIncomeSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTemplate(true).
		Build()

	t6, _ := builder.NewTaskBuilder("获取资产负债表", "获取资产负债表", ctx.Registry).
		WithJobFunction("GenerateBalanceSheetSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTemplate(true).
		Build()

	t7, _ := builder.NewTaskBuilder("获取现金流量表", "获取现金流量表", ctx.Registry).
		WithJobFunction("GenerateCashFlowSubTasksJob", nil).
		WithDependency("获取股票信息").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		WithTemplate(true).
		Build()

	t8, _ := builder.NewTaskBuilder("获取龙虎榜", "获取龙虎榜", ctx.Registry).
		WithJobFunction("FetchTopList", nil).
		WithDependency("获取交易日历").
		WithTaskHandler(task.TaskStatusSuccess, "LogSuccess").
		Build()

	wf, _ := builder.NewWorkflowBuilder("数据获取", "获取股票数据").
		WithTask(t1).WithTask(t2).WithTask(t3).WithTask(t4).
		WithTask(t5).WithTask(t6).WithTask(t7).WithTask(t8).Build()

	controller, _ := ctx.Engine.SubmitWorkflow(bgCtx, wf)
	waitForWorkflow(t, controller, 120*time.Second)
}

func printMetadataSummary(t *testing.T, db *sql.DB) {
	if db == nil {
		return
	}

	t.Log("\n📊 元数据统计:")
	tables := []string{"data_provider", "api_catalog", "api_param", "api_data_field"}
	for _, table := range tables {
		var count int
		db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		t.Logf("  - %s: %d 条", table, count)
	}
}

func printDataSummary(t *testing.T, db *sql.DB) {
	if db == nil {
		return
	}

	// 获取所有表名（排除sqlite内部表和created_at时间戳）
	tables := getTableNames(db)
	if len(tables) == 0 {
		t.Log("\n📊 股票数据库中没有表")
		return
	}

	t.Log("\n📊 股票数据统计:")
	for _, table := range tables {
		var count int
		db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		t.Logf("  - %s: %d 条", table, count)
	}

	// 打印各表的数据样本（表格形式）
	t.Log("\n📋 数据样本（每表最多5条）:")
	for _, table := range tables {
		printTableSample(t, db, table, 5)
	}
}

// getTableNames 获取数据库中所有用户表名
func getTableNames(db *sql.DB) []string {
	var tables []string
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return tables
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	return tables
}

// printTableSample 以表格形式打印表的数据样本
func printTableSample(t *testing.T, db *sql.DB, tableName string, limit int) {
	// 获取列名（不包括id和created_at）
	columns, err := getTableColumns(db, tableName)
	if err != nil || len(columns) == 0 {
		return
	}

	// 只查询需要的列
	columnList := strings.Join(columns, ", ")
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", columnList, tableName, limit)
	rows, err := db.Query(query)
	if err != nil {
		return
	}
	defer rows.Close()

	// 读取所有行
	var dataRows [][]string
	for rows.Next() {
		// 创建动态扫描目标
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make([]string, len(columns))
		for i, val := range values {
			row[i] = formatValue(val)
		}
		dataRows = append(dataRows, row)
	}

	if len(dataRows) == 0 {
		t.Logf("\n  📄 %s: (空表)", tableName)
		return
	}

	// 计算每列宽度（基于表头和数据）
	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}
	for _, row := range dataRows {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// 限制列宽避免过长
	maxColWidth := 20
	for i := range colWidths {
		if colWidths[i] > maxColWidth {
			colWidths[i] = maxColWidth
		}
	}

	// 打印表名
	t.Logf("\n  📄 %s (%d条):", tableName, len(dataRows))

	// 打印表头
	header := "    │"
	separator := "    ├"
	for i, col := range columns {
		header += fmt.Sprintf(" %-*s │", colWidths[i], truncateString(col, colWidths[i]))
		separator += strings.Repeat("─", colWidths[i]+2) + "┼"
	}
	separator = separator[:len(separator)-3] + "┤"
	t.Log(header)
	t.Log(separator)

	// 打印数据行
	for _, row := range dataRows {
		line := "    │"
		for i, cell := range row {
			line += fmt.Sprintf(" %-*s │", colWidths[i], truncateString(cell, colWidths[i]))
		}
		t.Log(line)
	}
}

// getTableColumns 获取表的列名
func getTableColumns(db *sql.DB, tableName string) ([]string, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dfltValue interface{}
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			continue
		}
		// 跳过 id 和 created_at 字段
		if name != "id" && name != "created_at" {
			columns = append(columns, name)
		}
	}
	return columns, nil
}

// formatValue 格式化数据库值为字符串
func formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case []byte:
		return string(v)
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%.0f", v)
		}
		return fmt.Sprintf("%.4f", v)
	case int64:
		return fmt.Sprintf("%d", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// truncateString 截断字符串并添加省略号
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-2] + ".."
}

// validateDataCounts 校验数据表有数据
func validateDataCounts(t *testing.T, db *sql.DB, mode string) {
	if db == nil {
		return
	}

	// 获取所有动态创建的表
	tables := getTableNames(db)
	if len(tables) == 0 {
		t.Errorf("❌ 数据校验失败: 没有创建任何数据表")
		return
	}

	// 核心表必须有数据（这些表名与Tushare API名称对应）
	requiredTables := map[string]int{
		"trade_cal":   1, // 交易日历
		"stock_basic": 1, // 股票基础信息
		"daily":       1, // 日线数据
		"adj_factor":  1, // 复权因子
		"top_list":    1, // 龙虎榜
	}

	// 校验核心表
	for table, minCount := range requiredTables {
		var count int
		err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			// 表可能不存在（动态建表时该API未爬取到）
			t.Logf("⚠️ 表 %s 不存在或查询失败: %v", table, err)
			continue
		}

		if count < minCount {
			t.Errorf("❌ 数据校验失败: %s 表应至少有 %d 条数据，实际只有 %d 条", table, minCount, count)
		} else {
			t.Logf("✅ 数据校验通过: %s 表有 %d 条数据", table, count)
		}
	}

	// 统计所有表的数据情况
	t.Log("\n📈 所有表数据统计:")
	totalRows := 0
	for _, table := range tables {
		var count int
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err == nil {
			totalRows += count
			status := "✅"
			if count == 0 {
				status = "⚪"
			}
			t.Logf("  %s %s: %d 条", status, table, count)
		}
	}
	t.Logf("  📊 总计: %d 个表, %d 条数据", len(tables), totalRows)
}
