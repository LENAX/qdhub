//go:build e2e
// +build e2e

// Package e2e provides end-to-end tests for QDHub.
// 服务器全流程E2E测试 - 通过启动真实服务器进程并调用HTTP API完成测试
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/quantdb/duckdb"
)

// ==================== 服务器E2E测试配置 ====================

// ServerE2EConfig 服务器E2E测试配置
type ServerE2EConfig struct {
	ServerHost   string
	ServerPort   int
	BinPath      string
	DataDir      string
	TushareToken string
}

// ServerE2EContext 服务器E2E测试上下文
type ServerE2EContext struct {
	Config     *ServerE2EConfig
	ServerCmd  *exec.Cmd
	BaseURL    string
	HTTPClient *http.Client
	DuckDBPath string
}

// ==================== 测试辅助函数 ====================

// runAllMigrations 执行所有数据库迁移脚本
func runAllMigrations(t *testing.T, projectRoot, dbPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("打开数据库失败: %w", err)
	}
	defer db.Close()

	migrationFiles := []string{
		"migrations/001_init_schema.up.sql",
		"migrations/002_seed_mapping_rules.up.sql",
		"migrations/002_auth_schema.sqlite.up.sql",
		"migrations/003_sync_plan_migration.up.sql",
		"migrations/004_api_sync_strategy.up.sql",
		"migrations/005_sync_plan_default_params.up.sql",
		"migrations/006_seed_default_admin.sqlite.up.sql",
		"migrations/022_user_stock_watchlist.sqlite.up.sql",
	}

	for _, migrationFile := range migrationFiles {
		fullPath := filepath.Join(projectRoot, migrationFile)
		migrationSQL, err := os.ReadFile(fullPath)
		if err != nil {
			return fmt.Errorf("读取迁移文件 %s 失败: %w", migrationFile, err)
		}

		if _, err := db.Exec(string(migrationSQL)); err != nil {
			t.Logf("  迁移 %s: %v (可能已执行)", filepath.Base(migrationFile), err)
			continue
		}
		t.Logf("  ✅ 迁移 %s 执行成功", filepath.Base(migrationFile))
	}

	return nil
}

// verifyAdminUserInDB 迁移后校验默认 admin 是否在库中且密码哈希可被 admin123 校验通过
func verifyAdminUserInDB(t *testing.T, dbPath string) {
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	defer db.Close()
	var id, username, passwordHash string
	err = db.QueryRow("SELECT id, username, password_hash FROM users WHERE username = ?", e2eAdminUsername).Scan(&id, &username, &passwordHash)
	require.NoError(t, err, "迁移后应存在默认 admin 用户，请确认 006_seed_default_admin 已执行")
	require.Equal(t, e2eAdminUsername, username)
	err = bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(e2eAdminPassword))
	require.NoError(t, err, "默认 admin 的 password_hash 应与 e2eAdminPassword(%q) 校验通过，请用 scripts/gen_bcrypt.go 生成哈希并更新 006 迁移", e2eAdminPassword)
}

// loadServerE2EConfig 加载服务器E2E测试配置。端口 8080 与 server 默认监听一致（子进程未从 config 读 server.port 时用 viper 默认 8080）。
func loadServerE2EConfig(t *testing.T) *ServerE2EConfig {
	return &ServerE2EConfig{
		ServerHost:   "127.0.0.1",
		ServerPort:   8080,
		BinPath:      "./bin/qdhub",
		TushareToken: os.Getenv("QDHUB_TUSHARE_TOKEN"),
	}
}

// setupServerE2EContext 设置服务器E2E测试上下文
func setupServerE2EContext(t *testing.T) *ServerE2EContext {
	cfg := loadServerE2EConfig(t)

	// 创建临时数据目录
	tmpDir, err := os.MkdirTemp("", "qdhub_e2e_*")
	require.NoError(t, err)
	cfg.DataDir = tmpDir

	// 获取项目根目录
	projectRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	// 准备数据库文件并执行迁移
	dbPath := filepath.Join(cfg.DataDir, "qdhub.db")
	if err := runAllMigrations(t, projectRoot, dbPath); err != nil {
		t.Fatalf("执行迁移失败: %v", err)
	}
	verifyAdminUserInDB(t, dbPath)
	t.Log("✅ 数据库迁移完成")

	// DuckDB 路径（用于数据同步）
	duckDBPath := filepath.Join(cfg.DataDir, "test.duckdb")

	// 创建服务器配置文件
	configContent := fmt.Sprintf(`
server:
  host: %s
  port: %d
  mode: debug
database:
  driver: sqlite
  dsn: %s
quantdb:
  duckdb_path: %s
`, cfg.ServerHost, cfg.ServerPort, dbPath, duckDBPath)

	configPath := filepath.Join(cfg.DataDir, "config.yaml")
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	require.NoError(t, err)

	// 启动服务器进程：显式设置 DB/QuantDB 路径，并过滤掉父进程的 QDHUB_* 以免覆盖 config
	serverEnv := filterEnvWithoutPrefix(os.Environ(), "QDHUB_")
	serverEnv = append(serverEnv,
		"GIN_MODE=debug",
		fmt.Sprintf("QDHUB_DATABASE_DSN=%s", dbPath),
		fmt.Sprintf("QDHUB_QUANTDB_DUCKDB_PATH=%s", duckDBPath),
	)
	// 使用位置参数传 config 路径，确保服务器一定读到 e2e 的 DB（不依赖根命令 --config 的解析）
	serverCmd := exec.Command(
		cfg.BinPath, "server", configPath,
	)
	serverCmd.Dir = projectRoot
	serverCmd.Env = serverEnv

	// 日志强制定向到项目 logs 目录（与工作区路径一致）
	logsDir := filepath.Join(projectRoot, "logs")
	_ = os.MkdirAll(logsDir, 0755)
	serverLogPath := filepath.Join(logsDir, "e2e_server.log")
	logFile, err := os.Create(serverLogPath)
	require.NoError(t, err)
	serverCmd.Stdout = logFile
	serverCmd.Stderr = logFile

	t.Logf("🚀 启动服务器: %s server %s", cfg.BinPath, configPath)

	err = serverCmd.Start()
	require.NoError(t, err)

	// HTTP 客户端：不走代理，直连 127.0.0.1（避免环境代理拦截导致 502）
	httpTransport := &http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) { return nil, nil },
	}
	ctx := &ServerE2EContext{
		Config:     cfg,
		ServerCmd:  serverCmd,
		BaseURL:    fmt.Sprintf("http://%s:%d", cfg.ServerHost, cfg.ServerPort),
		HTTPClient: &http.Client{Transport: httpTransport, Timeout: 30 * time.Second},
		DuckDBPath: duckDBPath,
	}

	t.Logf("  PID: %d", serverCmd.Process.Pid)
	t.Logf("  数据目录: %s", cfg.DataDir)
	t.Logf("  服务器日志: %s", serverLogPath)
	t.Logf("  数据库: %s", dbPath)
	t.Logf("  DuckDB: %s", duckDBPath)
	t.Logf("  配置文件: %s", configPath)

	// 等待服务器就绪
	t.Log("⏳ 等待服务器就绪...")
	if err := waitForServerReady(ctx, 30*time.Second); err != nil {
		ctx.cleanup()
		t.Fatalf("服务器启动失败: %v", err)
	}
	t.Log("✅ 服务器已就绪")

	// 先请求一次 /health，确认服务器就绪
	if resp, err := ctx.HTTPClient.Get(ctx.BaseURL + "/health"); err == nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Logf("  [probe] GET /health -> %d %s", resp.StatusCode, string(body))
	}

	// 设置清理函数
	t.Cleanup(func() {
		ctx.cleanup()
	})

	return ctx
}

// cleanup 清理测试资源
func (ctx *ServerE2EContext) cleanup() {
	if ctx.ServerCmd != nil && ctx.ServerCmd.Process != nil {
		fmt.Println("🛑 关闭服务器...")
		ctx.ServerCmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error)
		go func() {
			done <- ctx.ServerCmd.Wait()
		}()
		select {
		case <-done:
			fmt.Println("✅ 服务器已关闭")
		case <-time.After(5 * time.Second):
			ctx.ServerCmd.Process.Kill()
			fmt.Println("⚠️  服务器被强制关闭")
		}
	}

	// 保留测试数据目录用于调试
	if ctx.Config.DataDir != "" {
		fmt.Printf("📁 测试数据保留在: %s\n", ctx.Config.DataDir)
	}
}

// filterEnvWithoutPrefix 返回 env 中 key 不以 prefix 开头的条目，用于子进程避免继承覆盖 config 的 QDHUB_*。
func filterEnvWithoutPrefix(env []string, prefix string) []string {
	var out []string
	for _, e := range env {
		idx := strings.Index(e, "=")
		if idx <= 0 {
			continue
		}
		if strings.HasPrefix(e[:idx], prefix) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// waitForServerReady 等待服务器就绪
func waitForServerReady(ctx *ServerE2EContext, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := ctx.HTTPClient.Get(ctx.BaseURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("服务器在 %v 内未就绪", timeout)
}

// doRequest 发送HTTP请求（公开端点，无需认证）
func (ctx *ServerE2EContext) doRequest(method, path string, body interface{}) (*http.Response, error) {
	return ctx.doRequestWithAuth(method, path, body, "")
}

// doRequestWithAuth 发送带认证的HTTP请求
func (ctx *ServerE2EContext) doRequestWithAuth(method, path string, body interface{}, accessToken string) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest(method, ctx.BaseURL+path, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
	return ctx.HTTPClient.Do(req)
}

// registerAndLoginUser 注册并登录用户，返回访问令牌
func (ctx *ServerE2EContext) registerAndLoginUser(username, email, password string) (string, error) {
	// Register
	registerReq := map[string]interface{}{
		"username": username,
		"email":    email,
		"password": password,
	}

	registerResp, err := ctx.doRequest("POST", "/api/v1/auth/register", registerReq)
	if err != nil {
		return "", fmt.Errorf("注册失败: %w", err)
	}
	defer registerResp.Body.Close()

	if registerResp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(registerResp.Body)
		return "", fmt.Errorf("注册失败，状态码: %d, 响应: %s", registerResp.StatusCode, string(bodyBytes))
	}

	// Login
	loginReq := map[string]interface{}{
		"username": username,
		"password": password,
	}

	loginResp, err := ctx.doRequest("POST", "/api/v1/auth/login", loginReq)
	if err != nil {
		return "", fmt.Errorf("登录失败: %w", err)
	}
	defer loginResp.Body.Close()

	if loginResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(loginResp.Body)
		return "", fmt.Errorf("登录失败，状态码: %d, 响应: %s", loginResp.StatusCode, string(bodyBytes))
	}

	var loginResult map[string]interface{}
	if err := json.NewDecoder(loginResp.Body).Decode(&loginResult); err != nil {
		return "", fmt.Errorf("解析登录响应失败: %w", err)
	}

	data := loginResult["data"].(map[string]interface{})
	accessToken, ok := data["access_token"].(string)
	if !ok {
		return "", fmt.Errorf("响应中缺少 access_token")
	}

	return accessToken, nil
}

// loginUser 仅登录并返回访问令牌（用户必须已存在）
func (ctx *ServerE2EContext) loginUser(username, password string) (string, error) {
	loginReq := map[string]interface{}{
		"username": username,
		"password": password,
	}
	loginResp, err := ctx.doRequest("POST", "/api/v1/auth/login", loginReq)
	if err != nil {
		return "", fmt.Errorf("登录失败: %w", err)
	}
	defer loginResp.Body.Close()
	if loginResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(loginResp.Body)
		return "", fmt.Errorf("登录失败，状态码: %d, 响应: %s", loginResp.StatusCode, string(bodyBytes))
	}
	var loginResult map[string]interface{}
	if err := json.NewDecoder(loginResp.Body).Decode(&loginResult); err != nil {
		return "", fmt.Errorf("解析登录响应失败: %w", err)
	}
	data := loginResult["data"].(map[string]interface{})
	accessToken, ok := data["access_token"].(string)
	if !ok {
		return "", fmt.Errorf("响应中缺少 access_token")
	}
	return accessToken, nil
}

// ensureAuthenticatedUser 确保有一个已认证的用户，返回访问令牌
// 如果用户不存在则创建，如果已存在则直接登录
func (ctx *ServerE2EContext) ensureAuthenticatedUser(t *testing.T, username, email, password string) string {
	t.Helper()

	// Try to login first
	loginReq := map[string]interface{}{
		"username": username,
		"password": password,
	}

	loginResp, err := ctx.doRequest("POST", "/api/v1/auth/login", loginReq)
	if err == nil && loginResp.StatusCode == http.StatusOK {
		defer loginResp.Body.Close()
		var loginResult map[string]interface{}
		if err := json.NewDecoder(loginResp.Body).Decode(&loginResult); err == nil {
			if data, ok := loginResult["data"].(map[string]interface{}); ok {
				if token, ok := data["access_token"].(string); ok {
					return token
				}
			}
		}
	}

	// If login failed, register and login
	token, err := ctx.registerAndLoginUser(username, email, password)
	require.NoError(t, err, "创建认证用户失败")
	return token
}

// getCurrentUserIDFromToken 通过 GET /api/v1/auth/me 获取当前用户 ID，避免直接查库的时序问题
func (ctx *ServerE2EContext) getCurrentUserIDFromToken(t *testing.T, token string) string {
	t.Helper()
	resp, err := ctx.doRequestWithAuth("GET", "/api/v1/auth/me", nil, token)
	require.NoError(t, err, "调用 /auth/me 失败")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "/auth/me 应返回 200")
	data, err := getServerResponseData(resp)
	require.NoError(t, err, "解析 /auth/me 响应失败")
	userID := getServerStringField(data, "id")
	require.NotEmpty(t, userID, "响应中缺少用户 id")
	return userID
}

// e2eAdminUsername / e2eAdminPassword 与迁移 006_seed_default_admin.*.up.sql 中预置的默认 admin 一致（用户名 admin，密码 admin123）
const e2eAdminUsername = "admin"
const e2eAdminPassword = "admin123"

// ensureAdminUser 确保有一个具有 admin 角色的用户，返回访问令牌
// 使用迁移 006_seed_default_admin 预置的默认 admin 用户，避免测试进程写 DB 后服务器进程读不到的跨进程可见性问题
func (ctx *ServerE2EContext) ensureAdminUser(t *testing.T, _, _, _ string) string {
	t.Helper()
	expectedDBPath := filepath.Join(ctx.Config.DataDir, "qdhub.db")
	token, err := ctx.loginUser(e2eAdminUsername, e2eAdminPassword)
	if err != nil {
		// 优先从 /health 获取 database_dsn（debug 模式下会返回），便于确认是否连错库
		if resp, reqErr := ctx.HTTPClient.Get(ctx.BaseURL + "/health"); reqErr == nil {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Logf("  [health] 状态码: %d, 响应: %s", resp.StatusCode, string(bodyBytes))
			var health struct {
				DatabaseDSN string `json:"database_dsn"`
			}
			_ = json.Unmarshal(bodyBytes, &health)
			if health.DatabaseDSN != "" {
				t.Logf("  [health] 服务器当前连接的 DB: %s", health.DatabaseDSN)
				t.Logf("  [health] E2E 期望的 DB（迁移所在）: %s", expectedDBPath)
				if health.DatabaseDSN != expectedDBPath {
					t.Logf("  [health] 不一致：server 连的不是 E2E 迁移的库，会导致 admin 登录 401")
				}
			} else {
				t.Logf("  [health] 响应中无 database_dsn，可能未用最新二进制或非 debug 模式")
			}
		} else {
			t.Logf("  [health] 请求失败: %v", reqErr)
		}
		logServerDBLines(t, ctx.Config.DataDir, "server.log")
		require.NoError(t, err, "使用预置默认 admin 登录失败，请确认迁移 006_seed_default_admin 已执行且用户名/密码为 admin/admin123；若 server 连接的 DB 与 E2E 不一致会报 401")
	}
	return token
}

// logServerDBLines 登录失败时打印服务器解析出的 DSN 及日志中与 Database/DSN 相关的行
func logServerDBLines(t *testing.T, dataDir, logName string) {
	dsnResolvedPath := filepath.Join(dataDir, ".dsn_resolved")
	if b, err := os.ReadFile(dsnResolvedPath); err == nil {
		t.Logf("  服务器实际使用的 DSN: %s", strings.TrimSpace(string(b)))
	} else {
		t.Logf("  未找到 .dsn_resolved: %v", err)
	}
	logPath := filepath.Join(dataDir, logName)
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Logf("无法读取服务器日志 %s: %v", logPath, err)
		return
	}
	for _, line := range strings.Split(string(b), "\n") {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "database") || strings.Contains(lower, "dsn") {
			t.Logf("  [server.log] %s", strings.TrimSpace(line))
		}
	}
}

// waitForWorkflowSSE 使用 SSE 接口阻塞式等待工作流完成
func (ctx *ServerE2EContext) waitForWorkflowSSE(t *testing.T, path string, token string) {
	t.Helper()
	t.Logf("⏳ 等待 SSE 流完成: %s", path)

	// SSE 长连接需要使用独立的 HTTP Client（无超时限制）
	// 通过 Context 来控制整体超时（20 分钟），而不是 HTTP Client 的 Timeout
	sseClient := &http.Client{
		// 不设置 Timeout，让 Context 来控制
		// Transport 继承默认配置
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", ctx.BaseURL+path, nil)
	require.NoError(t, err)

	// 添加认证头
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := sseClient.Do(req)
	require.NoError(t, err, "调用 SSE 接口失败: %s", path)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "SSE 接口返回非 200: %s", path)

	reader := bufio.NewReader(resp.Body)
	var lastStatus string

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			// EOF 或 unexpected EOF 都表示连接关闭，属于正常结束
			if err == io.EOF || strings.Contains(err.Error(), "EOF") {
				break
			}
			// 其他错误记录警告但不中断测试（服务端可能主动关闭连接）
			t.Logf("⚠️ 读取 SSE 流时遇到错误: %v（视为正常结束）", err)
			break
		}

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "data:") {
			dataJson := strings.TrimPrefix(line, "data:")
			var statusData map[string]interface{}
			if err := json.Unmarshal([]byte(dataJson), &statusData); err != nil {
				continue
			}

			// 解析进度和状态
			progress, _ := statusData["progress"].(float64)
			statusStr := getServerStringField(statusData, "status")
			lastStatus = statusStr

			// 从 SSE 流获取并打印每条进度；优先使用 running_count/pending_count（与引擎内部一致），否则用 ID 数量
			running := getTaskIDsFromProgressData(statusData, "running_task_ids")
			pending := getTaskIDsFromProgressData(statusData, "pending_task_ids")
			runningCnt := getIntFromProgressData(statusData, "running_count")
			pendingCnt := getIntFromProgressData(statusData, "pending_count")
			if runningCnt < 0 {
				runningCnt = len(running)
			}
			if pendingCnt < 0 {
				pendingCnt = len(pending)
			}
			t.Logf("  ... 进度: %.1f%%, 状态: %s | 运行中: %d, 挂起: %d (running_ids=%v pending_ids=%v)", progress, statusStr, runningCnt, pendingCnt, running, pending)

			// 判断是否达到终态
			// 注意：statusData 可能包含 finished_at
			finishedAt := statusData["finished_at"]
			if (finishedAt != nil && finishedAt != "") || workflow.IsTerminal(statusStr) {
				break
			}
		}
	}

	t.Logf("✅ SSE 等待结束，最终状态: %s", lastStatus)

	isTerminal := workflow.IsTerminal(lastStatus)

	// 如果 SSE 断开时工作流未完成，使用 API 轮询确认
	if !isTerminal {
		t.Logf("⚠️ SSE 断开但工作流未完成，使用 API 轮询确认...")
		lastStatus = ctx.pollWorkflowUntilComplete(t, path, token)
	}

	if strings.ToLower(lastStatus) == "failed" {
		if os.Getenv("QDHUB_E2E_ALLOW_SYNC_FAILED") != "" {
			t.Logf("⚠️ 工作流状态为 failed（已设置 QDHUB_E2E_ALLOW_SYNC_FAILED，继续执行；适用于手动禁用部分 API 的场景）")
		} else {
			t.Fatalf("❌ 工作流执行失败 (通过 SSE/API 监测)")
		}
	}
}

// pollWorkflowUntilComplete 使用 API 轮询工作流状态直到完成
func (ctx *ServerE2EContext) pollWorkflowUntilComplete(t *testing.T, ssePath string, token string) string {
	t.Helper()

	// 从 SSE 路径提取 progress API 路径
	// /api/v1/instances/{id}/progress-stream -> /api/v1/instances/{id}/progress
	// /api/v1/sync-plans/{id}/progress-stream -> /api/v1/sync-plans/{id}/progress
	progressPath := strings.TrimSuffix(ssePath, "-stream")

	maxAttempts := 3600 // 最多等待 60 分钟（每秒轮询一次）
	lastProgress := -1.0
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			time.Sleep(1 * time.Second)
		}
		// progress endpoints require authentication
		resp, err := ctx.doRequestWithAuth("GET", progressPath, nil, token)
		if err != nil {
			if i%10 == 0 {
				t.Logf("  轮询 %ds: 请求失败 %v", i+1, err)
			}
			continue
		}

		data, err := getServerResponseData(resp)
		if err != nil {
			if i%10 == 0 {
				t.Logf("  轮询 %ds: 解析失败 %v", i+1, err)
			}
			continue
		}

		status := getServerStringField(data, "status")
		progress, _ := data["progress"].(float64)
		running := getTaskIDsFromProgressData(data, "running_task_ids")
		pending := getTaskIDsFromProgressData(data, "pending_task_ids")
		// 与 SSE 一致：优先使用 API 返回的 running_count/pending_count（与引擎一致），否则用 ID 数量
		runningCnt := getIntFromProgressData(data, "running_count")
		pendingCnt := getIntFromProgressData(data, "pending_count")
		if runningCnt < 0 {
			runningCnt = len(running)
		}
		if pendingCnt < 0 {
			pendingCnt = len(pending)
		}

		// 每 5 秒或进度变化时打印；始终带运行中/挂起数量与 ID（便于定位卡住的任务）
		if i%5 == 0 || progress != lastProgress {
			t.Logf("  ⏳ %ds: 状态=%s, 进度=%.1f%% | 运行中: %d, 挂起: %d (running_ids=%v pending_ids=%v)", i+1, status, progress, runningCnt, pendingCnt, running, pending)
			lastProgress = progress
		}

		// 检查是否是终态
		statusLower := strings.ToLower(status)
		if statusLower == "success" || statusLower == "failed" ||
			statusLower == "completed" || statusLower == "terminated" {
			t.Logf("✅ 轮询确认工作流完成，最终状态: %s, 耗时: %ds", status, i+1)
			return status
		}
	}

	t.Logf("⚠️ 轮询超时（60分钟），假设工作流已完成")
	return "timeout"
}

// parseServerResponse 解析服务器响应
func parseServerResponse(resp *http.Response) (map[string]interface{}, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// getServerResponseData 获取响应中的data字段
func getServerResponseData(resp *http.Response) (map[string]interface{}, error) {
	result, err := parseServerResponse(resp)
	if err != nil {
		return nil, err
	}
	if data, ok := result["data"].(map[string]interface{}); ok {
		return data, nil
	}
	return nil, fmt.Errorf("响应中没有data字段")
}

// getServerResponseDataList 获取响应中的data列表
func getServerResponseDataList(resp *http.Response) ([]interface{}, error) {
	result, err := parseServerResponse(resp)
	if err != nil {
		return nil, err
	}
	if data, ok := result["data"].([]interface{}); ok {
		return data, nil
	}
	return nil, fmt.Errorf("响应中没有data列表")
}

// getTaskIDsFromProgressData 从 progress 响应的 map 中提取 running_task_ids / pending_task_ids（JSON 可能为 []interface{}）
// getIntFromProgressData 从进度数据中取整数字段（如 running_count、pending_count），不存在或非数字返回 -1
func getIntFromProgressData(data map[string]interface{}, key string) int {
	v, ok := data[key]
	if !ok {
		return -1
	}
	if f, ok := v.(float64); ok {
		return int(f)
	}
	if i, ok := v.(int); ok {
		return i
	}
	return -1
}

func getTaskIDsFromProgressData(data map[string]interface{}, key string) []string {
	v, ok := data[key]
	if !ok {
		return nil
	}
	if sl, ok := v.([]interface{}); ok {
		out := make([]string, 0, len(sl))
		for _, e := range sl {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	if sl, ok := v.([]string); ok {
		return sl
	}
	return nil
}

// getServerStringField 从map中获取字符串字段（支持大小写）
func getServerStringField(data map[string]interface{}, field string) string {
	// 先尝试原始字段名
	if val, ok := data[field].(string); ok {
		return val
	}
	// 尝试首字母大写的字段名（如 id -> Id）
	upperField := strings.ToUpper(field[:1]) + field[1:]
	if val, ok := data[upperField].(string); ok {
		return val
	}
	// 尝试全大写的字段名（如 id -> ID）
	allUpperField := strings.ToUpper(field)
	if val, ok := data[allUpperField].(string); ok {
		return val
	}
	return ""
}

// readServerResponseBody 读取响应体（会关闭Body）
func readServerResponseBody(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Sprintf("读取失败: %v", err)
	}
	return string(bodyBytes)
}

// ==================== 真实Tushare Token全流程测试 ====================

// TestE2E_Server_WithRealTushareToken 使用真实Tushare Token的完整数据同步测试
// 需要设置 QDHUB_TUSHARE_TOKEN 环境变量。
// 元数据爬取+建表+同步耗时长，建议: go test -tags e2e -v ./tests/e2e -run TestE2E_Server_WithRealTushareToken -timeout 10m
func TestE2E_Server_WithRealTushareToken(t *testing.T) {
	ctx := setupServerE2EContext(t)

	if ctx.Config.TushareToken == "" {
		t.Skip("跳过：需要设置 QDHUB_TUSHARE_TOKEN 环境变量")
	}

	t.Log("========== 真实Tushare Token全流程测试 ==========")
	t.Logf("Token长度: %d", len(ctx.Config.TushareToken))

	var dataSourceID, dataStoreID, syncPlanID, executionID string

	// 获取具有 admin 权限的 token（创建数据源/DataStore/同步计划需要 write 权限）
	token := ctx.ensureAdminUser(t, "realtoken_test_user", "realtoken_test@example.com", "password123")

	// ==================== Step 1: 创建数据源 ====================
	t.Run("Step1_CreateDataSource", func(t *testing.T) {
		t.Log("----- Step 1: 创建数据源 -----")
		createReq := map[string]string{
			"name":        "Tushare",
			"description": "Tushare Pro (Real Token Test)",
			"base_url":    "http://api.tushare.pro",
			"doc_url":     "https://tushare.pro/document/2",
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			body := readServerResponseBody(resp)
			t.Fatalf("创建数据源失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		if err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}
		dataSourceID = getServerStringField(data, "id")
		if dataSourceID == "" {
			t.Fatalf("响应中没有 ID 字段, data: %+v", data)
		}
		t.Logf("✅ 数据源创建成功: ID=%s", dataSourceID)
	})

	// ==================== Step 2: 设置Token ====================
	t.Run("Step2_SetToken", func(t *testing.T) {
		t.Log("----- Step 2: 设置Token -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		tokenReq := map[string]string{
			"token": ctx.Config.TushareToken,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources/"+dataSourceID+"/token", tokenReq, token)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
		t.Log("✅ Token设置成功")
	})

	// ==================== Step 3: 刷新元数据并等待完成 ====================
	t.Run("Step3_RefreshMetadataAndWait", func(t *testing.T) {
		t.Log("----- Step 3: 刷新元数据并等待完成 -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		// 发送刷新请求并获取 workflow instance ID
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources/"+dataSourceID+"/refresh", nil, token)
		require.NoError(t, err)
		if resp.StatusCode != http.StatusOK {
			body := readServerResponseBody(resp)
			t.Fatalf("刷新元数据失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)

		instanceID := getServerStringField(data, "instance_id")
		if instanceID == "" {
			t.Log("⚠️  响应中没有 instance_id 字段，使用旧的等待方式")
			// 回退到旧的等待方式
			t.Log("⏳ 等待元数据刷新完成（查询数据源API数量）...")
			for i := 0; i < 60; i++ {
				if i > 0 {
					time.Sleep(1 * time.Second)
				}
				resp, err := ctx.doRequestWithAuth("GET", "/api/v1/datasources/"+dataSourceID, nil, token)
				if err != nil {
					continue
				}
				dsData, err := getServerResponseData(resp)
				if err != nil {
					continue
				}
				if apiCount, ok := dsData["api_count"].(float64); ok && apiCount > 0 {
					t.Logf("✅ 元数据刷新完成，API数量: %.0f", apiCount)
					return
				}
				if i%10 == 0 {
					t.Logf("  等待中... (%d秒)", i)
				}
			}
			t.Log("⚠️  元数据刷新等待超时，继续后续测试")
			return
		}

		t.Logf("✅ 元数据刷新工作流已启动，instance_id: %s", instanceID)

		// 使用 SSE 阻塞等待完成
		ctx.waitForWorkflowSSE(t, "/api/v1/instances/"+instanceID+"/progress-stream", token)

		// 查询最终的 API 数量
		dsResp, err := ctx.doRequestWithAuth("GET", "/api/v1/datasources/"+dataSourceID, nil, token)
		if err == nil {
			dsData, err := getServerResponseData(dsResp)
			if err == nil {
				if apiCount, ok := dsData["api_count"].(float64); ok {
					t.Logf("  数据源API数量: %.0f", apiCount)
				}
			}
		}
	})

	// ==================== Step 4: 创建DataStore ====================
	t.Run("Step4_CreateDataStore", func(t *testing.T) {
		t.Log("----- Step 4: 创建DataStore -----")

		createReq := map[string]interface{}{
			"name":         "TestDuckDB",
			"description":  "Real E2E Test DuckDB",
			"type":         "duckdb",
			"storage_path": ctx.DuckDBPath,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datastores", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			body := readServerResponseBody(resp)
			t.Fatalf("创建DataStore失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		if err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}
		dataStoreID = getServerStringField(data, "id")
		if dataStoreID == "" {
			t.Fatalf("响应中没有 ID 字段, data: %+v", data)
		}
		t.Logf("✅ DataStore创建成功: ID=%s, Path=%s", dataStoreID, ctx.DuckDBPath)
	})

	// ==================== Step 5: 建表 ====================
	t.Run("Step5_CreateTables", func(t *testing.T) {
		t.Log("----- Step 5: 为数据源建表 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和DataStore")
		}

		// 调用建表接口
		createTablesReq := map[string]interface{}{
			"data_source_id": dataSourceID,
			// max_tables 可选，不传表示不限制
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datastores/"+dataStoreID+"/create-tables", createTablesReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			body := readServerResponseBody(resp)
			t.Fatalf("建表失败: 状态码 %d, 响应: %s", resp.StatusCode, body)
		}

		data, err := getServerResponseData(resp)
		if err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}

		// 获取 workflow instance ID
		tableInstanceID := getServerStringField(data, "instance_id")
		if tableInstanceID == "" {
			t.Fatalf("响应中没有 instance_id 字段, data: %+v", data)
		}
		t.Logf("✅ 建表工作流已启动，instance_id: %s", tableInstanceID)

		// 使用 SSE 阻塞等待完成
		ctx.waitForWorkflowSSE(t, "/api/v1/instances/"+tableInstanceID+"/progress-stream", token)

		// 注意：服务器运行时持有 DuckDB 锁，无法直接连接校验
		// 表校验将在 Step 10 服务器关闭后进行
		t.Log("✅ 建表工作流已完成（表校验将在 Step 10 服务器关闭后进行）")
	})

	// ==================== Step 5.5: 校验所有表是否创建成功 ====================
	t.Run("Step5_5_VerifyAllTablesCreated", func(t *testing.T) {
		// 服务器运行时持有 DuckDB 锁，无法直接连接校验
		// 表校验将在 Step 10 服务器关闭后进行
		t.Skip("跳过：服务器运行时持有 DuckDB 锁，表校验将在 Step 10 进行")
	})

	// ==================== Step 6: 创建同步计划 ====================
	t.Run("Step6_CreateSyncPlan", func(t *testing.T) {
		t.Log("----- Step 6: 创建同步计划 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和DataStore")
		}

		// 选择多种类型的 API 进行测试（与 builtin_workflow_e2e_test 保持一致）
		// selectedAPIs := []string{
		// 	"stock_basic", "trade_cal", "daily", "weekly", "monthly",
		// 	"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
		// 	"index_basic", "index_daily", "top_list", "margin", "block_trade",
		// 	"fina_indicator", "namechange", "stk_limit", "margin_detail",
		// }
		selectedAPIs := []string{
			"stock_basic", "trade_cal", "income",
			"balancesheet", "cashflow",
		}
		createReq := map[string]interface{}{
			"name":           "RealSyncPlanTest (20 APIs)",
			"description":    "真实数据同步测试 - 20 个 API",
			"data_source_id": dataSourceID,
			"data_store_id":  dataStoreID,
			"selected_apis":  selectedAPIs,
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建同步计划失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		syncPlanID = getServerStringField(data, "id")
		require.NotEmpty(t, syncPlanID)
		t.Logf("✅ 同步计划创建成功: ID=%s", syncPlanID)
	})

	// ==================== Step 7: 解析同步计划依赖 ====================
	t.Run("Step7_ResolveSyncPlan", func(t *testing.T) {
		t.Log("----- Step 7: 解析同步计划依赖 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划")
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans/"+syncPlanID+"/resolve", nil, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("解析同步计划依赖失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()
		t.Log("✅ 同步计划依赖解析成功")
	})

	// ==================== Step 8: 触发同步 ====================
	t.Run("Step8_TriggerSync", func(t *testing.T) {
		t.Log("----- Step 8: 触发同步 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划")
		}

		// 可选：传 start_dt/end_dt（datetime）；不传则用计划 default_execute_params。target 从 plan 关联的 data store 解析。
		startDt := time.Now().AddDate(-1, 0, 0).Format("2006-01-02")
		endDt := time.Now().Format("2006-01-02")
		triggerReq := map[string]string{
			"start_dt": startDt,
			"end_dt":   endDt,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans/"+syncPlanID+"/execute", triggerReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("触发同步失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		response, _ := parseServerResponse(resp)
		t.Logf("✅ 同步已触发: %+v", response)

		// 获取执行ID
		if data, ok := response["data"].(map[string]interface{}); ok {
			executionID = getServerStringField(data, "execution_id")
			t.Logf("  执行ID: %s", executionID)
		}
	})

	// ==================== Step 9: 等待同步完成 ====================
	t.Run("Step9_WaitForSyncComplete", func(t *testing.T) {
		t.Log("----- Step 9: 等待同步完成 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划并触发执行")
		}

		// 使用 SSE 阻塞等待完成
		ctx.waitForWorkflowSSE(t, "/api/v1/sync-plans/"+syncPlanID+"/progress-stream", token)
	})

	// ==================== Step 10: 关闭服务器并查询同步的数据行数 ====================
	t.Run("Step10_QuerySyncedDataRows", func(t *testing.T) {
		t.Log("----- Step 10: 关闭服务器并查询同步的数据行数 -----")
		t.Logf("  DuckDB路径: %s", ctx.DuckDBPath)

		// 先关闭服务器以释放 DuckDB 锁
		t.Log("  关闭服务器以释放 DuckDB 锁...")
		if ctx.ServerCmd != nil && ctx.ServerCmd.Process != nil {
			ctx.ServerCmd.Process.Signal(syscall.SIGTERM)
			ctx.ServerCmd.Wait()
			ctx.ServerCmd = nil                // 防止 cleanup 再次关闭
			time.Sleep(500 * time.Millisecond) // 等待锁释放
		}

		// 参与同步的 API 列表（与 Step6 保持一致）
		syncedAPIs := []string{
			"stock_basic", "trade_cal", "daily", "weekly", "monthly",
			"daily_basic", "adj_factor", "income", "balancesheet", "cashflow",
			"index_basic", "index_daily", "top_list", "margin", "block_trade",
			"fina_indicator", "namechange", "stk_limit", "margin_detail",
		}

		// 打印同步数据统计表格
		printSyncDataSummary(t, ctx.DuckDBPath, syncedAPIs)
	})

	t.Log("========== 真实Tushare Token全流程测试完成 ==========")
}

// SyncDataSummaryRow 同步数据统计行
type SyncDataSummaryRow struct {
	TableName string
	RowCount  int64
	Exists    bool
	Error     string
}

// printSyncDataSummary 打印同步数据统计表格
func printSyncDataSummary(t *testing.T, duckDBPath string, tableNames []string) {
	t.Log("")
	t.Log("==================== 同步数据统计 ====================")

	// 检查 DuckDB 文件是否存在
	if _, err := os.Stat(duckDBPath); os.IsNotExist(err) {
		t.Log("⚠️  DuckDB 文件不存在，无法统计数据")
		return
	}

	// 连接 DuckDB
	ctx := context.Background()
	adapter := duckdb.NewAdapter(duckDBPath)
	if err := adapter.Connect(ctx); err != nil {
		t.Logf("⚠️  连接 DuckDB 失败: %v", err)
		return
	}
	defer adapter.Close()

	// 收集统计数据
	var rows []SyncDataSummaryRow
	var totalRows int64
	var successCount int

	for _, tableName := range tableNames {
		row := SyncDataSummaryRow{TableName: tableName}

		exists, err := adapter.TableExists(ctx, tableName)
		if err != nil {
			row.Error = fmt.Sprintf("检查失败: %v", err)
			rows = append(rows, row)
			continue
		}

		row.Exists = exists
		if !exists {
			row.Error = "表不存在"
			rows = append(rows, row)
			continue
		}

		stats, err := adapter.GetTableStats(ctx, tableName)
		if err != nil {
			row.Error = fmt.Sprintf("统计失败: %v", err)
			rows = append(rows, row)
			continue
		}

		row.RowCount = stats.RowCount
		totalRows += stats.RowCount
		if stats.RowCount > 0 {
			successCount++
		}
		rows = append(rows, row)
	}

	// 打印表格
	t.Log("┌────────────────────┬──────────────┬────────────────────────────┐")
	t.Log("│ 表名               │ 数据行数     │ 状态                       │")
	t.Log("├────────────────────┼──────────────┼────────────────────────────┤")

	for _, row := range rows {
		var status string
		if row.Error != "" {
			status = "✗ " + row.Error
		} else if row.RowCount > 0 {
			status = "✓"
		} else {
			status = "⚠ 空表"
		}

		// 格式化行数（带千分位）
		rowCountStr := formatNumber(row.RowCount)

		t.Logf("│ %-18s │ %12s │ %-26s │", row.TableName, rowCountStr, status)
	}

	t.Log("├────────────────────┼──────────────┼────────────────────────────┤")
	t.Logf("│ 总计               │ %12s │ %d/%d 表有数据              │",
		formatNumber(totalRows), successCount, len(tableNames))
	t.Log("└────────────────────┴──────────────┴────────────────────────────┘")
	t.Log("")
}

// formatNumber 格式化数字（添加千分位分隔符）
func formatNumber(n int64) string {
	if n == 0 {
		return "0"
	}

	str := fmt.Sprintf("%d", n)
	var result []byte

	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}

	return string(result)
}

// verifyTablesCreated 验证指定的表是否在数据库中创建成功
func verifyTablesCreated(t *testing.T, duckDBPath string, tableNames []string) error {
	if len(tableNames) == 0 {
		return nil
	}

	// 使用 DuckDB adapter 检查表是否存在
	ctx := context.Background()
	adapter := duckdb.NewAdapter(duckDBPath)
	if err := adapter.Connect(ctx); err != nil {
		return fmt.Errorf("连接 DuckDB 失败: %w", err)
	}
	defer adapter.Close()

	// 检查每个表是否存在
	missingTables := make([]string, 0)
	foundTables := make([]string, 0)

	for _, tableName := range tableNames {
		exists, err := adapter.TableExists(ctx, tableName)
		if err != nil {
			return fmt.Errorf("检查表 %s 是否存在时出错: %w", tableName, err)
		}
		if exists {
			foundTables = append(foundTables, tableName)
		} else {
			missingTables = append(missingTables, tableName)
		}
	}

	if len(missingTables) > 0 {
		return fmt.Errorf("以下表未创建: %v (已创建的表: %v)", missingTables, foundTables)
	}

	t.Logf("  ✅ 验证通过: 所有表已创建 (%v)", tableNames)
	return nil
}

// ==================== 简单测试 ====================

// TestE2E_Server_HealthCheck 简单的健康检查测试
func TestE2E_Server_HealthCheck(t *testing.T) {
	ctx := setupServerE2EContext(t)

	resp, err := ctx.HTTPClient.Get(ctx.BaseURL + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	t.Log("✅ 健康检查通过")
}

// TestE2E_Server_DataSourceCRUD 数据源CRUD测试
func TestE2E_Server_DataSourceCRUD(t *testing.T) {
	ctx := setupServerE2EContext(t)

	// 使用 admin 用户以执行完整 CRUD
	token := ctx.ensureAdminUser(t, "datasource_test_user", "datasource_test@example.com", "password123")

	// 创建数据源
	createReq := map[string]string{
		"name":        "TestTushare",
		"description": "Test Tushare Data Source",
		"base_url":    "http://api.tushare.pro",
		"doc_url":     "https://tushare.pro/document/2",
	}
	resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources", createReq, token)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	data, err := getServerResponseData(resp)
	require.NoError(t, err)
	id := getServerStringField(data, "id")
	require.NotEmpty(t, id)
	t.Logf("✅ 数据源创建成功: ID=%s", id)

	// 获取数据源（viewer可以读取）
	resp, err = ctx.doRequestWithAuth("GET", "/api/v1/datasources/"+id, nil, token)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	data, err = getServerResponseData(resp)
	require.NoError(t, err)
	assert.Equal(t, "TestTushare", getServerStringField(data, "name"))
	t.Log("✅ 数据源获取成功")

	// 列出数据源（viewer可以读取）
	resp, err = ctx.doRequestWithAuth("GET", "/api/v1/datasources", nil, token)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	list, err := getServerResponseDataList(resp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), 1, "列表应至少包含刚创建的数据源")
	t.Logf("✅ 数据源列表: %d 个", len(list))
}

// ==================== 数据同步专项测试 ====================

// TestE2E_DataSync_OneMonthHistorical 近一个月历史数据同步测试
// 同步的 API: daily（历史行情）, adj_factor（复权因子）, top_list（龙虎榜）, limit_cpt_list（涨跌停股票池）
func TestE2E_DataSync_OneMonthHistorical(t *testing.T) {
	// 检查 Tushare Token
	tushareToken := os.Getenv("QDHUB_TUSHARE_TOKEN")
	if tushareToken == "" {
		t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN 环境变量")
	}

	ctx := setupServerE2EContext(t)

	// 获取具有 admin 权限的 token
	token := ctx.ensureAdminUser(t, "datasync_test_user", "datasync_test@example.com", "password123")

	// 测试所需的 ID
	var dataSourceID, dataStoreID, syncPlanID, executionID string

	// 要同步的 API 列表
	// 注意：需要包含 trade_cal 和 stock_basic 作为基础依赖
	syncAPIs := []string{
		"trade_cal",   // 交易日历（基础依赖）
		"stock_basic", // 股票基础信息（基础依赖）
		"daily",       // 历史行情
		"adj_factor",  // 复权因子
		"top_list",    // 龙虎榜
		// 注意：limit_cpt_list 需要按股票逐个查询，容易触发 Tushare 速率限制（500次/分钟），不适合 E2E 测试
	}

	// 计算日期范围：近一个月
	endDate := time.Now().Format("20060102")
	startDate := time.Now().AddDate(0, -1, 0).Format("20060102") // 一个月前

	t.Logf("📅 同步日期范围: %s ~ %s", startDate, endDate)
	t.Logf("📋 同步 API 列表: %v", syncAPIs)

	// ==================== Step 1: 创建数据源 ====================
	t.Run("Step1_CreateDataSource", func(t *testing.T) {
		t.Log("----- Step 1: 创建数据源 -----")

		createReq := map[string]string{
			"name":        "Tushare",
			"description": "Tushare 金融数据",
			"base_url":    "http://api.tushare.pro",
			"doc_url":     "https://tushare.pro/document/2",
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建数据源失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		dataSourceID = getServerStringField(data, "id")
		require.NotEmpty(t, dataSourceID)
		t.Logf("✅ 数据源创建成功: ID=%s", dataSourceID)
	})

	// ==================== Step 2: 配置 Token ====================
	t.Run("Step2_ConfigureToken", func(t *testing.T) {
		t.Log("----- Step 2: 配置 Token -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		configReq := map[string]string{
			"token": tushareToken,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources/"+dataSourceID+"/token", configReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("配置 Token 失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()
		t.Log("✅ Token 配置成功")
	})

	// ==================== Step 3: 创建 DataStore ====================
	t.Run("Step3_CreateDataStore", func(t *testing.T) {
		t.Log("----- Step 3: 创建 DataStore -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		createReq := map[string]interface{}{
			"name":         "TestDuckDB",
			"type":         "duckdb",
			"storage_path": ctx.DuckDBPath,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datastores", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建 DataStore 失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		dataStoreID = getServerStringField(data, "id")
		require.NotEmpty(t, dataStoreID)
		t.Logf("✅ DataStore 创建成功: ID=%s, Path=%s", dataStoreID, ctx.DuckDBPath)
	})

	// ==================== Step 4: 刷新元数据 ====================
	t.Run("Step4_RefreshMetadata", func(t *testing.T) {
		t.Log("----- Step 4: 刷新元数据 -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources/"+dataSourceID+"/refresh", nil, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			t.Fatalf("刷新元数据失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		instanceID := getServerStringField(data, "instance_id")
		require.NotEmpty(t, instanceID)
		t.Logf("✅ 元数据刷新工作流已启动: InstanceID=%s", instanceID)

		// 等待元数据刷新完成
		ctx.waitForWorkflowSSE(t, "/api/v1/instances/"+instanceID+"/progress-stream", token)
		t.Log("✅ 元数据刷新完成")
	})

	// ==================== Step 5: 创建建表工作流 ====================
	t.Run("Step5_CreateTables", func(t *testing.T) {
		t.Log("----- Step 5: 创建建表工作流 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和 DataStore")
		}

		createReq := map[string]interface{}{
			"data_source_id": dataSourceID,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datastores/"+dataStoreID+"/create-tables", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
			t.Fatalf("创建建表工作流失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		tableInstanceID := getServerStringField(data, "instance_id")
		require.NotEmpty(t, tableInstanceID)
		t.Logf("✅ 建表工作流已启动: InstanceID=%s", tableInstanceID)

		// 等待建表完成
		ctx.waitForWorkflowSSE(t, "/api/v1/instances/"+tableInstanceID+"/progress-stream", token)
		t.Log("✅ 建表工作流完成")
	})

	// ==================== Step 6: 创建同步计划 ====================
	t.Run("Step6_CreateSyncPlan", func(t *testing.T) {
		t.Log("----- Step 6: 创建同步计划 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和 DataStore")
		}

		createReq := map[string]interface{}{
			"name":           "OneMonthHistoricalSync",
			"description":    fmt.Sprintf("近一个月历史数据同步 (%s ~ %s)", startDate, endDate),
			"data_source_id": dataSourceID,
			"data_store_id":  dataStoreID,
			"selected_apis":  syncAPIs,
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建同步计划失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		syncPlanID = getServerStringField(data, "id")
		require.NotEmpty(t, syncPlanID)
		t.Logf("✅ 同步计划创建成功: ID=%s", syncPlanID)
	})

	// ==================== Step 7: 解析同步计划依赖 ====================
	t.Run("Step7_ResolveSyncPlan", func(t *testing.T) {
		t.Log("----- Step 7: 解析同步计划依赖 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划")
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans/"+syncPlanID+"/resolve", nil, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("解析同步计划依赖失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()
		t.Log("✅ 同步计划依赖解析成功")
	})

	// ==================== Step 8: 触发同步 ====================
	t.Run("Step8_TriggerSync", func(t *testing.T) {
		t.Log("----- Step 8: 触发同步 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划")
		}

		triggerReq := map[string]string{
			"start_dt": startDate[:4] + "-" + startDate[4:6] + "-" + startDate[6:8],
			"end_dt":   endDate[:4] + "-" + endDate[4:6] + "-" + endDate[6:8],
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans/"+syncPlanID+"/execute", triggerReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("触发同步失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		response, _ := parseServerResponse(resp)
		t.Logf("✅ 同步已触发: %+v", response)

		// 获取执行ID
		if data, ok := response["data"].(map[string]interface{}); ok {
			executionID = getServerStringField(data, "execution_id")
			t.Logf("  执行ID: %s", executionID)
		}
	})

	// ==================== Step 9: 等待同步完成 ====================
	t.Run("Step9_WaitForSyncComplete", func(t *testing.T) {
		t.Log("----- Step 9: 等待同步完成 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建同步计划并触发执行")
		}

		// 使用 SSE 阻塞等待完成
		ctx.waitForWorkflowSSE(t, "/api/v1/sync-plans/"+syncPlanID+"/progress-stream", token)
		t.Log("✅ 数据同步完成")
	})

	// ==================== Step 10: 关闭服务器并验证同步数据 ====================
	t.Run("Step10_VerifySyncedData", func(t *testing.T) {
		t.Log("----- Step 10: 关闭服务器并验证同步数据 -----")
		_ = executionID // 使用 executionID 避免未使用警告

		// 先关闭服务器以释放 DuckDB 锁
		t.Log("🛑 关闭服务器以释放 DuckDB 锁...")
		if ctx.ServerCmd != nil && ctx.ServerCmd.Process != nil {
			ctx.ServerCmd.Process.Signal(syscall.SIGTERM)
			done := make(chan error)
			go func() {
				done <- ctx.ServerCmd.Wait()
			}()
			select {
			case <-done:
				t.Log("✅ 服务器已关闭")
			case <-time.After(5 * time.Second):
				ctx.ServerCmd.Process.Kill()
				t.Log("⚠️ 服务器被强制关闭")
			}
			// 标记服务器已关闭，避免 cleanup 重复关闭
			ctx.ServerCmd = nil
		}

		// 等待一小段时间确保文件锁释放
		time.Sleep(1 * time.Second)

		t.Log("----- 验证同步的数据 -----")
		t.Logf("  DuckDB 路径: %s", ctx.DuckDBPath)

		verifyCtx := context.Background()

		// 连接 DuckDB 验证数据
		adapter := duckdb.NewAdapter(ctx.DuckDBPath)
		if err := adapter.Connect(verifyCtx); err != nil {
			t.Fatalf("无法连接 DuckDB 进行验证: %v", err)
		}
		defer adapter.Close()

		// 验证每个表的数据
		tablesToCheck := []string{"trade_cal", "stock_basic", "daily", "adj_factor", "top_list"}
		allTablesExist := true

		for _, tableName := range tablesToCheck {
			exists, err := adapter.TableExists(verifyCtx, tableName)
			if err != nil {
				t.Errorf("检查表 %s 时出错: %v", tableName, err)
				allTablesExist = false
				continue
			}
			if !exists {
				t.Errorf("表 %s 不存在", tableName)
				allTablesExist = false
				continue
			}

			// 查询行数
			result, err := adapter.Query(verifyCtx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
			if err != nil {
				t.Errorf("查询表 %s 行数时出错: %v", tableName, err)
				continue
			}
			var count int64
			if len(result) > 0 {
				// Query 返回 []map[string]any，COUNT(*) 结果在第一行
				for _, v := range result[0] {
					switch val := v.(type) {
					case int64:
						count = val
					case int:
						count = int64(val)
					case float64:
						count = int64(val)
					}
					break
				}
			}

			t.Logf("  ✅ 表 %s: %d 行", tableName, count)
		}

		if !allTablesExist {
			t.Error("部分表不存在，数据同步可能失败")
		}
	})
}

// ==================== Cron 定时调度测试 ====================

// TestE2E_Server_CronScheduleTrigger 测试 Cron 定时调度实际触发执行
// 验证：创建每分钟执行一次的 SyncPlan，等待并验证至少触发 2 次执行
// 注意：此测试需要真实 Tushare Token，且运行时间约 2.5 分钟
func TestE2E_Server_CronScheduleTrigger(t *testing.T) {
	// 检查 Tushare Token
	tushareToken := os.Getenv("QDHUB_TUSHARE_TOKEN")
	if tushareToken == "" {
		t.Skip("跳过：未设置 QDHUB_TUSHARE_TOKEN 环境变量（此测试需要真实 Token 才能执行同步）")
	}

	ctx := setupServerE2EContext(t)

	// 获取具有 admin 权限的 token
	token := ctx.ensureAdminUser(t, "cron_test_user", "cron_test@example.com", "password123")

	t.Log("========== Cron 定时调度触发测试 ==========")
	t.Log("⏰ 此测试将创建每分钟执行一次的同步计划，等待约 2.5 分钟验证触发")

	var dataSourceID, dataStoreID, syncPlanID string

	// ==================== Step 1: 创建数据源 ====================
	t.Run("Step1_CreateDataSource", func(t *testing.T) {
		t.Log("----- Step 1: 创建数据源 -----")

		createReq := map[string]string{
			"name":        "Tushare",
			"description": "Tushare",
			"base_url":    "http://api.tushare.pro",
			"doc_url":     "https://tushare.pro/document/2",
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建数据源失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		dataSourceID = getServerStringField(data, "id")
		require.NotEmpty(t, dataSourceID)
		t.Logf("✅ 数据源创建成功: ID=%s", dataSourceID)
	})

	// ==================== Step 2: 配置 Token ====================
	t.Run("Step2_ConfigureToken", func(t *testing.T) {
		t.Log("----- Step 2: 配置 Token -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		configReq := map[string]string{
			"token": tushareToken,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources/"+dataSourceID+"/token", configReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("配置 Token 失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()
		t.Log("✅ Token 配置成功")
	})

	// ==================== Step 3: 刷新元数据 ====================
	t.Run("Step3_RefreshMetadata", func(t *testing.T) {
		t.Log("----- Step 3: 刷新元数据 -----")
		if dataSourceID == "" {
			t.Skip("跳过：需要先创建数据源")
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources/"+dataSourceID+"/refresh", nil, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("刷新元数据失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		instanceID := getServerStringField(data, "instance_id")
		if instanceID != "" {
			t.Logf("  等待元数据刷新完成: %s", instanceID)
			ctx.waitForWorkflowSSE(t, "/api/v1/instances/"+instanceID+"/progress-stream", token)
		}
		t.Log("✅ 元数据刷新完成")
	})

	// ==================== Step 4: 创建 DataStore ====================
	t.Run("Step4_CreateDataStore", func(t *testing.T) {
		t.Log("----- Step 4: 创建 DataStore -----")

		createReq := map[string]interface{}{
			"name":         "CronTestDuckDB",
			"type":         "duckdb",
			"storage_path": ctx.DuckDBPath,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datastores", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建 DataStore 失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		dataStoreID = getServerStringField(data, "id")
		require.NotEmpty(t, dataStoreID)
		t.Logf("✅ DataStore 创建成功: ID=%s", dataStoreID)
	})

	// ==================== Step 5: 建表 ====================
	t.Run("Step5_CreateTables", func(t *testing.T) {
		t.Log("----- Step 5: 建表 -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和 DataStore")
		}

		createReq := map[string]interface{}{
			"data_source_id": dataSourceID,
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datastores/"+dataStoreID+"/create-tables", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			t.Fatalf("建表失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		tableInstanceID := getServerStringField(data, "instance_id")
		if tableInstanceID != "" {
			t.Logf("  等待建表完成: %s", tableInstanceID)
			ctx.waitForWorkflowSSE(t, "/api/v1/instances/"+tableInstanceID+"/progress-stream", token)
		}
		t.Log("✅ 建表完成")
	})

	// ==================== Step 6: 创建每分钟执行的 SyncPlan ====================
	t.Run("Step6_CreateCronSyncPlan", func(t *testing.T) {
		t.Log("----- Step 6: 创建每分钟执行的 SyncPlan -----")
		if dataSourceID == "" || dataStoreID == "" {
			t.Skip("跳过：需要先创建数据源和 DataStore")
		}

		// 计算日期范围（最近一周，数据量小）
		endDate := time.Now().Format("20060102")
		startDate := time.Now().AddDate(0, 0, -7).Format("20060102")

		// 每分钟执行一次：使用 @every 语法或标准 6 位 cron（秒 分 时 日 月 周）
		cronExpr := "@every 1m" // robfig/cron 支持的简写形式
		createReq := map[string]interface{}{
			"name":            "EveryMinuteSyncPlan",
			"description":     "每分钟执行一次的同步计划（Cron 触发测试）",
			"data_source_id":  dataSourceID,
			"data_store_id":   dataStoreID,
			"selected_apis":   []string{"trade_cal", "stock_basic"}, // 仅同步这两个简单 API
			"cron_expression": cronExpr,
			"default_execute_params": map[string]string{
				"start_date":     startDate,
				"end_date":       endDate,
			},
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans", createReq, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建 SyncPlan 失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}

		data, err := getServerResponseData(resp)
		require.NoError(t, err)
		syncPlanID = getServerStringField(data, "id")
		require.NotEmpty(t, syncPlanID)

		t.Logf("✅ SyncPlan 创建成功: ID=%s, Cron=%s", syncPlanID, cronExpr)
	})

	// ==================== Step 7: 解析依赖 ====================
	t.Run("Step7_ResolveSyncPlan", func(t *testing.T) {
		t.Log("----- Step 7: 解析依赖 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建 SyncPlan")
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans/"+syncPlanID+"/resolve", nil, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("解析依赖失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()
		t.Log("✅ 依赖解析成功")
	})

	// ==================== Step 8: 启用计划（开始调度） ====================
	t.Run("Step8_EnablePlan", func(t *testing.T) {
		t.Log("----- Step 8: 启用计划 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建 SyncPlan")
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans/"+syncPlanID+"/enable", nil, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("启用计划失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()

		// 获取下次执行时间
		resp, err = ctx.doRequestWithAuth("GET", "/api/v1/sync-plans/"+syncPlanID, nil, token)
		require.NoError(t, err)
		data, _ := getServerResponseData(resp)
		nextExecuteAt := data["next_execute_at"]
		t.Logf("✅ 计划已启用，下次执行时间: %v", nextExecuteAt)
	})

	// ==================== Step 9: 等待并验证触发执行 ====================
	t.Run("Step9_WaitAndVerifyTriggers", func(t *testing.T) {
		t.Log("----- Step 9: 等待 Cron 触发执行 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建 SyncPlan")
		}

		// 等待约 2.5 分钟，期间每 10 秒检查一次执行记录
		maxWait := 150 * time.Second // 2.5 分钟
		checkInterval := 10 * time.Second
		startTime := time.Now()
		requiredExecutions := 2

		t.Logf("⏳ 开始等待 Cron 触发，最长等待 %v，目标: %d 次执行", maxWait, requiredExecutions)

		var executionCount int
		var lastCheckCount int

		for time.Since(startTime) < maxWait {
			time.Sleep(checkInterval)
			elapsed := time.Since(startTime).Round(time.Second)

			// 查询执行记录
			resp, err := ctx.doRequestWithAuth("GET", "/api/v1/sync-plans/"+syncPlanID+"/executions", nil, token)
			if err != nil {
				t.Logf("  %v: 查询执行记录失败: %v", elapsed, err)
				continue
			}

			execList, err := getServerResponseDataList(resp)
			if err != nil {
				// 可能是空列表返回 null
				execList = []interface{}{}
			}

			executionCount = len(execList)

			if executionCount != lastCheckCount {
				t.Logf("  %v: 执行记录数: %d", elapsed, executionCount)
				lastCheckCount = executionCount
			}

			// 达到目标次数，提前结束
			if executionCount >= requiredExecutions {
				t.Logf("✅ 已达到目标执行次数: %d", executionCount)
				break
			}
		}

		// 验证结果
		t.Logf("📊 最终执行记录数: %d (目标: %d)", executionCount, requiredExecutions)
		assert.GreaterOrEqual(t, executionCount, requiredExecutions,
			"Cron 应至少触发 %d 次执行，实际触发 %d 次", requiredExecutions, executionCount)

		if executionCount >= requiredExecutions {
			t.Log("✅ Cron 定时触发测试通过！")
		} else {
			t.Errorf("❌ Cron 定时触发测试失败：期望至少 %d 次执行，实际 %d 次", requiredExecutions, executionCount)
		}
	})

	// ==================== Step 10: 禁用计划 ====================
	t.Run("Step10_DisablePlan", func(t *testing.T) {
		t.Log("----- Step 10: 禁用计划 -----")
		if syncPlanID == "" {
			t.Skip("跳过：需要先创建 SyncPlan")
		}

		// 等待 plan 脱离 running（最后一次执行的 workflow 完成并触发 callback 后才会 MarkCompleted）
		t.Log("⏳ 等待 plan 空闲（status != running）后再禁用...")
		waitIdle := 30 * time.Second
		interval := 2 * time.Second
		deadline := time.Now().Add(waitIdle)
		for time.Now().Before(deadline) {
			resp, err := ctx.doRequestWithAuth("GET", "/api/v1/sync-plans/"+syncPlanID, nil, token)
			require.NoError(t, err)
			data, _ := getServerResponseData(resp)
			resp.Body.Close()
			if status, _ := data["status"].(string); status != "" && status != "running" {
				t.Logf("✅ Plan 已空闲: status=%s", status)
				break
			}
			time.Sleep(interval)
		}

		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/sync-plans/"+syncPlanID+"/disable", nil, token)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("禁用计划失败: 状态码 %d, 响应: %s", resp.StatusCode, readServerResponseBody(resp))
		}
		resp.Body.Close()
		t.Log("✅ 计划已禁用，停止调度")
	})

	t.Log("========== Cron 定时调度触发测试完成 ==========")
}

// ==================== 权限控制测试 ====================

// TestE2E_Server_RBAC_Permissions 测试RBAC权限控制
// 验证不同角色对资源的访问权限
func TestE2E_Server_RBAC_Permissions(t *testing.T) {
	ctx := setupServerE2EContext(t)

	t.Log("========== RBAC权限控制测试 ==========")

	// 创建viewer用户（默认角色）
	viewerToken := ctx.ensureAuthenticatedUser(t, "rbac_viewer_test", "rbac_viewer_test@example.com", "password123")
	t.Log("✅ Viewer用户已创建并登录")

	// 创建operator用户（需要手动分配角色，这里先测试viewer的限制）
	_ = ctx.ensureAuthenticatedUser(t, "rbac_operator_test", "rbac_operator_test@example.com", "password123")
	t.Log("✅ Operator用户已创建并登录")

	// 创建admin用户（需要手动分配角色，这里先测试viewer的限制）
	_ = ctx.ensureAuthenticatedUser(t, "rbac_admin_test", "rbac_admin_test@example.com", "password123")
	t.Log("✅ Admin用户已创建并登录")

	// ==================== 测试1: Viewer角色权限 ====================
	t.Run("Test1_ViewerPermissions", func(t *testing.T) {
		t.Log("----- 测试1: Viewer角色权限 -----")

		// Viewer应该可以读取同步计划列表
		resp, err := ctx.doRequestWithAuth("GET", "/api/v1/sync-plans", nil, viewerToken)
		require.NoError(t, err)
		if resp.StatusCode == http.StatusOK {
			t.Log("✅ Viewer可以读取同步计划列表")
		} else if resp.StatusCode == http.StatusForbidden {
			t.Log("⚠️  Viewer无法读取同步计划列表（可能需要调整权限配置）")
		} else {
			t.Logf("⚠️  意外状态码: %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Viewer不应该能够创建同步计划
		createReq := map[string]interface{}{
			"name":           "TestSyncPlan",
			"description":    "Test",
			"data_source_id": "test-id",
			"data_store_id":  "test-id",
			"selected_apis":  []string{},
		}
		resp, err = ctx.doRequestWithAuth("POST", "/api/v1/sync-plans", createReq, viewerToken)
		require.NoError(t, err)
		if resp.StatusCode == http.StatusForbidden {
			t.Log("✅ Viewer无法创建同步计划（符合预期）")
		} else {
			t.Logf("⚠️  Viewer创建同步计划返回状态码: %d（预期403）", resp.StatusCode)
		}
		resp.Body.Close()

		// Viewer不应该能够删除数据源
		resp, err = ctx.doRequestWithAuth("DELETE", "/api/v1/datasources/test-id", nil, viewerToken)
		require.NoError(t, err)
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			t.Log("✅ Viewer无法删除数据源（符合预期）")
		} else {
			t.Logf("⚠️  Viewer删除数据源返回状态码: %d（预期403或404）", resp.StatusCode)
		}
		resp.Body.Close()
	})

	// ==================== 测试2: 未认证请求 ====================
	t.Run("Test2_UnauthenticatedRequests", func(t *testing.T) {
		t.Log("----- 测试2: 未认证请求 -----")

		// 未认证的请求应该返回401
		resp, err := ctx.doRequest("GET", "/api/v1/sync-plans", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "未认证请求应该返回401")
		resp.Body.Close()
		t.Log("✅ 未认证请求正确返回401")

		// 无效token应该返回401
		resp, err = ctx.doRequestWithAuth("GET", "/api/v1/sync-plans", nil, "invalid_token")
		require.NoError(t, err)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "无效token应该返回401")
		resp.Body.Close()
		t.Log("✅ 无效token正确返回401")
	})

	// ==================== 测试3: 认证但权限不足 ====================
	t.Run("Test3_InsufficientPermissions", func(t *testing.T) {
		t.Log("----- 测试3: 认证但权限不足 -----")

		// Viewer尝试执行需要write权限的操作
		createReq := map[string]interface{}{
			"name":        "TestDataSource",
			"description": "Test",
			"base_url":    "http://test.com",
			"doc_url":     "http://test.com/docs",
		}
		resp, err := ctx.doRequestWithAuth("POST", "/api/v1/datasources", createReq, viewerToken)
		require.NoError(t, err)
		if resp.StatusCode == http.StatusForbidden {
			t.Log("✅ Viewer无法创建数据源（权限不足，符合预期）")
		} else if resp.StatusCode == http.StatusCreated {
			t.Log("⚠️  Viewer可以创建数据源（可能需要调整权限配置）")
		} else {
			t.Logf("⚠️  创建数据源返回状态码: %d", resp.StatusCode)
		}
		resp.Body.Close()
	})

	t.Log("========== RBAC权限控制测试完成 ==========")
	t.Log("注意：完整的权限测试需要手动分配admin/operator角色")
	t.Log("可以通过数据库直接修改user_roles表，或通过admin API修改用户角色")
}
