//go:build e2e
// +build e2e

// Package e2e 实时数据源管理 E2E：HTTP CRUD、health、heartbeat SSE、connect/disconnect；connect 后收 mock 数据 5 秒再 disconnect。
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	coreRealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/application/contracts"
	"qdhub/internal/application/impl"
	"qdhub/internal/domain/auth"
	"qdhub/internal/domain/realtime"
	"qdhub/internal/domain/shared"
	authinfra "qdhub/internal/infrastructure/auth"
	realtimeinfra "qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/healthcheck"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/realtimestore"
	httpapi "qdhub/internal/interfaces/http"
	"qdhub/pkg/crypto"
)

func setupRealtimeSourcesE2EDB(t *testing.T) (*persistence.DB, func()) {
	t.Helper()
	dbPath := filepath.Join(os.TempDir(), "test_e2e_realtime_sources.db")
	os.Remove(dbPath)

	db, err := persistence.NewDB(dbPath)
	require.NoError(t, err)

	// 最小迁移：001 + 002 auth + 028 realtime_sources + 029 seed（兼容 cwd=qdhub 或 cwd=tests/e2e）
	migrationsDir := "migrations"
	if _, err := os.Stat(filepath.Join(migrationsDir, "028_realtime_sources.up.sql")); err != nil {
		migrationsDir = "../../migrations"
	}
	for _, name := range []string{
		"001_init_schema.up.sql",
		"002_auth_schema.sqlite.up.sql",
		"006_seed_default_admin.sqlite.up.sql",
		"028_realtime_sources.up.sql",
		"029_seed_realtime_sources.sqlite.up.sql",
	} {
		p := filepath.Join(migrationsDir, name)
		body, err := os.ReadFile(p)
		if err != nil {
			t.Skipf("migration not found: %s (run from qdhub or tests/e2e)", p)
			return nil, func() {}
		}
		_, err = db.Exec(string(body))
		require.NoError(t, err, "run %s", name)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}
	return db, cleanup
}

func setupRealtimeSourcesE2EServer(t *testing.T, db *persistence.DB) (*httptest.Server, string, func()) {
	t.Helper()
	ctx := context.Background()

	// Auth
	userRepo := repository.NewUserRepository(db)
	passwordHasher := auth.NewBcryptPasswordHasher(0)
	jwtManager := authinfra.NewJWTManager("test_secret_key_123456789012345678901234567890", 1*time.Hour, 24*time.Hour)
	enforcer, err := authinfra.NewCasbinEnforcer(db.DB, persistence.DBTypeSQLite)
	require.NoError(t, err)
	require.NoError(t, authinfra.InitializeDefaultPolicies(enforcer))
	require.NoError(t, authinfra.EnsureRealtimeSourcesPolicies(enforcer))
	authSvc := impl.NewAuthApplicationService(userRepo, userRepo, passwordHasher, jwtManager)

	// Register admin and login
	_, err = authSvc.Register(ctx, contracts.RegisterRequest{
		Username: "e2e_admin",
		Email:    "e2e@test.com",
		Password: "admin123",
	})
	require.NoError(t, err)
	user, err := userRepo.GetByUsername(ctx, "e2e_admin")
	require.NoError(t, err)
	require.NotNil(t, user)
	require.NoError(t, userRepo.AssignRole(ctx, user.ID, "admin"))
	loginResp, err := authSvc.Login(ctx, contracts.LoginRequest{Username: "e2e_admin", Password: "admin123"})
	require.NoError(t, err)
	token := loginResp.AccessToken

	// Realtime sources
	realtimeRepo := repository.NewRealtimeSourceRepository(db)
	realtimeSvc := impl.NewRealtimeSourceApplicationService(realtimeRepo, &healthcheck.Checker{})
	selector := realtimestore.NewRealtimeSourceSelector()
	realtimeHandler := httpapi.NewRealtimeSourceHandler(realtimeSvc, selector, nil)

	// Minimal HTTP server: auth + realtime-sources only. jwt_manager 必须在 JWTAuthMiddleware 之前注入。
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(func(c *gin.Context) { c.Set("jwt_manager", jwtManager); c.Next() })
	engine.Use(httpapi.JWTAuthMiddleware())
	engine.Use(httpapi.CasbinRBACMiddleware(enforcer))
	engine.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	realtimeHandler.RegisterRoutes(engine.Group("/api/v1"))

	ts := httptest.NewServer(engine)
	return ts, token, func() { ts.Close() }
}

// e2eCollectorConnector 在 E2E 中按 id 启动/停止 ForwardTickCollector，与 RealtimeWSHandler 共用 DefaultLatestQuoteStore 与 selector。
type e2eCollectorConnector struct {
	repo     realtime.RealtimeSourceRepository
	selector *realtimestore.RealtimeSourceSelector
	mu       sync.Mutex
	cancel   map[string]context.CancelFunc
}

func newE2ECollectorConnector(repo realtime.RealtimeSourceRepository, selector *realtimestore.RealtimeSourceSelector) *e2eCollectorConnector {
	return &e2eCollectorConnector{repo: repo, selector: selector, cancel: make(map[string]context.CancelFunc)}
}

func (e *e2eCollectorConnector) Connect(id string) error {
	src, err := e.repo.Get(shared.ID(id))
	if err != nil || src == nil {
		return err
	}
	if src.Type != realtime.TypeTushareForward {
		return nil
	}
	m, err := src.ConfigMap()
	if err != nil {
		return err
	}
	wsURL, _ := m["ws_url"].(string)
	rsaPath, _ := m["rsa_public_key_path"].(string)
	if wsURL == "" || rsaPath == "" {
		return fmt.Errorf("tushare_forward requires ws_url and rsa_public_key_path")
	}
	e.selector.SwitchTo(realtimestore.SourceTushareForward)
	collector := &realtimeinfra.ForwardTickCollector{
		ForwardWSURL:            wsURL,
		ForwardRSAPublicKeyPath: rsaPath,
		Selector:                e.selector,
	}
	runCtx, cancel := context.WithCancel(context.Background())
	e.mu.Lock()
	if old, ok := e.cancel[id]; ok {
		old()
	}
	e.cancel[id] = cancel
	e.mu.Unlock()
	publish := func(*coreRealtime.RealtimeEvent) error { return nil }
	go func() { _ = collector.Run(runCtx, &coreRealtime.ContinuousTaskConfig{}, publish) }()
	return nil
}

func (e *e2eCollectorConnector) Disconnect(id string) error {
	e.mu.Lock()
	c, ok := e.cancel[id]
	if ok {
		delete(e.cancel, id)
	}
	e.mu.Unlock()
	if ok && c != nil {
		c()
	}
	return nil
}

// setupRealtimeSourcesE2EServerWithConnector 与 setupRealtimeSourcesE2EServer 类似，但注入 e2eCollectorConnector 并注册 RealtimeWSHandler，用于 connect→收数→disconnect 测试。
func setupRealtimeSourcesE2EServerWithConnector(t *testing.T, db *persistence.DB) (*httptest.Server, string, func()) {
	t.Helper()
	ctx := context.Background()

	userRepo := repository.NewUserRepository(db)
	passwordHasher := auth.NewBcryptPasswordHasher(0)
	jwtManager := authinfra.NewJWTManager("test_secret_key_123456789012345678901234567890", 1*time.Hour, 24*time.Hour)
	enforcer, err := authinfra.NewCasbinEnforcer(db.DB, persistence.DBTypeSQLite)
	require.NoError(t, err)
	require.NoError(t, authinfra.InitializeDefaultPolicies(enforcer))
	require.NoError(t, authinfra.EnsureRealtimeSourcesPolicies(enforcer))
	authSvc := impl.NewAuthApplicationService(userRepo, userRepo, passwordHasher, jwtManager)

	_, err = authSvc.Register(ctx, contracts.RegisterRequest{
		Username: "e2e_admin",
		Email:    "e2e@test.com",
		Password: "admin123",
	})
	require.NoError(t, err)
	user, err := userRepo.GetByUsername(ctx, "e2e_admin")
	require.NoError(t, err)
	require.NotNil(t, user)
	require.NoError(t, userRepo.AssignRole(ctx, user.ID, "admin"))
	loginResp, err := authSvc.Login(ctx, contracts.LoginRequest{Username: "e2e_admin", Password: "admin123"})
	require.NoError(t, err)
	token := loginResp.AccessToken

	realtimeRepo := repository.NewRealtimeSourceRepository(db)
	realtimeSvc := impl.NewRealtimeSourceApplicationService(realtimeRepo, &healthcheck.Checker{})
	selector := realtimestore.NewRealtimeSourceSelector()
	connector := newE2ECollectorConnector(realtimeRepo, selector)
	realtimeHandler := httpapi.NewRealtimeSourceHandler(realtimeSvc, selector, connector)
	store := realtimestore.DefaultLatestQuoteStore()
	wsHandler := httpapi.NewRealtimeWSHandler(store, nil, selector)

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(func(c *gin.Context) { c.Set("jwt_manager", jwtManager); c.Next() })
	engine.Use(httpapi.JWTAuthMiddleware())
	engine.Use(httpapi.CasbinRBACMiddleware(enforcer))
	engine.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	api := engine.Group("/api/v1")
	realtimeHandler.RegisterRoutes(api)
	wsHandler.RegisterRoutes(api)

	ts := httptest.NewServer(engine)
	return ts, token, func() { ts.Close() }
}

// mockForwardWSServerStreamingE2E 与 integration 中的 mockForwardWSServerStreaming 一致：duration 内每 interval 发一条加密 tick。
func mockForwardWSServerStreamingE2E(t *testing.T, priv *rsa.PrivateKey, baseRow map[string]interface{}, duration, interval time.Duration) *httptest.Server {
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/realtime", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		defer conn.Close()
		_, encKey, err := conn.ReadMessage()
		require.NoError(t, err)
		aesKey, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, encKey, nil)
		require.NoError(t, err)
		sess, err := crypto.NewSessionCipher(aesKey)
		require.NoError(t, err)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		deadline := time.Now().Add(duration)
		seq := 0
		for time.Now().Before(deadline) {
			select {
			case <-ticker.C:
				row := make(map[string]interface{})
				for k, v := range baseRow {
					row[k] = v
				}
				row["seq"] = seq
				seq++
				body, err := json.Marshal(row)
				require.NoError(t, err)
				enc, err := sess.Encrypt(body)
				require.NoError(t, err)
				if err := conn.WriteMessage(websocket.BinaryMessage, enc); err != nil {
					return
				}
			}
		}
	})
	return httptest.NewServer(mux)
}

func TestE2E_RealtimeSources_CRUD(t *testing.T) {
	db, cleanupDB := setupRealtimeSourcesE2EDB(t)
	defer cleanupDB()
	ts, token, cleanup := setupRealtimeSourcesE2EServer(t, db)
	defer cleanup()

	base := ts.URL + "/api/v1/realtime-sources"
	authHeader := "Bearer " + token

	t.Run("List", func(t *testing.T) {
		req, _ := http.NewRequest("GET", base, nil)
		req.Header.Set("Authorization", authHeader)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "list realtime-sources")
		var out struct {
			Data []interface{} `json:"data"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		assert.GreaterOrEqual(t, len(out.Data), 1) // seed 至少有 1 条
	})

	t.Run("Create", func(t *testing.T) {
		body := map[string]interface{}{
			"name":     "E2E Forward",
			"type":     realtime.TypeTushareForward,
			"config":   `{"ws_url":"ws://localhost:8888/realtime","rsa_public_key_path":"/tmp/pub.pem"}`,
			"priority": 10,
			"enabled":  true,
		}
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", base, bytes.NewReader(raw))
		req.Header.Set("Authorization", authHeader)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		var out struct {
			Data struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		require.NotEmpty(t, out.Data.ID)

		t.Run("Get", func(t *testing.T) {
			req, _ := http.NewRequest("GET", base+"/"+out.Data.ID, nil)
			req.Header.Set("Authorization", authHeader)
			resp2, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp2.Body.Close()
			assert.Equal(t, http.StatusOK, resp2.StatusCode)
		})

		t.Run("Update", func(t *testing.T) {
			body := map[string]interface{}{"name": "E2E Forward Updated"}
			raw, _ := json.Marshal(body)
			req, _ := http.NewRequest("PUT", base+"/"+out.Data.ID, bytes.NewReader(raw))
			req.Header.Set("Authorization", authHeader)
			req.Header.Set("Content-Type", "application/json")
			resp2, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp2.Body.Close()
			assert.Equal(t, http.StatusOK, resp2.StatusCode)
		})

		t.Run("Delete", func(t *testing.T) {
			req, _ := http.NewRequest("DELETE", base+"/"+out.Data.ID, nil)
			req.Header.Set("Authorization", authHeader)
			resp2, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp2.Body.Close()
			assert.Equal(t, http.StatusNoContent, resp2.StatusCode)
		})
	})
}

func TestE2E_RealtimeSources_Health(t *testing.T) {
	db, cleanupDB := setupRealtimeSourcesE2EDB(t)
	defer cleanupDB()
	ts, token, cleanup := setupRealtimeSourcesE2EServer(t, db)
	defer cleanup()

	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/realtime-sources/health", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Data struct {
			Sources         []interface{}      `json:"sources"`
			CurrentSource   string             `json:"current_source"`
			SourcesHealth   map[string]string `json:"sources_health"`
			SourcesError    map[string]string `json:"sources_error"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.NotNil(t, out.Data.Sources)
}

func TestE2E_RealtimeSources_Heartbeat_SSE(t *testing.T) {
	db, cleanupDB := setupRealtimeSourcesE2EDB(t)
	defer cleanupDB()
	ts, token, cleanup := setupRealtimeSourcesE2EServer(t, db)
	defer cleanup()

	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/realtime-sources/heartbeat", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream"))

	// 读至少一条 SSE 事件（或几秒内）
	scanner := bufio.NewScanner(resp.Body)
	scanner.Split(bufio.ScanLines)
	gotEvent := false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			gotEvent = true
			break
		}
	}
	assert.True(t, gotEvent, "expected at least one SSE data event within 8s")
}

func TestE2E_RealtimeSources_ConnectDisconnect(t *testing.T) {
	db, cleanupDB := setupRealtimeSourcesE2EDB(t)
	defer cleanupDB()
	ts, token, cleanup := setupRealtimeSourcesE2EServer(t, db)
	defer cleanup()

	// 先拿到一条已有源的 id（seed）
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/realtime-sources", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var list struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&list))
	require.GreaterOrEqual(t, len(list.Data), 1)
	id := list.Data[0].ID

	// Connect（无 connector 时返回 200 + message）
	req2, _ := http.NewRequest("POST", ts.URL+"/api/v1/realtime-sources/"+id+"/connect", nil)
	req2.Header.Set("Authorization", "Bearer "+token)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	// Disconnect
	req3, _ := http.NewRequest("POST", ts.URL+"/api/v1/realtime-sources/"+id+"/disconnect", nil)
	req3.Header.Set("Authorization", "Bearer "+token)
	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
}

// TestE2E_RealtimeSources_ConnectReceiveDisconnect 起 mock 流式服务 → 建 tushare_forward 源 → connect → WS 持续收 5 秒并打印到 console → disconnect。
func TestE2E_RealtimeSources_ConnectReceiveDisconnect(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	dir := t.TempDir()
	pubPath := filepath.Join(dir, "pub.pem")
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: pubBytes}), 0600))

	baseRow := map[string]interface{}{"ts_code": "000001.SZ", "price": 10.5, "vol": 1000.0}
	mockSrv := mockForwardWSServerStreamingE2E(t, priv, baseRow, 10*time.Second, 300*time.Millisecond)
	defer mockSrv.Close()
	wsURL := "ws" + mockSrv.URL[4:] + "/realtime"

	db, cleanupDB := setupRealtimeSourcesE2EDB(t)
	defer cleanupDB()
	ts, token, cleanup := setupRealtimeSourcesE2EServerWithConnector(t, db)
	defer cleanup()

	// 创建 tushare_forward 源（mock 的 ws_url + rsa path）；API 的 config 为 JSON 字符串
	config := map[string]interface{}{"ws_url": wsURL, "rsa_public_key_path": pubPath}
	configBytes, _ := json.Marshal(config)
	payload := map[string]interface{}{
		"name": "E2E Forward", "type": "tushare_forward", "config": string(configBytes),
		"priority": 1, "is_primary": false, "enabled": true, "health_check_on_startup": false,
	}
	bodyBytes, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", ts.URL+"/api/v1/realtime-sources", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var createResp struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&createResp))
	sourceID := createResp.Data.ID
	require.NotEmpty(t, sourceID)

	// Connect：启动 ForwardTickCollector，写 DefaultLatestQuoteStore
	reqConnect, _ := http.NewRequest("POST", ts.URL+"/api/v1/realtime-sources/"+sourceID+"/connect", nil)
	reqConnect.Header.Set("Authorization", "Bearer "+token)
	respConnect, err := http.DefaultClient.Do(reqConnect)
	require.NoError(t, err)
	respConnect.Body.Close()
	require.Equal(t, http.StatusOK, respConnect.StatusCode)

	// 给 collector 一点时间连上 mock 并收到首包
	time.Sleep(800 * time.Millisecond)

	// WebSocket 连 /api/v1/ws/realtime-quotes，带 JWT
	wsURLQuotes := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/v1/ws/realtime-quotes"
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	conn, _, err := websocket.DefaultDialer.Dial(wsURLQuotes, header)
	require.NoError(t, err)
	defer conn.Close()

	// 持续收 5 秒，每条打印到 console
	deadline := time.Now().Add(5 * time.Second)
	count := 0
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		count++
		fmt.Printf("[E2E realtime-quotes] #%d %s\n", count, string(msg))
		t.Logf("[E2E realtime-quotes] #%d %s", count, string(msg))
	}
	t.Logf("received %d messages in 5s", count)
	assert.Greater(t, count, 0, "expected at least one WS snapshot in 5s")

	// Disconnect
	reqDisconnect, _ := http.NewRequest("POST", ts.URL+"/api/v1/realtime-sources/"+sourceID+"/disconnect", nil)
	reqDisconnect.Header.Set("Authorization", "Bearer "+token)
	respDisconnect, err := http.DefaultClient.Do(reqDisconnect)
	require.NoError(t, err)
	respDisconnect.Body.Close()
	assert.Equal(t, http.StatusOK, respDisconnect.StatusCode)
}
