//go:build e2e
// +build e2e

// Package e2e 转发端实时行情 E2E：使用 mock ts_proxy 验证 QDHub 从转发端接收 tick 并写入 Store。
package e2e

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LENAX/task-engine/pkg/core/engine"
	"github.com/LENAX/task-engine/pkg/storage/sqlite"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/workflow"
	"qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/persistence"
	"qdhub/internal/infrastructure/persistence/repository"
	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/internal/infrastructure/taskengine"
	"qdhub/pkg/crypto"
)

func mockForwardWSServerE2E(t *testing.T, priv *rsa.PrivateKey, tickRow map[string]interface{}) *httptest.Server {
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

		body, err := json.Marshal(tickRow)
		require.NoError(t, err)
		enc, err := sess.Encrypt(body)
		require.NoError(t, err)
		err = conn.WriteMessage(websocket.BinaryMessage, enc)
		require.NoError(t, err)
	})
	return httptest.NewServer(mux)
}

// TestE2E_ForwardRealtimeTick 验证：forward 开关 + mock ts_proxy 下，ExecuteRealtimeDataSync(ts_realtime_mkt_tick) 使用转发端并收到一条 tick 写入 LatestQuoteStore。
func TestE2E_ForwardRealtimeTick(t *testing.T) {
	realtimestore.DefaultLatestQuoteStore().Clear()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	dir := t.TempDir()
	pubPath := filepath.Join(dir, "pub.pem")
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)
	err = os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: pubBytes}), 0600)
	require.NoError(t, err)

	tickRow := map[string]interface{}{
		"ts_code": "000001.SZ",
		"price":   10.5,
		"vol":     1000.0,
	}
	srv := mockForwardWSServerE2E(t, priv, tickRow)
	defer srv.Close()
	wsURL := "ws" + srv.URL[4:] + "/realtime"

	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "e2e_forward.db")
	db, err := persistence.NewDB(dsn)
	require.NoError(t, err)
	defer db.Close()

	aggregateRepo, err := sqlite.NewWorkflowAggregateRepoFromDSN(dsn)
	require.NoError(t, err)
	eng, err := engine.NewEngineWithAggregateRepo(5, 30, aggregateRepo)
	require.NoError(t, err)
	err = eng.Start(ctx)
	require.NoError(t, err)
	defer eng.Stop()

	taskEngineDeps := &taskengine.Dependencies{
		DataSourceRegistry:      nil,
		MetadataRepo:             nil,
		RealtimeAdapterRegistry:  realtime.NewDefaultRegistry(), // 非 nil 以满足 executeRealtimeStreaming 校验
		RealtimeBufferRegistry:   nil,
	}
	err = taskengine.Initialize(ctx, eng, taskEngineDeps)
	require.NoError(t, err)

	workflowRepo, err := repository.NewWorkflowDefinitionRepository(db)
	require.NoError(t, err)
	taskEngineAdapter := taskengine.NewTaskEngineAdapter(eng, 0)
	metadataRepo := repository.NewMetadataRepository(db)

	selector := realtimestore.NewRealtimeSourceSelector()
	selector.SwitchTo(realtimestore.SourceTushareProxy)

	workflowExecutor := taskengine.NewWorkflowExecutor(
		workflowRepo,
		taskEngineAdapter,
		metadataRepo,
		taskEngineDeps.RealtimeAdapterRegistry,
		"production", // 避免 ts_realtime_mkt_tick 被替换为 realtime_quote
		selector,
		nil, // no RealtimeSource table in this e2e; use env
		"forward",
		wsURL,
		pubPath,
		0,
	)

	req := workflow.RealtimeDataSyncRequest{
		DataSourceID:   shared.NewID(),
		DataSourceName: "tushare",
		Token:          "e2e-token",
		TargetDBPath:   filepath.Join(t.TempDir(), "tick.duckdb"),
		APINames:       []string{"ts_realtime_mkt_tick"},
	}
	instanceID, err := workflowExecutor.ExecuteRealtimeDataSync(ctx, req)
	require.NoError(t, err)
	require.False(t, instanceID.IsEmpty())

	store := realtimestore.DefaultLatestQuoteStore()
	deadline := time.Now().Add(15 * time.Second)
	var quote map[string]interface{}
	for time.Now().Before(deadline) {
		quote, _ = store.Get("000001.SZ")
		if quote != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NotNil(t, quote, "expected one tick 000001.SZ in LatestQuoteStore within 15s")
	require.Equal(t, "000001.SZ", quote["ts_code"])
}
