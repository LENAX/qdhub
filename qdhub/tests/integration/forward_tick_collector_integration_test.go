//go:build integration
// +build integration

package integration

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreRealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/internal/infrastructure/datasource/tushare/realtime"
	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/pkg/crypto"
)

// mockForwardWSServer 模拟 ts_proxy 转发端：接受连接后读一条 RSA 加密的 AES 密钥，再发一条 AES-GCM 加密的 tick JSON。
func mockForwardWSServer(t *testing.T, priv *rsa.PrivateKey, tickRow map[string]interface{}) *httptest.Server {
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

func TestForwardTickCollector_Integration(t *testing.T) {
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
	srv := mockForwardWSServer(t, priv, tickRow)
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:] + "/realtime"

	var gotPayload *coreRealtime.DataArrivedPayload
	var gotMu sync.Mutex
	done := make(chan struct{})
	publish := func(ev *coreRealtime.RealtimeEvent) error {
		if ev == nil || ev.Payload == nil {
			return nil
		}
		if p, ok := ev.Payload.(*coreRealtime.DataArrivedPayload); ok {
			gotMu.Lock()
			gotPayload = p
			gotMu.Unlock()
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil
	}

	selector := realtimestore.NewRealtimeSourceSelector()
	selector.SwitchTo(realtimestore.SourceTushareProxy)

	collector := &realtime.ForwardTickCollector{
		ForwardWSURL:            wsURL,
		ForwardRSAPublicKeyPath: pubPath,
		Selector:                selector,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = collector.Run(ctx, &coreRealtime.ContinuousTaskConfig{}, publish)
	}()

	select {
	case <-done:
		// 收到一条 tick
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for forwarded tick")
	}

	gotMu.Lock()
	p := gotPayload
	gotMu.Unlock()
	require.NotNil(t, p)
	assert.Equal(t, realtimestore.SourceTushareProxy, p.Source)
	data, ok := p.Data.([]map[string]interface{})
	require.True(t, ok, "Data should be []map[string]interface{}")
	require.Len(t, data, 1)
	assert.Equal(t, "000001.SZ", data[0]["ts_code"])

	cancel()
}

// mockForwardWSServerStreaming 持续 duration 时间内每隔 interval 发送一条加密 tick（含 seq 序号），用于 15 秒流式测试。
func mockForwardWSServerStreaming(t *testing.T, priv *rsa.PrivateKey, baseRow map[string]interface{}, duration, interval time.Duration) *httptest.Server {
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

// TestForwardTickCollector_Integration_Streaming15s 持续约 15 秒流式传输：mock 端按间隔发 tick，客户端应收至少 20 条。
func TestForwardTickCollector_Integration_Streaming15s(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	dir := t.TempDir()
	pubPath := filepath.Join(dir, "pub.pem")
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)
	err = os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: pubBytes}), 0600)
	require.NoError(t, err)

	baseRow := map[string]interface{}{"ts_code": "000001.SZ", "price": 10.5, "vol": 1000.0}
	streamDuration := 15 * time.Second
	streamInterval := 500 * time.Millisecond
	srv := mockForwardWSServerStreaming(t, priv, baseRow, streamDuration, streamInterval)
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:] + "/realtime"

	var ticks []*coreRealtime.DataArrivedPayload
	var mu sync.Mutex
	publish := func(ev *coreRealtime.RealtimeEvent) error {
		if ev == nil || ev.Payload == nil {
			return nil
		}
		if p, ok := ev.Payload.(*coreRealtime.DataArrivedPayload); ok {
			mu.Lock()
			ticks = append(ticks, p)
			mu.Unlock()
		}
		return nil
	}

	selector := realtimestore.NewRealtimeSourceSelector()
	selector.SwitchTo(realtimestore.SourceTushareProxy)
	collector := &realtime.ForwardTickCollector{
		ForwardWSURL:            wsURL,
		ForwardRSAPublicKeyPath: pubPath,
		Selector:                selector,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = collector.Run(ctx, &coreRealtime.ContinuousTaskConfig{}, publish)
	}()

	time.Sleep(streamDuration + 2*time.Second)

	mu.Lock()
	snapshot := make([]*coreRealtime.DataArrivedPayload, len(ticks))
	copy(snapshot, ticks)
	mu.Unlock()
	cancel()

	assert.GreaterOrEqual(t, len(snapshot), 20, "expected at least 20 ticks in 15s streaming (interval 500ms)")
	for i, p := range snapshot {
		assert.Equal(t, realtimestore.SourceTushareProxy, p.Source)
		data, ok := p.Data.([]map[string]interface{})
		require.True(t, ok)
		require.Len(t, data, 1)
		assert.Equal(t, "000001.SZ", data[0]["ts_code"])
		if i < 3 {
			assert.NotNil(t, data[0]["seq"])
		}
	}
}

// mockForwardWSServerInterruptReconnect 第一次连接发 ticksFirst 条后主动断开，后续连接每次发 ticksPerConn 条后断开，用于重连测试。
func mockForwardWSServerInterruptReconnect(t *testing.T, priv *rsa.PrivateKey, baseRow map[string]interface{}, ticksFirst, ticksPerConn int) *httptest.Server {
	upgrader := websocket.Upgrader{}
	var connCount atomic.Int32
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

		n := ticksPerConn
		if connCount.Load() == 0 {
			n = ticksFirst
		}
		connCount.Add(1)

		for i := 0; i < n; i++ {
			row := make(map[string]interface{})
			for k, v := range baseRow {
				row[k] = v
			}
			row["seq"] = i
			row["conn"] = connCount.Load()
			body, err := json.Marshal(row)
			require.NoError(t, err)
			enc, err := sess.Encrypt(body)
			require.NoError(t, err)
			if err := conn.WriteMessage(websocket.BinaryMessage, enc); err != nil {
				return
			}
			time.Sleep(80 * time.Millisecond)
		}
		// 主动关闭连接，触发客户端 runOnce 返回并进入重连
	})
	return httptest.NewServer(mux)
}

// TestForwardTickCollector_Integration_InterruptReconnect 模拟中途断线重连：第一次连接发 3 条后断开，重连后再发 3 条，共至少 6 条。
func TestForwardTickCollector_Integration_InterruptReconnect(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	dir := t.TempDir()
	pubPath := filepath.Join(dir, "pub.pem")
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)
	err = os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: pubBytes}), 0600)
	require.NoError(t, err)

	baseRow := map[string]interface{}{"ts_code": "000001.SZ", "price": 10.5}
	srv := mockForwardWSServerInterruptReconnect(t, priv, baseRow, 3, 3)
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:] + "/realtime"

	var ticks []*coreRealtime.DataArrivedPayload
	var mu sync.Mutex
	publish := func(ev *coreRealtime.RealtimeEvent) error {
		if ev == nil || ev.Payload == nil {
			return nil
		}
		if p, ok := ev.Payload.(*coreRealtime.DataArrivedPayload); ok {
			mu.Lock()
			ticks = append(ticks, p)
			mu.Unlock()
		}
		return nil
	}

	selector := realtimestore.NewRealtimeSourceSelector()
	selector.SwitchTo(realtimestore.SourceTushareProxy)
	collector := &realtime.ForwardTickCollector{
		ForwardWSURL:            wsURL,
		ForwardRSAPublicKeyPath: pubPath,
		Selector:                selector,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = collector.Run(ctx, &coreRealtime.ContinuousTaskConfig{}, publish)
	}()

	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(ticks)
		mu.Unlock()
		if n >= 6 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	mu.Lock()
	snapshot := make([]*coreRealtime.DataArrivedPayload, len(ticks))
	copy(snapshot, ticks)
	mu.Unlock()
	cancel()

	assert.GreaterOrEqual(t, len(snapshot), 6, "expected at least 6 ticks (3 before disconnect + 3 after reconnect)")
	for _, p := range snapshot {
		assert.Equal(t, realtimestore.SourceTushareProxy, p.Source)
		data, ok := p.Data.([]map[string]interface{})
		require.True(t, ok)
		require.Len(t, data, 1)
		assert.Equal(t, "000001.SZ", data[0]["ts_code"])
	}
}
