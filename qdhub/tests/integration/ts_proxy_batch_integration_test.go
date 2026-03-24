//go:build integration
// +build integration

package integration

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qdhub/pkg/crypto"
	"qdhub/ts_proxy/normalize"
	"qdhub/ts_proxy/server"
)

// TestTSProxy_Broadcast_BatchEncryptedArray 集成：真实 ts_proxy Broadcast 将多条 tick 攒批为 JSON 数组后 AES 加密下发，客户端解密后应为数组。
func TestTSProxy_Broadcast_BatchEncryptedArray(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	dir := t.TempDir()
	privPath := filepath.Join(dir, "private.pem")
	pubPath := filepath.Join(dir, "pub.pem")
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	require.NoError(t, os.WriteFile(privPath, privPEM, 0600))
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: pubBytes}), 0600))

	bcast, err := server.NewBroadcast(privPath)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/realtime", bcast.ServeWS)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	wsURL := "ws" + srv.URL[4:] + "/realtime"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	defer conn.Close()

	pub, err := crypto.LoadRSAPublicKeyFromFile(pubPath)
	require.NoError(t, err)
	aesKey, err := crypto.GenerateKey(32)
	require.NoError(t, err)
	encKey, err := crypto.EncryptAESKeyWithRSA(pub, aesKey)
	require.NoError(t, err)
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, encKey))
	conn.SetWriteDeadline(time.Time{})

	sess, err := crypto.NewSessionCipher(aesKey)
	require.NoError(t, err)

	// 等待服务端完成 Scheme B 并入队 session，避免 PushTick 早于 session 注册
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 3; i++ {
		bcast.PushTick(normalize.TickRow{
			"ts_code": "000001.SZ",
			"price":   float64(10 + i),
		})
	}
	// server 端 writeLoop 每 batchInterval(1s) flush 一次
	time.Sleep(1500 * time.Millisecond)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(15*time.Second)))
	_, enc, err := conn.ReadMessage()
	require.NoError(t, err)
	plain, err := sess.Decrypt(enc)
	require.NoError(t, err)
	assert.Equal(t, byte('['), plain[0], "payload should be JSON array")

	var arr []map[string]interface{}
	require.NoError(t, json.Unmarshal(plain, &arr))
	require.GreaterOrEqual(t, len(arr), 1, "at least one tick in batch")
	found := false
	for _, row := range arr {
		if tc, _ := row["ts_code"].(string); tc == "000001.SZ" {
			found = true
			break
		}
	}
	assert.True(t, found, "batch should contain ts_code 000001.SZ")
}
