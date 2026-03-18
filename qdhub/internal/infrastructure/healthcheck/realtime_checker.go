// Package healthcheck provides per-type health checks for realtime data sources.
package healthcheck

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"qdhub/internal/domain/realtime"
	"qdhub/pkg/crypto"
)

const (
	StatusHealthy     = "healthy"
	StatusUnhealthy   = "unhealthy"
	StatusUnavailable = "unavailable"
)

// Checker implements application-level RealtimeSourceHealthChecker (inject into RealtimeSourceApplicationService).
type Checker struct{}

// Check runs a one-off health check for the given RealtimeSource.
func (Checker) Check(ctx context.Context, src *realtime.RealtimeSource) (status string, errMsg string, err error) {
	return check(ctx, src)
}

// check runs a one-off health check for the given RealtimeSource and returns status, error message, and error.
func check(ctx context.Context, src *realtime.RealtimeSource) (status string, errMsg string, err error) {
	switch src.Type {
	case realtime.TypeTushareForward:
		return checkTushareForward(ctx, src)
	case realtime.TypeTushareWS:
		return checkTushareWS(ctx, src)
	case realtime.TypeSina:
		return checkSina(ctx, src)
	case realtime.TypeEastmoney:
		return checkEastmoney(ctx, src)
	default:
		return StatusUnavailable, "", fmt.Errorf("unknown type: %s", src.Type)
	}
}

// Check is the package-level function for use by startup or tests.
func Check(ctx context.Context, src *realtime.RealtimeSource) (status string, errMsg string, err error) {
	return check(ctx, src)
}

func checkTushareForward(ctx context.Context, src *realtime.RealtimeSource) (string, string, error) {
	cfg, err := src.ConfigMap()
	if err != nil {
		return StatusUnhealthy, err.Error(), err
	}
	wsURL, _ := cfg["ws_url"].(string)
	wsURL = strings.TrimSpace(wsURL)
	rsaPath, _ := cfg["rsa_public_key_path"].(string)
	rsaPath = strings.TrimSpace(rsaPath)
	if wsURL == "" || rsaPath == "" {
		return StatusUnhealthy, "missing ws_url or rsa_public_key_path in config", nil
	}
	return checkTushareForwardWithURL(ctx, wsURL, rsaPath)
}

// CheckTushareForwardWithURL 用于启动自检等场景：当 DB 中源 config 为空时，可用应用配置（config/env）的 ws_url、rsa_path 做连通性检查。
func CheckTushareForwardWithURL(ctx context.Context, wsURL, rsaPath string) (status string, errMsg string, err error) {
	wsURL = strings.TrimSpace(wsURL)
	rsaPath = strings.TrimSpace(rsaPath)
	if wsURL == "" || rsaPath == "" {
		return StatusUnhealthy, "missing ws_url or rsa_public_key_path", nil
	}
	return checkTushareForwardWithURL(ctx, wsURL, rsaPath)
}

func checkTushareForwardWithURL(ctx context.Context, wsURL, rsaPath string) (string, string, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return StatusUnavailable, err.Error(), nil
	}
	defer conn.Close()
	conn.SetReadLimit(4 << 20)

	pub, err := crypto.LoadRSAPublicKeyFromFile(rsaPath)
	if err != nil {
		return StatusUnhealthy, "load RSA public key: " + err.Error(), nil
	}
	aesKey, err := crypto.GenerateKey(32)
	if err != nil {
		return StatusUnhealthy, "generate AES key: " + err.Error(), nil
	}
	encKey, err := crypto.EncryptAESKeyWithRSA(pub, aesKey)
	if err != nil {
		return StatusUnhealthy, "encrypt AES key: " + err.Error(), nil
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteMessage(websocket.BinaryMessage, encKey); err != nil {
		return StatusUnavailable, "send key: " + err.Error(), nil
	}
	sess, err := crypto.NewSessionCipher(aesKey)
	if err != nil {
		return StatusUnhealthy, "session cipher: " + err.Error(), nil
	}
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return StatusUnavailable, "read after key: " + err.Error(), nil
	}
	if _, err := sess.Decrypt(msg); err != nil {
		return StatusUnhealthy, "decrypt first frame: " + err.Error(), nil
	}
	return StatusHealthy, "", nil
}

func checkTushareWS(ctx context.Context, src *realtime.RealtimeSource) (string, string, error) {
	cfg, err := src.ConfigMap()
	if err != nil {
		return StatusUnhealthy, err.Error(), err
	}
	endpoint, _ := cfg["endpoint"].(string)
	if endpoint == "" {
		endpoint = "wss://ws.tushare.pro/listening"
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return StatusUnavailable, err.Error(), nil
	}
	conn.Close()
	return StatusHealthy, "", nil
}

func checkSina(ctx context.Context, src *realtime.RealtimeSource) (string, string, error) {
	// Probe a known sina quote URL
	url := "https://hq.sinajs.cn/list=sh000001"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return StatusUnhealthy, err.Error(), nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; QDHub/1.0)")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return StatusUnavailable, err.Error(), nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return StatusUnavailable, fmt.Sprintf("HTTP %d", resp.StatusCode), nil
	}
	return StatusHealthy, "", nil
}

func checkEastmoney(ctx context.Context, src *realtime.RealtimeSource) (string, string, error) {
	// Probe a known eastmoney API
	url := "https://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&fields=f43,f44,f45,f46,f47,f48,f49,f50,f51,f52"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return StatusUnhealthy, err.Error(), nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; QDHub/1.0)")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return StatusUnavailable, err.Error(), nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return StatusUnavailable, fmt.Sprintf("HTTP %d", resp.StatusCode), nil
	}
	return StatusHealthy, "", nil
}
