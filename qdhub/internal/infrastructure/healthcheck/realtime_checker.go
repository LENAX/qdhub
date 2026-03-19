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

var (
	// 北京时区，用于判断是否在交易时段
	beijingTZ *time.Location
)

func init() {
	beijingTZ, _ = time.LoadLocation("Asia/Shanghai")
	if beijingTZ == nil {
		beijingTZ = time.FixedZone("CST", 8*3600)
	}
}

// IsTradingWindow 判断给定时间（UTC）是否处于交易时段：北京时间 9:15–15:00，且为交易日。
// 若 provider 非 nil，则用 provider.IsTradingDay(ctx, date) 查 trade_cal 排除节假日；否则仅用星期（周一至周五）判断。
func IsTradingWindow(ctx context.Context, utcTime time.Time, provider TradingDayProvider) bool {
	t := utcTime.In(beijingTZ)
	hour, min := t.Hour(), t.Minute()
	minutes := hour*60 + min
	if minutes < 9*60+15 || minutes >= 15*60 {
		return false
	}
	date := t.Format("20060102")
	if provider != nil {
		ok, err := provider.IsTradingDay(ctx, date)
		if err != nil {
			return false
		}
		return ok
	}
	weekday := t.Weekday()
	return weekday != time.Sunday && weekday != time.Saturday
}

// IsBeijingTradingWindow 兼容旧调用：仅按星期判断，不查 trade_cal。新逻辑请用 IsTradingWindow(ctx, t, provider)。
func IsBeijingTradingWindow(utcTime time.Time) bool {
	return IsTradingWindow(context.Background(), utcTime, nil)
}

const (
	StatusHealthy     = "healthy"
	StatusUnhealthy   = "unhealthy"
	StatusUnavailable = "unavailable"
)

// Checker implements application-level RealtimeSourceHealthChecker (inject into RealtimeSourceApplicationService).
// TradingDayProvider 可选：非 nil 时用 trade_cal 表排除节假日；nil 时仅按星期判断。
type Checker struct {
	TradingDayProvider TradingDayProvider
}

// Check runs a one-off health check for the given RealtimeSource.
func (c Checker) Check(ctx context.Context, src *realtime.RealtimeSource) (status string, errMsg string, err error) {
	return check(ctx, src, c.TradingDayProvider)
}

// check runs a one-off health check for the given RealtimeSource and returns status, error message, and error.
func check(ctx context.Context, src *realtime.RealtimeSource, provider TradingDayProvider) (status string, errMsg string, err error) {
	switch src.Type {
	case realtime.TypeTushareProxy:
		return checkTushareProxy(ctx, src, provider)
	case realtime.TypeTushareWS:
		return checkTushareWS(ctx, src)
	case realtime.TypeSina:
		return checkSina(ctx, src)
	case realtime.TypeEastmoney:
		return checkEastmoney(ctx, src)
	case realtime.TypeNews:
		return StatusHealthy, "", nil
	default:
		return StatusUnavailable, fmt.Sprintf("unknown type: %s", src.Type), nil
	}
}

// Check is the package-level function for use by startup or tests（无 provider 时按星期判断）.
func Check(ctx context.Context, src *realtime.RealtimeSource) (status string, errMsg string, err error) {
	return check(ctx, src, nil)
}

func checkTushareProxy(ctx context.Context, src *realtime.RealtimeSource, provider TradingDayProvider) (string, string, error) {
	cfg, err := src.ConfigMap()
	if err != nil {
		return StatusUnhealthy, err.Error(), nil
	}
	wsURL, _ := cfg["ws_url"].(string)
	wsURL = strings.TrimSpace(wsURL)
	rsaPath, _ := cfg["rsa_public_key_path"].(string)
	rsaPath = strings.TrimSpace(rsaPath)
	if wsURL == "" || rsaPath == "" {
		return StatusUnhealthy, "missing ws_url or rsa_public_key_path in realtime source config (or set tushare.proxy_ws_url and tushare.proxy_rsa_public_key_path in config file / TUSHARE_PROXY_WS_URL and TUSHARE_PROXY_RSA_PUBLIC_KEY_PATH in env)", nil
	}
	return checkTushareProxyWithURL(ctx, wsURL, rsaPath, provider)
}

// CheckTushareProxyWithURL 用于启动自检等场景。provider 可选，非 nil 时用 trade_cal 排除节假日。
func CheckTushareProxyWithURL(ctx context.Context, wsURL, rsaPath string, provider TradingDayProvider) (status string, errMsg string, err error) {
	wsURL = strings.TrimSpace(wsURL)
	rsaPath = strings.TrimSpace(rsaPath)
	if wsURL == "" || rsaPath == "" {
		return StatusUnhealthy, "missing ws_url or rsa_public_key_path (set in realtime_sources.config or tushare.proxy_ws_url / tushare.proxy_rsa_public_key_path or env TUSHARE_PROXY_WS_URL / TUSHARE_PROXY_RSA_PUBLIC_KEY_PATH)", nil
	}
	return checkTushareProxyWithURL(ctx, wsURL, rsaPath, provider)
}

func checkTushareProxyWithURL(ctx context.Context, wsURL, rsaPath string, provider TradingDayProvider) (string, string, error) {
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
	// 非交易时段（非北京 9:15–15:00 且交易日，有 provider 时按 trade_cal 排除节假日）：建连+密钥交换即视为健康
	if !IsTradingWindow(ctx, time.Now().UTC(), provider) {
		return StatusHealthy, "", nil
	}
	// 交易时段：必须收到并解密至少一帧，否则报异常
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
		return StatusUnhealthy, err.Error(), nil
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
