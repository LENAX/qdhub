package realtime

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	coreRealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

var tushareWSDialer = &websocket.Dialer{
	TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
	HandshakeTimeout: 15 * time.Second,
	ReadBufferSize:   1024 * 1024,
	WriteBufferSize:  64 * 1024,
	EnableCompression: true,
}

const (
	tushareWSURL = "wss://ws.tushare.pro/listening"
)

var tushareTickRecordFields = []string{
	"code", "name", "trade_time", "pre_price", "price", "open", "high", "low", "close",
	"open_int", "volume", "amount", "num",
	"ask_price1", "ask_volume1", "bid_price1", "bid_volume1",
	"ask_price2", "ask_volume2", "bid_price2", "bid_volume2",
	"ask_price3", "ask_volume3", "bid_price3", "bid_volume3",
	"ask_price4", "ask_volume4", "bid_price4", "bid_volume4",
	"ask_price5", "ask_volume5", "bid_price5", "bid_volume5",
}

// TushareWSTickCollector 通过 tushare WS 持续消费全市场 tick。
type TushareWSTickCollector struct {
	Token        string
	TargetDBPath string
	Topic        string
	Codes        []string
}

var _ coreRealtime.DataCollector = (*TushareWSTickCollector)(nil)

func (c *TushareWSTickCollector) Run(
	ctx context.Context,
	cfg *coreRealtime.ContinuousTaskConfig,
	publish coreRealtime.PublishFunc,
) error {
	if strings.TrimSpace(c.Token) == "" {
		return fmt.Errorf("tushare ws token is required")
	}
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		start := time.Now()
		if err := c.runOnce(ctx, cfg, publish); err != nil {
			logrus.Warnf("[TushareWSTickCollector] stream stopped: %v", err)
		}
		if time.Since(start) > 30*time.Second {
			backoff = time.Second
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *TushareWSTickCollector) runOnce(
	ctx context.Context,
	cfg *coreRealtime.ContinuousTaskConfig,
	publish coreRealtime.PublishFunc,
) error {
	headers := http.Header{}
	headers.Set("User-Agent", "Mozilla/5.0")
	conn, _, err := tushareWSDialer.Dial(tushareWSURL, headers)
	if err != nil {
		return fmt.Errorf("dial ws: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(10 * 1024 * 1024)

	topic := strings.TrimSpace(c.Topic)
	if topic == "" {
		topic = "HQ_STK_TICK"
	}
	codes := c.Codes
	if len(codes) == 0 {
		codes = []string{"3*.SZ", "0*.SZ", "6*.SH"}
	}

	req := map[string]interface{}{
		"action": "listening",
		"token":  c.Token,
		"data": map[string]interface{}{
			topic: codes,
		},
	}
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("send listening request: %w", err)
	}
	logrus.Infof("[TushareWSTickCollector] subscribed topic=%s codes=%v", topic, codes)

	pingInterval := 30 * time.Second
	if cfg != nil && cfg.FlushInterval > 0 && cfg.FlushInterval < pingInterval {
		pingInterval = cfg.FlushInterval
	}
	pingTicker := time.NewTicker(pingInterval)
	defer pingTicker.Stop()

	errCh := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pingTicker.C:
				if werr := conn.WriteJSON(map[string]interface{}{"action": "ping"}); werr != nil {
					select {
					case errCh <- fmt.Errorf("send ping: %w", werr):
					default:
					}
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case werr := <-errCh:
			return werr
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read message: %w", err)
		}
		rows, err := c.parseRows(msg)
		if err != nil {
			logrus.Warnf("[TushareWSTickCollector] parse message failed: %v", err)
			continue
		}
		if len(rows) == 0 {
			continue
		}
		event := coreRealtime.NewRealtimeEvent(coreRealtime.EventDataArrived, "", "", &coreRealtime.DataArrivedPayload{
			Data:   rows,
			Source: "tushare_ws",
		})
		if err := publish(event); err != nil {
			logrus.Warnf("[TushareWSTickCollector] publish failed: %v", err)
		}
	}
}

func (c *TushareWSTickCollector) parseRows(msg []byte) ([]map[string]interface{}, error) {
	var resp map[string]interface{}
	if err := json.Unmarshal(msg, &resp); err != nil {
		return nil, err
	}
	if status, ok := resp["status"].(bool); ok && !status {
		if m, _ := resp["message"].(string); m != "" {
			return nil, fmt.Errorf("upstream status=false: %s", m)
		}
		return nil, fmt.Errorf("upstream status=false")
	}
	data, ok := resp["data"].(map[string]interface{})
	if !ok || data == nil {
		return nil, nil
	}

	code, _ := data["code"].(string)
	record := data["record"]
	if record == nil {
		return nil, nil
	}
	row := normalizeTushareTickRecord(record, code)
	if row == nil {
		return nil, nil
	}
	if c.TargetDBPath != "" {
		row["target_db_path"] = c.TargetDBPath
	}
	return []map[string]interface{}{row}, nil
}

func normalizeTushareTickRecord(raw interface{}, code string) map[string]interface{} {
	switch v := raw.(type) {
	case map[string]interface{}:
		if code == "" {
			if c, _ := v["code"].(string); strings.TrimSpace(c) != "" {
				code = strings.TrimSpace(c)
			}
		}
		if code != "" {
			v["code"] = code
			if _, ok := v["ts_code"]; !ok || strings.TrimSpace(fmt.Sprintf("%v", v["ts_code"])) == "" {
				v["ts_code"] = code
			}
		}
		if c, _ := v["ts_code"].(string); strings.TrimSpace(c) != "" {
			if _, ok := v["code"]; !ok || strings.TrimSpace(fmt.Sprintf("%v", v["code"])) == "" {
				v["code"] = c
			}
		}
		return v
	case []interface{}:
		out := make(map[string]interface{}, len(tushareTickRecordFields))
		for i, field := range tushareTickRecordFields {
			if i < len(v) {
				out[field] = v[i]
			}
		}
		if out["code"] == nil && code != "" {
			out["code"] = code
		}
		if out["ts_code"] == nil {
			if out["code"] != nil {
				out["ts_code"] = out["code"]
			} else if code != "" {
				out["ts_code"] = code
			}
		}
		if out["code"] == nil && out["ts_code"] != nil {
			out["code"] = out["ts_code"]
		}
		if out["ts_code"] == nil && code != "" {
			out["ts_code"] = code
		}
		return out
	default:
		return nil
	}
}
