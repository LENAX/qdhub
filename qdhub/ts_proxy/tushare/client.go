package tushare

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"qdhub/ts_proxy/normalize"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const (
	TushareWSURL   = "wss://ws.tushare.pro/listening"
	PingInterval   = 30 * time.Second
	DefaultTopic   = "HQ_STK_TICK"
	DefaultReconnectMax = 30
)

// DefaultCodes is the default subscription codes (full market).
var DefaultCodes = []string{"3*.SZ", "0*.SZ", "6*.SH"}

// Client connects to Tushare WS, subscribes, and pushes normalized rows to OnTick.
type Client struct {
	Token     string
	Topic     string
	Codes     []string
	ReconnectMax int // 0 = unlimited
	OnTick    func(row normalize.TickRow)
	Dialer    *websocket.Dialer

	mu       sync.Mutex
	conn     *websocket.Conn
	reconnectCount int
}

// Run connects, sends listening, then reads messages and calls OnTick for each normalized row.
// On disconnect, reconnects with backoff until ReconnectMax (default 30) is reached.
func (c *Client) Run(ctx context.Context) error {
	if strings.TrimSpace(c.Token) == "" {
		return fmt.Errorf("tushare token is required")
	}
	if c.OnTick == nil {
		return fmt.Errorf("OnTick is required")
	}
	if c.Topic == "" {
		c.Topic = DefaultTopic
	}
	if len(c.Codes) == 0 {
		c.Codes = DefaultCodes
	}
	maxRetry := c.ReconnectMax
	if maxRetry <= 0 {
		maxRetry = DefaultReconnectMax
	}

	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		c.mu.Lock()
		count := c.reconnectCount
		c.mu.Unlock()
		if count >= maxRetry {
			logrus.Errorf("[tushare] reconnect max %d reached, stopping", maxRetry)
			return fmt.Errorf("reconnect max %d reached", maxRetry)
		}

		err := c.runOnce(ctx)
		if err != nil && ctx.Err() != nil {
			return nil
		}
		if err != nil {
			logrus.Warnf("[tushare] runOnce failed: %v", err)
		}

		c.mu.Lock()
		c.reconnectCount++
		c.mu.Unlock()

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

func (c *Client) runOnce(ctx context.Context) error {
	dialer := c.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	headers := http.Header{}
	headers.Set("User-Agent", "Mozilla/5.0")
	conn, _, err := dialer.Dial(TushareWSURL, headers)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	conn.SetReadLimit(10 * 1024 * 1024)

	req := map[string]interface{}{
		"action": "listening",
		"token":  c.Token,
		"data":   map[string]interface{}{c.Topic: c.Codes},
	}
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("write listening: %w", err)
	}
	logrus.Infof("[tushare] subscribed topic=%s codes=%v", c.Topic, c.Codes)

	pingTicker := time.NewTicker(PingInterval)
	defer pingTicker.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			code, record, err := normalize.ParseTushareMessage(msg)
			if err != nil {
				logrus.Warnf("[tushare] parse message: %v", err)
				continue
			}
			if record == nil {
				continue
			}
			row := normalize.NormalizeTushareRecord(record, code)
			if row != nil {
				c.OnTick(row)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return nil
		case <-done:
			return fmt.Errorf("read loop exited")
		case <-pingTicker.C:
			if err := conn.WriteJSON(map[string]string{"action": "ping"}); err != nil {
				return fmt.Errorf("ping: %w", err)
			}
		}
	}
}
