package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"

	coreRealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/pkg/crypto"
)

const (
	forwardFlushInterval = 3 * time.Second
	forwardReadDeadline  = 90 * time.Second
	forwardPongWrite     = 10 * time.Second
)

// ForwardTickCollector 从 ts_proxy 转发端接收加密 tick 流（方案 B：RSA 交换 AES + AES 解密）。
// ts_proxy 端以 JSON 数组批量推送 tick，本端兼容单条 JSON 对象（旧协议）和 JSON 数组（新协议）。
type ForwardTickCollector struct {
	ForwardWSURL            string // 如 ws://host:8888/realtime
	ForwardRSAPublicKeyPath string // 转发端 RSA 公钥路径，用于加密本端生成的 AES 密钥
	TargetDBPath            string
	Selector                *realtimestore.RealtimeSourceSelector
}

var _ coreRealtime.DataCollector = (*ForwardTickCollector)(nil)

func (c *ForwardTickCollector) Run(
	ctx context.Context,
	cfg *coreRealtime.ContinuousTaskConfig,
	publish coreRealtime.PublishFunc,
) error {
	if strings.TrimSpace(c.ForwardWSURL) == "" {
		return fmt.Errorf("forward WS URL is required")
	}
	if strings.TrimSpace(c.ForwardRSAPublicKeyPath) == "" {
		return fmt.Errorf("forward RSA public key path is required")
	}
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		start := time.Now()
		if err := c.runOnce(ctx, publish); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logrus.Warnf("[ForwardTickCollector] runOnce: %v", err)
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

func (c *ForwardTickCollector) runOnce(ctx context.Context, publish coreRealtime.PublishFunc) error {
	conn, _, err := websocket.DefaultDialer.Dial(c.ForwardWSURL, nil)
	if err != nil {
		return fmt.Errorf("dial forward: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(4 << 20)

	pub, err := crypto.LoadRSAPublicKeyFromFile(c.ForwardRSAPublicKeyPath)
	if err != nil {
		return fmt.Errorf("load RSA public key: %w", err)
	}
	aesKey, err := crypto.GenerateKey(32)
	if err != nil {
		return fmt.Errorf("generate AES key: %w", err)
	}
	encKey, err := crypto.EncryptAESKeyWithRSA(pub, aesKey)
	if err != nil {
		return fmt.Errorf("encrypt AES key: %w", err)
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := conn.WriteMessage(websocket.BinaryMessage, encKey); err != nil {
		return fmt.Errorf("send key: %w", err)
	}
	conn.SetWriteDeadline(time.Time{})
	logrus.Infof("[ForwardTickCollector] scheme B key sent, reading tick stream")

	conn.SetReadDeadline(time.Now().Add(forwardReadDeadline))
	conn.SetPingHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(forwardReadDeadline))
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(forwardPongWrite))
	})

	sess, err := crypto.NewSessionCipher(aesKey)
	if err != nil {
		return fmt.Errorf("session cipher: %w", err)
	}

	store := realtimestore.DefaultLatestQuoteStore()
	batch := make([]map[string]interface{}, 0, 256)

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		event := coreRealtime.NewRealtimeEvent(coreRealtime.EventDataArrived, "", "", &coreRealtime.DataArrivedPayload{
			Data:   batch,
			Source: realtimestore.SourceTushareProxy,
		})
		if err := publish(event); err != nil {
			logrus.Warnf("[ForwardTickCollector] publish batch(%d): %v", len(batch), err)
			return
		}
		batch = make([]map[string]interface{}, 0, 256)
		runtime.GC()
	}

	type readResult struct {
		msg []byte
		err error
	}
	readCh := make(chan readResult, 1)
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			readCh <- readResult{msg, err}
			if err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(forwardFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			return nil
		case <-ticker.C:
			flushBatch()
		case r := <-readCh:
			if r.err != nil {
				flushBatch()
				return fmt.Errorf("read: %w", r.err)
			}
			plain, err := sess.Decrypt(r.msg)
			if err != nil {
				logrus.Warnf("[ForwardTickCollector] decrypt: %v", err)
				continue
			}
			rows := c.parseTicks(plain)
			for _, row := range rows {
				if c.TargetDBPath != "" {
					row["target_db_path"] = c.TargetDBPath
				}
				if tsCode, _ := row["ts_code"].(string); tsCode != "" {
					if c.Selector == nil || c.Selector.ShouldWriteToStore(realtimestore.SourceTushareProxy) {
						store.Update(tsCode, row)
					}
				}
				batch = append(batch, row)
			}
		}
	}
}

// parseTicks 兼容新协议（JSON 数组批量）和旧协议（单条 JSON 对象）。
func (c *ForwardTickCollector) parseTicks(data []byte) []map[string]interface{} {
	if len(data) == 0 {
		return nil
	}
	if data[0] == '[' {
		var rows []map[string]interface{}
		if err := json.Unmarshal(data, &rows); err != nil {
			logrus.Warnf("[ForwardTickCollector] unmarshal array: %v", err)
			return nil
		}
		return rows
	}
	var row map[string]interface{}
	if err := json.Unmarshal(data, &row); err != nil {
		logrus.Warnf("[ForwardTickCollector] unmarshal: %v", err)
		return nil
	}
	if row == nil {
		return nil
	}
	return []map[string]interface{}{row}
}
