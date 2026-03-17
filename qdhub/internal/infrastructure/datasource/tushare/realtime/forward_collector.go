package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	coreRealtime "github.com/LENAX/task-engine/pkg/core/realtime"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	"qdhub/internal/infrastructure/realtimestore"
	"qdhub/pkg/crypto"
)

// ForwardTickCollector 从 ts_proxy 转发端接收加密 tick 流（方案 B：RSA 交换 AES + AES 解密）。
type ForwardTickCollector struct {
	ForwardWSURL           string // 如 ws://host:8888/realtime
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
		if err := c.runOnce(ctx, publish); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logrus.Warnf("[ForwardTickCollector] runOnce: %v", err)
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
	logrus.Infof("[ForwardTickCollector] scheme B key sent, reading tick stream")

	sess, err := crypto.NewSessionCipher(aesKey)
	if err != nil {
		return fmt.Errorf("session cipher: %w", err)
	}

	store := realtimestore.DefaultLatestQuoteStore()
	writeStore := c.Selector == nil || c.Selector.ShouldWriteToStore(realtimestore.SourceTushareForward)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		plain, err := sess.Decrypt(msg)
		if err != nil {
			logrus.Warnf("[ForwardTickCollector] decrypt: %v", err)
			continue
		}
		var row map[string]interface{}
		if err := json.Unmarshal(plain, &row); err != nil {
			logrus.Warnf("[ForwardTickCollector] unmarshal: %v", err)
			continue
		}
		if row == nil {
			continue
		}
		if c.TargetDBPath != "" {
			row["target_db_path"] = c.TargetDBPath
		}
		rows := []map[string]interface{}{row}
		if tsCode, _ := row["ts_code"].(string); tsCode != "" {
			if writeStore {
				store.Update(tsCode, row)
			}
		}
		event := coreRealtime.NewRealtimeEvent(coreRealtime.EventDataArrived, "", "", &coreRealtime.DataArrivedPayload{
			Data:   rows,
			Source: realtimestore.SourceTushareForward,
		})
		if err := publish(event); err != nil {
			logrus.Warnf("[ForwardTickCollector] publish: %v", err)
		}
	}
}
