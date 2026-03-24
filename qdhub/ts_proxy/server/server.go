package server

import (
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"qdhub/ts_proxy/crypto"
	"qdhub/ts_proxy/normalize"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const (
	writeWait      = 30 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 4 << 20
	batchInterval  = time.Second
	tickChSize     = 16384
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 256 << 10,
	CheckOrigin:     func(*http.Request) bool { return true },
}

// Session holds a client connection and its session AES cipher after scheme B.
type Session struct {
	conn   *websocket.Conn
	cipher *crypto.SessionCipher
	mu     sync.Mutex
	tickCh chan json.RawMessage
	done   chan struct{}
}

func (s *Session) writeLoop() {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			if err := s.flushBatch(); err != nil {
				logrus.Warnf("[ts_proxy] flush batch: %v, closing session", err)
				_ = s.conn.Close()
				return
			}
		}
	}
}

func (s *Session) flushBatch() error {
	var batch []json.RawMessage
	for {
		select {
		case tick := <-s.tickCh:
			batch = append(batch, tick)
		default:
			goto flush
		}
	}
flush:
	if len(batch) == 0 {
		return nil
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	ciphertext, err := s.cipher.Encrypt(payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.conn.SetWriteDeadline(time.Now().Add(writeWait))
	err = s.conn.WriteMessage(websocket.BinaryMessage, ciphertext)
	s.mu.Unlock()
	return err
}

// Broadcast broadcasts encrypted tick to all sessions that have completed key exchange.
type Broadcast struct {
	mu       sync.Mutex
	sessions []*Session
	rsaPriv  *rsa.PrivateKey
}

// NewBroadcast creates a broadcast that will use the given RSA private key to decrypt client-sent AES keys.
func NewBroadcast(rsaPrivateKeyPath string) (*Broadcast, error) {
	priv, err := crypto.LoadRSAPrivateKeyFromFile(rsaPrivateKeyPath)
	if err != nil {
		return nil, err
	}
	return &Broadcast{rsaPriv: priv}, nil
}

// ServeWS handles a single WebSocket connection: first frame = RSA-encrypted AES key (scheme B), then server pushes encrypted ticks via batched writes.
func (b *Broadcast) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logrus.Warnf("[ts_proxy] upgrade: %v", err)
		return
	}
	defer conn.Close()
	conn.SetReadLimit(256)
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	_, keyMsg, err := conn.ReadMessage()
	if err != nil {
		logrus.Warnf("[ts_proxy] read key frame: %v", err)
		return
	}
	aesKey, err := crypto.DecryptAESKeyWithRSA(b.rsaPriv, keyMsg)
	if err != nil {
		logrus.Warnf("[ts_proxy] decrypt AES key: %v", err)
		return
	}
	cipher, err := crypto.NewSessionCipher(aesKey)
	if err != nil {
		logrus.Warnf("[ts_proxy] session cipher: %v", err)
		return
	}
	sess := &Session{
		conn:   conn,
		cipher: cipher,
		tickCh: make(chan json.RawMessage, tickChSize),
		done:   make(chan struct{}),
	}
	b.mu.Lock()
	b.sessions = append(b.sessions, sess)
	b.mu.Unlock()
	defer close(sess.done)
	defer b.remove(sess)

	conn.SetReadLimit(0)
	conn.SetReadDeadline(time.Time{})
	conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-sess.done:
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(writeWait)); err != nil {
					return
				}
			}
		}
	}()

	go sess.writeLoop()

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (b *Broadcast) remove(s *Session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, sess := range b.sessions {
		if sess == s {
			b.sessions = append(b.sessions[:i], b.sessions[i+1:]...)
			return
		}
	}
}

// PushTick sends the normalized tick to all sessions' batch channels (non-blocking).
func (b *Broadcast) PushTick(row normalize.TickRow) {
	plain, err := json.Marshal(row)
	if err != nil {
		logrus.Warnf("[ts_proxy] marshal tick: %v", err)
		return
	}
	b.mu.Lock()
	sessions := append([]*Session(nil), b.sessions...)
	b.mu.Unlock()
	for _, sess := range sessions {
		select {
		case sess.tickCh <- json.RawMessage(plain):
		default:
		}
	}
}
