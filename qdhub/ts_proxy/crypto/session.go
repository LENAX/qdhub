package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

var (
	ErrInvalidKey       = errors.New("invalid key: must be 16, 24, or 32 bytes")
	ErrCiphertextTooShort = errors.New("ciphertext too short")
	ErrDecryptFailed    = errors.New("decryption failed")
)

// SessionCipher encrypts/decrypts with AES-GCM for scheme B session.
type SessionCipher struct {
	key []byte
}

// NewSessionCipher creates a cipher for the given AES key (16/24/32 bytes).
func NewSessionCipher(key []byte) (*SessionCipher, error) {
	if n := len(key); n != 16 && n != 24 && n != 32 {
		return nil, ErrInvalidKey
	}
	return &SessionCipher{key: key}, nil
}

// Encrypt appends nonce (12 bytes) + ciphertext. Suitable for Binary frame.
func (c *SessionCipher) Encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// GenerateKey returns a random key of the given size (e.g. 32 for AES-256).
func GenerateKey(size int) ([]byte, error) {
	if size <= 0 || size > 64 {
		return nil, ErrInvalidKey
	}
	b := make([]byte, size)
	_, err := rand.Read(b)
	return b, err
}

// Decrypt expects nonce (12 bytes) + ciphertext. Returns plaintext.
func (c *SessionCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrCiphertextTooShort
	}
	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plain, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, ErrDecryptFailed
	}
	return plain, nil
}
