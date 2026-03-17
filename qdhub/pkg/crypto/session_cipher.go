package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)

// SessionCipher decrypts AES-GCM frames (nonce + ciphertext) for scheme B client session.
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

// Decrypt expects ciphertext as nonce (12 bytes) + ciphertext. Returns plaintext.
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
		return nil, ErrDecryptionFailed
	}
	return plain, nil
}

// Encrypt produces nonce (12 bytes) + ciphertext for scheme B server session (e.g. mock ts_proxy).
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
