// Package crypto provides encryption and decryption utilities for sensitive data.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
)

var (
	// ErrInvalidKey is returned when the encryption key is invalid.
	ErrInvalidKey = errors.New("invalid encryption key: must be 16, 24, or 32 bytes")
	// ErrCiphertextTooShort is returned when the ciphertext is too short.
	ErrCiphertextTooShort = errors.New("ciphertext too short")
	// ErrDecryptionFailed is returned when decryption fails.
	ErrDecryptionFailed = errors.New("decryption failed")
)

// TokenEncryptor provides encryption and decryption for API tokens.
type TokenEncryptor interface {
	// Encrypt encrypts plaintext and returns base64-encoded ciphertext.
	Encrypt(plaintext string) (string, error)
	// Decrypt decrypts base64-encoded ciphertext and returns plaintext.
	Decrypt(ciphertext string) (string, error)
}

// AESTokenEncryptor implements TokenEncryptor using AES-GCM.
type AESTokenEncryptor struct {
	key []byte
}

// NewAESTokenEncryptor creates a new AES-GCM encryptor.
// The key must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256.
func NewAESTokenEncryptor(key []byte) (*AESTokenEncryptor, error) {
	keyLen := len(key)
	if keyLen != 16 && keyLen != 24 && keyLen != 32 {
		return nil, ErrInvalidKey
	}
	return &AESTokenEncryptor{key: key}, nil
}

// NewAESTokenEncryptorFromBase64 creates a new AES-GCM encryptor from a base64-encoded key.
func NewAESTokenEncryptorFromBase64(keyBase64 string) (*AESTokenEncryptor, error) {
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return nil, errors.New("failed to decode base64 key: " + err.Error())
	}
	return NewAESTokenEncryptor(key)
}

// Encrypt encrypts plaintext using AES-GCM and returns base64-encoded ciphertext.
func (e *AESTokenEncryptor) Encrypt(plaintext string) (string, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts base64-encoded ciphertext using AES-GCM and returns plaintext.
func (e *AESTokenEncryptor) Decrypt(ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", errors.New("failed to decode ciphertext: " + err.Error())
	}

	block, err := aes.NewCipher(e.key)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", ErrCiphertextTooShort
	}

	nonce, ciphertextBytes := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		return "", ErrDecryptionFailed
	}

	return string(plaintext), nil
}

// NoopTokenEncryptor is a no-op encryptor for testing or when encryption is disabled.
type NoopTokenEncryptor struct{}

// NewNoopTokenEncryptor creates a new no-op encryptor.
func NewNoopTokenEncryptor() *NoopTokenEncryptor {
	return &NoopTokenEncryptor{}
}

// Encrypt returns the plaintext as-is (no encryption).
func (e *NoopTokenEncryptor) Encrypt(plaintext string) (string, error) {
	return plaintext, nil
}

// Decrypt returns the ciphertext as-is (no decryption).
func (e *NoopTokenEncryptor) Decrypt(ciphertext string) (string, error) {
	return ciphertext, nil
}

// GenerateKey generates a random encryption key of the specified size.
// Size must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256.
func GenerateKey(size int) ([]byte, error) {
	if size != 16 && size != 24 && size != 32 {
		return nil, ErrInvalidKey
	}
	key := make([]byte, size)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

// GenerateKeyBase64 generates a random encryption key and returns it as base64.
func GenerateKeyBase64(size int) (string, error) {
	key, err := GenerateKey(size)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(key), nil
}
