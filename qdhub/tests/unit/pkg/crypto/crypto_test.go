package crypto_test

import (
	"encoding/base64"
	"testing"

	"qdhub/pkg/crypto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAESTokenEncryptor_EncryptDecrypt(t *testing.T) {
	// Generate a 32-byte key for AES-256
	key, err := crypto.GenerateKey(32)
	require.NoError(t, err)

	encryptor, err := crypto.NewAESTokenEncryptor(key)
	require.NoError(t, err)

	testCases := []struct {
		name      string
		plaintext string
	}{
		{"empty string", ""},
		{"simple text", "hello world"},
		{"api token", "abc123xyz789"},
		{"long token", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"},
		{"unicode text", "你好世界🌍"},
		{"special chars", "!@#$%^&*()_+-=[]{}|;':\",./<>?"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encrypt
			ciphertext, err := encryptor.Encrypt(tc.plaintext)
			require.NoError(t, err)
			assert.NotEqual(t, tc.plaintext, ciphertext)

			// Decrypt
			decrypted, err := encryptor.Decrypt(ciphertext)
			require.NoError(t, err)
			assert.Equal(t, tc.plaintext, decrypted)
		})
	}
}

func TestAESTokenEncryptor_DifferentKeySizes(t *testing.T) {
	keySizes := []int{16, 24, 32}
	plaintext := "test-api-token-12345"

	for _, size := range keySizes {
		t.Run("AES key size", func(t *testing.T) {
			key, err := crypto.GenerateKey(size)
			require.NoError(t, err)

			encryptor, err := crypto.NewAESTokenEncryptor(key)
			require.NoError(t, err)

			ciphertext, err := encryptor.Encrypt(plaintext)
			require.NoError(t, err)

			decrypted, err := encryptor.Decrypt(ciphertext)
			require.NoError(t, err)
			assert.Equal(t, plaintext, decrypted)
		})
	}
}

func TestAESTokenEncryptor_InvalidKeySize(t *testing.T) {
	invalidSizes := []int{0, 1, 15, 17, 23, 25, 31, 33, 64}

	for _, size := range invalidSizes {
		t.Run("invalid key size", func(t *testing.T) {
			key := make([]byte, size)
			_, err := crypto.NewAESTokenEncryptor(key)
			assert.Error(t, err)
			assert.Equal(t, crypto.ErrInvalidKey, err)
		})
	}
}

func TestAESTokenEncryptor_FromBase64(t *testing.T) {
	// Generate a key and encode it
	key, err := crypto.GenerateKey(32)
	require.NoError(t, err)
	keyBase64 := base64.StdEncoding.EncodeToString(key)

	encryptor, err := crypto.NewAESTokenEncryptorFromBase64(keyBase64)
	require.NoError(t, err)

	plaintext := "test-token"
	ciphertext, err := encryptor.Encrypt(plaintext)
	require.NoError(t, err)

	decrypted, err := encryptor.Decrypt(ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestAESTokenEncryptor_InvalidBase64Key(t *testing.T) {
	_, err := crypto.NewAESTokenEncryptorFromBase64("not-valid-base64!")
	assert.Error(t, err)
}

func TestAESTokenEncryptor_UniqueNonce(t *testing.T) {
	key, err := crypto.GenerateKey(32)
	require.NoError(t, err)

	encryptor, err := crypto.NewAESTokenEncryptor(key)
	require.NoError(t, err)

	plaintext := "same-text"

	// Encrypt the same text multiple times
	ciphertexts := make([]string, 10)
	for i := 0; i < 10; i++ {
		ct, err := encryptor.Encrypt(plaintext)
		require.NoError(t, err)
		ciphertexts[i] = ct
	}

	// All ciphertexts should be different (due to random nonce)
	for i := 0; i < len(ciphertexts); i++ {
		for j := i + 1; j < len(ciphertexts); j++ {
			assert.NotEqual(t, ciphertexts[i], ciphertexts[j],
				"Ciphertexts should be different for same plaintext")
		}
	}

	// But all should decrypt to the same plaintext
	for _, ct := range ciphertexts {
		decrypted, err := encryptor.Decrypt(ct)
		require.NoError(t, err)
		assert.Equal(t, plaintext, decrypted)
	}
}

func TestAESTokenEncryptor_WrongKey(t *testing.T) {
	key1, _ := crypto.GenerateKey(32)
	key2, _ := crypto.GenerateKey(32)

	encryptor1, _ := crypto.NewAESTokenEncryptor(key1)
	encryptor2, _ := crypto.NewAESTokenEncryptor(key2)

	plaintext := "secret-token"
	ciphertext, err := encryptor1.Encrypt(plaintext)
	require.NoError(t, err)

	// Try to decrypt with wrong key
	_, err = encryptor2.Decrypt(ciphertext)
	assert.Error(t, err)
	assert.Equal(t, crypto.ErrDecryptionFailed, err)
}

func TestAESTokenEncryptor_InvalidCiphertext(t *testing.T) {
	key, _ := crypto.GenerateKey(32)
	encryptor, _ := crypto.NewAESTokenEncryptor(key)

	testCases := []struct {
		name       string
		ciphertext string
		expectErr  error
	}{
		{"invalid base64", "not-valid-base64!", nil},
		{"too short", base64.StdEncoding.EncodeToString([]byte("short")), crypto.ErrCiphertextTooShort},
		{"corrupted", base64.StdEncoding.EncodeToString(make([]byte, 50)), crypto.ErrDecryptionFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := encryptor.Decrypt(tc.ciphertext)
			assert.Error(t, err)
		})
	}
}

func TestNoopTokenEncryptor(t *testing.T) {
	encryptor := crypto.NewNoopTokenEncryptor()

	plaintext := "test-token-12345"

	// Encrypt should return the same value
	ciphertext, err := encryptor.Encrypt(plaintext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, ciphertext)

	// Decrypt should return the same value
	decrypted, err := encryptor.Decrypt(ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestGenerateKey(t *testing.T) {
	validSizes := []int{16, 24, 32}
	for _, size := range validSizes {
		t.Run("valid size", func(t *testing.T) {
			key, err := crypto.GenerateKey(size)
			require.NoError(t, err)
			assert.Len(t, key, size)
		})
	}

	invalidSizes := []int{0, 1, 15, 64}
	for _, size := range invalidSizes {
		t.Run("invalid size", func(t *testing.T) {
			_, err := crypto.GenerateKey(size)
			assert.Error(t, err)
		})
	}
}

func TestGenerateKeyBase64(t *testing.T) {
	keyBase64, err := crypto.GenerateKeyBase64(32)
	require.NoError(t, err)

	// Should be valid base64
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	require.NoError(t, err)
	assert.Len(t, key, 32)
}
