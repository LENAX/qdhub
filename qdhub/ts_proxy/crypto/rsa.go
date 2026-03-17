package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
)

// LoadRSAPrivateKeyFromFile loads PEM-encoded RSA private key from path.
func LoadRSAPrivateKeyFromFile(path string) (*rsa.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("no PEM block in private key file")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		key2, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err2 != nil {
			return nil, err
		}
		k, ok := key2.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not RSA")
		}
		return k, nil
	}
	return key, nil
}

// LoadRSAPublicKeyFromFile loads PEM-encoded RSA public key from path.
func LoadRSAPublicKeyFromFile(path string) (*rsa.PublicKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("no PEM block in public key file")
	}
	pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		pub2, err2 := x509.ParsePKIXPublicKey(block.Bytes)
		if err2 != nil {
			return nil, err
		}
		k, ok := pub2.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("public key is not RSA")
		}
		return k, nil
	}
	return pub, nil
}

// DecryptAESKeyWithRSA decrypts RSA-OAEP encrypted AES key (e.g. 256-bit) with private key.
func DecryptAESKeyWithRSA(priv *rsa.PrivateKey, ciphertext []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
}

// EncryptAESKeyWithRSA encrypts AES key with public key for scheme B client (RSA-OAEP with SHA-256).
func EncryptAESKeyWithRSA(pub *rsa.PublicKey, plaintext []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
}
