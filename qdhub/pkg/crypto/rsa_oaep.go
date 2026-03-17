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

// LoadRSAPublicKeyFromFile loads a PEM-encoded RSA public key from path.
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

// EncryptAESKeyWithRSA encrypts the given plaintext (e.g. AES key) with RSA-OAEP (SHA-256) for scheme B client.
func EncryptAESKeyWithRSA(pub *rsa.PublicKey, plaintext []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
}
