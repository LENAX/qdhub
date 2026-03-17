package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
)

func TestEncryptDecryptAESKeyWithRSA(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pub := &priv.PublicKey
	aesKey := make([]byte, 32)
	if _, err := rand.Read(aesKey); err != nil {
		t.Fatal(err)
	}
	ciphertext, err := EncryptAESKeyWithRSA(pub, aesKey)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := DecryptAESKeyWithRSA(priv, ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) != len(aesKey) {
		t.Errorf("len(plain)=%d want %d", len(plain), len(aesKey))
	}
	for i := range aesKey {
		if plain[i] != aesKey[i] {
			t.Errorf("plain[%d] mismatch", i)
			break
		}
	}
}
