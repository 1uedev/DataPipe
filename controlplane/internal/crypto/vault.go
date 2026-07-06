// Package crypto implements SEC-120's credential store encryption: values
// are envelope-encrypted (a random per-credential data-encryption-key, DEK,
// encrypts the value; the DEK itself is wrapped under a master
// key-encryption-key, KEK). Rotating the KEK only ever needs to re-wrap the
// small DEKs, never the secret values themselves — that's the whole point
// of the envelope. Credential values are write-only by construction: Vault
// only exposes Seal and Open, and callers (see controlplane/internal/api)
// never route Open's result back into an API response.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

const keyLen = 32 // AES-256

// Vault holds the KEKs a control plane instance can encrypt/decrypt with,
// keyed by version so old credentials remain readable after rotation.
type Vault struct {
	keys           map[int][]byte
	currentVersion int
}

// NewVault creates a Vault whose only (current) KEK is masterKey, which
// must be exactly 32 bytes (AES-256). Read it from the DATAPIPE_MASTER_KEY
// environment variable (base64-encoded) in production.
func NewVault(masterKey []byte) (*Vault, error) {
	if len(masterKey) != keyLen {
		return nil, fmt.Errorf("crypto: master key must be %d bytes, got %d", keyLen, len(masterKey))
	}
	key := make([]byte, keyLen)
	copy(key, masterKey)
	return &Vault{keys: map[int][]byte{1: key}, currentVersion: 1}, nil
}

// Sealed is the envelope-encrypted form of a secret, ready to persist
// verbatim in the credentials table.
type Sealed struct {
	KeyVersion      int
	WrappedDEK      string
	WrappedDEKNonce string
	Ciphertext      string
	Nonce           string
}

// Seal encrypts plaintext under a fresh random DEK, then wraps that DEK
// under the current KEK.
func (v *Vault) Seal(plaintext []byte) (Sealed, error) {
	dek := make([]byte, keyLen)
	if _, err := rand.Read(dek); err != nil {
		return Sealed{}, fmt.Errorf("crypto: generating DEK: %w", err)
	}
	defer zero(dek)

	ciphertext, nonce, err := aesGCMEncrypt(dek, plaintext)
	if err != nil {
		return Sealed{}, err
	}

	kek := v.keys[v.currentVersion]
	wrappedDEK, wrappedNonce, err := aesGCMEncrypt(kek, dek)
	if err != nil {
		return Sealed{}, err
	}

	return Sealed{
		KeyVersion:      v.currentVersion,
		WrappedDEK:      base64.StdEncoding.EncodeToString(wrappedDEK),
		WrappedDEKNonce: base64.StdEncoding.EncodeToString(wrappedNonce),
		Ciphertext:      base64.StdEncoding.EncodeToString(ciphertext),
		Nonce:           base64.StdEncoding.EncodeToString(nonce),
	}, nil
}

// Open reverses Seal: unwraps the DEK with the KEK version recorded on s,
// then decrypts the value.
func (v *Vault) Open(s Sealed) ([]byte, error) {
	kek, ok := v.keys[s.KeyVersion]
	if !ok {
		return nil, fmt.Errorf("crypto: unknown key version %d", s.KeyVersion)
	}

	wrappedDEK, err := base64.StdEncoding.DecodeString(s.WrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("crypto: decoding wrapped dek: %w", err)
	}
	wrappedNonce, err := base64.StdEncoding.DecodeString(s.WrappedDEKNonce)
	if err != nil {
		return nil, fmt.Errorf("crypto: decoding wrapped dek nonce: %w", err)
	}
	dek, err := aesGCMDecrypt(kek, wrappedNonce, wrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("crypto: unwrapping dek: %w", err)
	}
	defer zero(dek)

	ciphertext, err := base64.StdEncoding.DecodeString(s.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("crypto: decoding ciphertext: %w", err)
	}
	nonce, err := base64.StdEncoding.DecodeString(s.Nonce)
	if err != nil {
		return nil, fmt.Errorf("crypto: decoding nonce: %w", err)
	}
	return aesGCMDecrypt(dek, nonce, ciphertext)
}

func aesGCMEncrypt(key, plaintext []byte) (ciphertext, nonce []byte, err error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, nil, err
	}
	nonce = make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("crypto: generating nonce: %w", err)
	}
	return gcm.Seal(nil, nonce, plaintext, nil), nonce, nil
}

func aesGCMDecrypt(key, nonce, ciphertext []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("crypto: decrypt: %w", err)
	}
	return plaintext, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm: %w", err)
	}
	return gcm, nil
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
