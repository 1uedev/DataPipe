package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func testMasterKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, keyLen)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generating test master key: %v", err)
	}
	return key
}

func TestSEC120_SealOpenRoundTrip(t *testing.T) {
	v, err := NewVault(testMasterKey(t))
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}

	plaintext := []byte(`{"username":"admin","password":"hunter2"}`)
	sealed, err := v.Seal(plaintext)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	if bytes.Contains([]byte(sealed.Ciphertext), plaintext) {
		t.Fatal("ciphertext contains the plaintext verbatim")
	}

	got, err := v.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("Open() = %q, want %q", got, plaintext)
	}
}

func TestSEC120_RejectsWrongSizeMasterKey(t *testing.T) {
	if _, err := NewVault([]byte("too-short")); err == nil {
		t.Error("expected NewVault to reject a non-32-byte key")
	}
}

func TestSEC120_EachSealUsesAFreshDEK(t *testing.T) {
	v, err := NewVault(testMasterKey(t))
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}

	a, err := v.Seal([]byte("same-plaintext"))
	if err != nil {
		t.Fatalf("Seal a: %v", err)
	}
	b, err := v.Seal([]byte("same-plaintext"))
	if err != nil {
		t.Fatalf("Seal b: %v", err)
	}

	if a.WrappedDEK == b.WrappedDEK {
		t.Error("two Seal calls produced the same wrapped DEK — DEKs must be per-credential random")
	}
	if a.Ciphertext == b.Ciphertext {
		t.Error("two Seal calls of the same plaintext produced identical ciphertext")
	}
}

func TestSEC120_OpenFailsWithWrongKey(t *testing.T) {
	v1, err := NewVault(testMasterKey(t))
	if err != nil {
		t.Fatalf("NewVault v1: %v", err)
	}
	v2, err := NewVault(testMasterKey(t))
	if err != nil {
		t.Fatalf("NewVault v2: %v", err)
	}

	sealed, err := v1.Seal([]byte("secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := v2.Open(sealed); err == nil {
		t.Error("Open with a different vault's key should fail")
	}
}

func TestSEC120_OpenFailsOnTamperedCiphertext(t *testing.T) {
	v, err := NewVault(testMasterKey(t))
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	sealed, err := v.Seal([]byte("secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	tampered := sealed
	tampered.Ciphertext = sealed.Ciphertext[:len(sealed.Ciphertext)-4] + "abcd"
	if _, err := v.Open(tampered); err == nil {
		t.Error("Open should reject tampered ciphertext (GCM authentication)")
	}
}

func TestSEC120_UnknownKeyVersionRejected(t *testing.T) {
	v, err := NewVault(testMasterKey(t))
	if err != nil {
		t.Fatalf("NewVault: %v", err)
	}
	sealed, err := v.Seal([]byte("secret"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	sealed.KeyVersion = 99
	if _, err := v.Open(sealed); err == nil {
		t.Error("Open with an unknown key version should fail")
	}
}
