package cryptox

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Golden vectors produced with OpenSSL (AES-256-CBC, zero IV, PKCS#7)
// under the protocol's static key.
func TestAESEncryptGolden(t *testing.T) {
	pt := append([]byte{0xaa, 0xbb, 0x00, 0x00}, []byte("hello-mt5")...)
	got, err := AESEncrypt(StaticKey, pt)
	if err != nil {
		t.Fatal(err)
	}
	want := mustHex(t, "675856fd3131052ff7520477084c9107")
	if !bytes.Equal(got, want) {
		t.Fatalf("encrypt = %x, want %x", got, want)
	}

	got, err = AESEncrypt(StaticKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	want = mustHex(t, "1614093a744799c9a5b7e30fe0082f3e")
	if !bytes.Equal(got, want) {
		t.Fatalf("encrypt empty = %x, want %x", got, want)
	}
}

func TestAESRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	for _, pt := range [][]byte{
		{}, {0x01}, []byte("0123456789abcdef"), []byte("a longer session payload 12345"),
	} {
		ct, err := AESEncrypt(key, pt)
		if err != nil {
			t.Fatal(err)
		}
		back, err := AESDecrypt(key, ct)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(back, pt) {
			t.Fatalf("round trip of %q gave %q", pt, back)
		}
	}
}

func TestAESDecryptBadPaddingStillReturnsPlaintext(t *testing.T) {
	key := make([]byte, 32)
	ct, err := AESEncrypt(key, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	ct[len(ct)-1] ^= 0xFF // corrupt the padding
	if _, err := AESDecrypt(key, ct); !errors.Is(err, ErrInvalidPadding) {
		t.Fatalf("err = %v, want ErrInvalidPadding", err)
	}
}

func TestXORChain(t *testing.T) {
	// Hand-derived from the algorithm: out[i] = in[i] ^ (prev + key[i&0xF]),
	// prev chains over plaintext, starting at 0.
	in := []byte{0x01, 0x02}
	want := []byte{0x01 ^ (0 + 65), 0x02 ^ (0x01 + 182)}
	got := XOREncrypt(in)
	if !bytes.Equal(got, want) {
		t.Fatalf("encrypt = %v, want %v", got, want)
	}
	if back := XORDecrypt(got); !bytes.Equal(back, in) {
		t.Fatalf("round trip gave %v", back)
	}
	data := []byte("the quick brown fox jumps over the lazy dog")
	if back := XORDecrypt(XOREncrypt(data)); !bytes.Equal(back, data) {
		t.Fatalf("long round trip gave %q", back)
	}
}
