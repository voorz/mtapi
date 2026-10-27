// Package cryptox implements the cryptographic primitives of the MT5
// WebSocket protocol: AES-256-CBC with a zero IV and PKCS#7 padding,
// plus the XOR chain cipher used by the protocol's wire encoding.
//
// The implementations mirror the reference Python module
// (MT5API/mt5client/crypto.py) byte for byte.
package cryptox

import (
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
)

// StaticKey protects the authentication handshake (command 0). The
// session key exchanged during that handshake protects everything else.
var StaticKey = []byte{
	0x02, 0xde, 0x02, 0xa1, 0xa6, 0x5c, 0xc7, 0x94, 0x68, 0x4f, 0xcb, 0xea, 0x1e, 0xcb, 0x0f, 0xd7,
	0x4a, 0xe6, 0x57, 0xe4, 0x36, 0x62, 0xc1, 0x1e, 0xee, 0x88, 0x5d, 0x2f, 0xd6, 0x4f, 0x49, 0x64,
}

// zeroIV is the fixed all-zero CBC initialisation vector of the protocol.
var zeroIV = make([]byte, aes.BlockSize)

// ErrInvalidPadding is returned when a decrypted payload does not carry
// valid PKCS#7 padding and cannot be trusted to be intact.
var ErrInvalidPadding = errors.New("cryptox: invalid PKCS#7 padding")

// AESEncrypt encrypts plaintext with AES-256-CBC (zero IV) after applying
// PKCS#7 padding. The key must be 32 bytes (the static key or a session
// key). A plaintext whose length is already a multiple of the block
// size gains a full padding block, exactly as in the reference.
func AESEncrypt(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("cryptox: %w", err)
	}
	padLen := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := make([]byte, len(plaintext)+padLen)
	copy(padded, plaintext)
	for i := len(plaintext); i < len(padded); i++ {
		padded[i] = byte(padLen)
	}
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, zeroIV).CryptBlocks(out, padded)
	return out, nil
}

// AESDecrypt decrypts data produced by AESEncrypt. If the trailing
// padding is not valid PKCS#7, the plaintext is returned together with
// ErrInvalidPadding; the reference implementation silently returns the
// raw plaintext in that case, and callers that mirror it may ignore the
// error — but it is reported here so corruption is never invisible.
func AESDecrypt(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("cryptox: %w", err)
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("cryptox: ciphertext length %d is not a multiple of the block size", len(ciphertext))
	}
	pt := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, zeroIV).CryptBlocks(pt, ciphertext)
	padLen := int(pt[len(pt)-1])
	if padLen < 1 || padLen > aes.BlockSize {
		return pt, ErrInvalidPadding
	}
	for _, b := range pt[len(pt)-padLen:] {
		if int(b) != padLen {
			return pt, ErrInvalidPadding
		}
	}
	return pt[:len(pt)-padLen], nil
}

// xorKey is the fixed 16-byte key of the protocol's XOR chain cipher.
var xorKey = []byte{65, 182, 127, 88, 56, 12, 240, 45, 123, 57, 8, 254, 33, 187, 65, 88}

// XOREncrypt applies the XOR chain cipher: each byte is XORed with the
// previous plaintext byte plus a key byte (the chain starts at 0).
func XOREncrypt(data []byte) []byte {
	out := make([]byte, len(data))
	prev := byte(0)
	for i, b := range data {
		out[i] = b ^ (prev + xorKey[i&0xF])
		prev = b
	}
	return out
}

// XORDecrypt reverses XOREncrypt. The chain runs over plaintext bytes,
// exactly as in the reference implementation.
func XORDecrypt(data []byte) []byte {
	out := make([]byte, len(data))
	prev := byte(0)
	for i, b := range data {
		out[i] = b ^ (prev + xorKey[i&0xF])
		prev = out[i]
	}
	return out
}

// XOREncryptBlock applies the block form of the XOR chain cipher used
// by servers.dat records: the chain runs over ciphertext bytes and
// restarts at prev=0 for every block (that is, every call).
func XOREncryptBlock(data []byte) []byte {
	out := make([]byte, len(data))
	prev := byte(0)
	for i, b := range data {
		out[i] = b ^ (prev + xorKey[i&0xF])
		prev = out[i]
	}
	return out
}

// XORDecryptBlock reverses XOREncryptBlock.
func XORDecryptBlock(data []byte) []byte {
	out := make([]byte, len(data))
	prev := byte(0)
	for i, b := range data {
		out[i] = b ^ (prev + xorKey[i&0xF])
		prev = b
	}
	return out
}
