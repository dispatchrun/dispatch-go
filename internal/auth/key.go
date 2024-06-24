//go:build !durable

package auth

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"strings"
)

// ParsePublicKey parses a ed25519 public key.
func ParsePublicKey(encodedKey string) (ed25519.PublicKey, error) {
	if strings.Contains(encodedKey, "BEGIN PUBLIC KEY") {
		return parsePemPublicKey(encodedKey)
	}
	return parseBase64PublicKey(encodedKey)
}

var (
	errInvalidPemKey    = errors.New("invalid PEM ed25519 public key")
	errInvalidBase64Key = errors.New("invalid base64 ed25519 public key")
)

func parsePemPublicKey(encodedKey string) (ed25519.PublicKey, error) {
	// Be forgiving when parsing PEM formatted keys, which may
	// have passed through environment variables.
	encodedKey = strings.ReplaceAll(encodedKey, "\\n", "\n")

	block, _ := pem.Decode([]byte(encodedKey))
	if block == nil {
		return nil, errInvalidPemKey
	}
	anyKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, errInvalidPemKey
	}
	key, ok := anyKey.(ed25519.PublicKey)
	if !ok {
		return nil, errInvalidPemKey
	}
	return key, nil
}

func parseBase64PublicKey(encodedKey string) (ed25519.PublicKey, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, errInvalidBase64Key
	}
	return ed25519.PublicKey(key), nil
}
