//go:build !durable

package dispatchtest

import (
	"crypto/ed25519"
	"encoding/base64"
)

// KeyPair generates a random ed25519 key pair.
//
// The public and private key are base64 encoded, so that
// they can be passed directly to the various Dispatch components.
func KeyPair() (signingKey, verificationKey string) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
	verificationKey = base64.StdEncoding.EncodeToString(publicKey[:])
	signingKey = base64.StdEncoding.EncodeToString(privateKey[:])
	return
}
