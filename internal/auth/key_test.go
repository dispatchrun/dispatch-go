package auth_test

import (
	"crypto/ed25519"
	"slices"
	"strconv"
	"testing"

	"github.com/dispatchrun/dispatch-go/internal/auth"
)

func TestParsePublicKey(t *testing.T) {
	want := ed25519.PublicKey{0x26, 0xb4, 0xb, 0x8f, 0x93, 0xff, 0xf3, 0xd8, 0x97, 0x11, 0x2f, 0x7e, 0xbc, 0x58, 0x2b, 0x23, 0x2d, 0xbd, 0x72, 0x51, 0x7d, 0x8, 0x2f, 0xe8, 0x3c, 0xfb, 0x30, 0xdd, 0xce, 0x43, 0xd1, 0xbb}

	for i, encodedKey := range []string{
		`-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=
-----END PUBLIC KEY-----`,
		`-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=\n-----END PUBLIC KEY-----`,
		"JrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=",
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			key, err := auth.ParsePublicKey(encodedKey)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(key, want) {
				t.Errorf("unexpected public key: %#v", key)
			}
		})
	}

}
