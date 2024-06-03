package auth

import (
	"bytes"
	"crypto/ed25519"
	"net/http"
	"testing"
	"time"

	"github.com/offblocks/httpsig"
)

func TestVerify(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	altPublicKey, altPrivateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name            string
		signingKey      ed25519.PrivateKey
		verificationKey ed25519.PublicKey
		created         time.Time
		signFields      []string
		omitDigest      bool
		omitCreated     bool
		error           bool
	}{
		{
			name:            "ok",
			signingKey:      privateKey,
			verificationKey: publicKey,
		},
		{
			name:            "missing Content-Digest header",
			signingKey:      privateKey,
			verificationKey: publicKey,
			omitDigest:      true, // don't include Content-Digest header
			error:           true,
		},
		{
			name:            "missing signature",
			signingKey:      nil,
			verificationKey: publicKey,
			error:           true,
		},
		{
			name:            "key mismatch (1)",
			signingKey:      altPrivateKey,
			verificationKey: publicKey,
			error:           true,
		},
		{
			name:            "key mismatch (2)",
			signingKey:      privateKey,
			verificationKey: altPublicKey,
			error:           true,
		},
		{
			name:            "missing signature fields",
			signingKey:      privateKey,
			verificationKey: publicKey,
			// missing the required content-digest field
			signFields: []string{"@method", "@path", "@authority", "content-type"},
			error:      true,
		},
		{
			name:            "missing 'created' signature param",
			signingKey:      privateKey,
			verificationKey: publicKey,
			omitCreated:     true,
			error:           true,
		},
		{
			name:            "created in the future (below tolerance)",
			signingKey:      privateKey,
			verificationKey: publicKey,
			created:         time.Now().Add(1 * time.Second),
		},
		{
			name:            "created in the future (above tolerance)",
			signingKey:      privateKey,
			verificationKey: publicKey,
			created:         time.Now().Add(1 * time.Minute),
			error:           true,
		},
		{
			name:            "created in the past (within max_age)",
			signingKey:      privateKey,
			verificationKey: publicKey,
			created:         time.Now().Add(-1 * time.Minute),
		},
		{
			name:            "created in the past (outside of max_age)",
			signingKey:      privateKey,
			verificationKey: publicKey,
			created:         time.Now().Add(-10 * time.Minute),
			error:           true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte("{}")
			req, err := http.NewRequest("POST", "http://example.com", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Add("Content-Type", "application/json")

			digestor := httpsig.NewDigestor(httpsig.WithDigestAlgorithms(httpsig.DigestAlgorithmSha512))
			digest, err := digestor.Digest(body)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Add(httpsig.ContentDigestHeader, digest[httpsig.ContentDigestHeader][0])

			signFields := test.signFields
			if signFields == nil {
				signFields = []string{"@method", "@path", "@authority", "content-type", "content-digest"}
			}

			if test.signingKey != nil {
				var signParams *httpsig.SignatureParameters
				if test.omitCreated {
					signParams = &httpsig.SignatureParameters{Created: nil}
				} else if !test.created.IsZero() {
					signParams = &httpsig.SignatureParameters{Created: &test.created}
				}
				signer := httpsig.NewSigner(
					httpsig.WithSignName("dispatch"),
					httpsig.WithSignEd25519("default", test.signingKey),
					httpsig.WithSignFields(signFields...),
					httpsig.WithSignParamValues(signParams),
				)
				req.Header, err = signer.Sign(httpsig.MessageFromRequest(req))
				if err != nil {
					t.Fatal(err)
				}
			}

			if test.omitDigest {
				req.Header.Del(httpsig.ContentDigestHeader)
			}

			verifier := NewVerifier(test.verificationKey)
			if err := verifier.Verify(req); !test.error && err != nil {
				t.Fatal(err)
			} else if test.error && err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
