package auth

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/offblocks/httpsig"
)

var digestor = httpsig.NewDigestor(httpsig.WithDigestAlgorithms(httpsig.DigestAlgorithmSha512))

// Signer signs HTTP requests.
type Signer struct {
	signer *httpsig.Signer
}

// NewSigner creates a Signer that signs HTTP requests using the specified
// signing key, in the same way that Dispatch would sign requests.
func NewSigner(signingKey ed25519.PrivateKey) *Signer {
	return &Signer{
		signer: httpsig.NewSigner(
			httpsig.WithSignName("dispatch"),
			httpsig.WithSignEd25519("default", signingKey),
			httpsig.WithSignFields("@method", "@path", "@authority", "content-type", "content-digest"),
		),
	}
}

// Sign signs a request.
func (s *Signer) Sign(req *http.Request) error {
	body, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}
	req.Body = io.NopCloser(bytes.NewReader(body))

	// Generate the Content-Digest header.
	digestHeaders, err := digestor.Digest(body)
	if err != nil {
		return fmt.Errorf("failed to generate content digest: %w", err)
	}
	for name, values := range digestHeaders {
		req.Header[name] = append(req.Header[name], values...)
	}

	// Sign the request.
	headers, err := s.signer.Sign(httpsig.MessageFromRequest(req))
	if err != nil {
		return fmt.Errorf("failed to sign request: %w", err)
	}
	req.Header = headers
	return nil
}

// Client wraps an HTTP client to automatically sign requests.
func (s *Signer) Client(client connect.HTTPClient) *SigningClient {
	return &SigningClient{client, s}
}

// SigningClient is an HTTP client that automatically signs requests.
type SigningClient struct {
	client connect.HTTPClient
	signer *Signer
}

// Do signs and sends an HTTP request, and returns the HTTP response.
func (c *SigningClient) Do(req *http.Request) (*http.Response, error) {
	if err := c.signer.Sign(req); err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}
	return c.client.Do(req)
}

// Verifier verifies that requests were signed by Dispatch.
type Verifier struct {
	verifier *httpsig.Verifier
}

// NewVerifier creates a Verifier that verifies that requests were
// signed by Dispatch using the private key associated with this
// public verification key.
func NewVerifier(verificationKey ed25519.PublicKey) *Verifier {
	verifier := httpsig.NewVerifier(
		httpsig.WithVerifyEd25519("default", verificationKey),
		httpsig.WithVerifyAll(true),
		httpsig.WithVerifyMaxAge(5*time.Minute),
		httpsig.WithVerifyTolerance(5*time.Second),
		httpsig.WithVerifyRequiredParams("created"),
		// The httpsig library checks the strings below against marshaled
		// httpsfv items, hence the double quoting.
		httpsig.WithVerifyRequiredFields(`"@method"`, `"@path"`, `"@authority"`, `"content-type"`, `"content-digest"`),
	)
	return &Verifier{verifier}
}

// Verify verifies that a request was signed by Dispatch.
func (v *Verifier) Verify(r *http.Request) error {
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	// Verify the Content-Digest header.
	if _, ok := r.Header[httpsig.ContentDigestHeader]; !ok {
		return fmt.Errorf("missing Content-Digest header")
	} else if err := digestor.Verify(body, r.Header); err != nil {
		return fmt.Errorf("invalid Content-Digest header: %w", err)
	}

	// Verify the signature.
	if err := v.verifier.Verify(httpsig.MessageFromRequest(r)); err != nil {
		return fmt.Errorf("missing or invalid signature: %w", err)
	}
	return nil
}

// Middleware wraps an HTTP handler in order to validate request signatures.
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := v.Verify(r); err != nil {
			slog.Warn("request was not signed correctly", "error", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
