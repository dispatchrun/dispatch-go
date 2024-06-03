package dispatch

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/offblocks/httpsig"
)

// Dispatch is a Dispatch endpoint.
type Dispatch struct {
	// EndpointUrl is the URL that this Dispatch endpoint
	// is accessible from.
	//
	// If omitted, the value of the DISPATCH_ENDPOINT_URL environment
	// variable is used.
	EndpointUrl string

	// VerificationKey is the ed25519 public key to use when verifying
	// Dispatch request signatures.
	//
	// If omitted, the value of the DISPATCH_VERIFICATION_KEY environment
	// variable is used.
	//
	// If a verification key is not provided, request signatures will
	// not be validated.
	VerificationKey string

	// Env are environment variables to parse configuration from.
	// If nil, environment variables are read from os.Environ().
	Env []string

	// Registry is the registry of functions to dispatch calls to.
	Registry

	// Client is the client to use when dispatching function calls.
	Client

	mu sync.Mutex
}

// Handler returns an HTTP handler for Dispatch, along with the path
// that the handler should be registered at.
func (d *Dispatch) Handler(opts ...connect.HandlerOption) (string, http.Handler, error) {
	interceptor, err := validate.NewInterceptor()
	if err != nil {
		return "", nil, err
	}
	opts = append(opts, connect.WithInterceptors(interceptor))

	path, handler := sdkv1connect.NewFunctionServiceHandler(&dispatchFunctionServiceHandler{d}, opts...)
	handler, err = d.validateSignatures(handler)
	if err != nil {
		return "", nil, err
	}
	return path, handler, nil
}

// The gRPC handler is unexported. This is so that the http.Handler can be
// wrapped in order to validate request signatures.
type dispatchFunctionServiceHandler struct {
	dispatch *Dispatch
}

func (d *dispatchFunctionServiceHandler) Run(ctx context.Context, req *connect.Request[sdkv1.RunRequest]) (*connect.Response[sdkv1.RunResponse], error) {
	res := d.dispatch.Registry.Run(ctx, req.Msg)
	return connect.NewResponse(res), nil
}

func (d *Dispatch) validateSignatures(next http.Handler) (http.Handler, error) {
	key, err := d.verificationKey()
	if err != nil {
		return nil, err
	}
	if key == nil {
		// Don't print this warning when running under the CLI.
		if endpoint := d.endpoint(); !strings.HasPrefix(endpoint, "bridge://") {
			slog.Warn("request signature validation is disabled")
		}
		return next, nil
	}

	verifier := httpsig.NewVerifier(
		httpsig.WithVerifyEd25519("default", key),
		httpsig.WithVerifyAll(true),
		httpsig.WithVerifyMaxAge(5*time.Minute),
		httpsig.WithVerifyTolerance(5*time.Second),
		// The httpsig library checks the strings below against marshaled
		// httpsfv items, hence the double quoting.
		httpsig.WithVerifyRequiredFields(`"@method"`, `"@path"`, `"@authority"`, `"content-type"`, `"content-digest"`),
	)

	digestor := httpsig.NewDigestor(httpsig.WithDigestAlgorithms(httpsig.DigestAlgorithmSha512))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Read the body into memory so that the Content-Digest header
		// can be verified.
		// TODO: put a limit on the read
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			slog.Warn("failed to read request body", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		if _, ok := r.Header[httpsig.ContentDigestHeader]; !ok {
			slog.Warn("missing content digest header")
			w.WriteHeader(http.StatusBadRequest)
			return
		} else if err := digestor.Verify(body, r.Header); err != nil {
			slog.Warn("invalid content digest header", "error", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		if err := verifier.Verify(httpsig.MessageFromRequest(r)); err != nil {
			slog.Warn("missing or invalid request signature", "error", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	}), nil
}

// ListenAndServe serves the Dispatch endpoint on the specified address.
func (d *Dispatch) ListenAndServe(addr string) error {
	path, handler, err := d.Handler()
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle(path, handler)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return server.ListenAndServe()
}

// Dispatch dispatches a batch of function calls.
func (d *Dispatch) Dispatch(ctx context.Context, calls []*sdkv1.Call) ([]DispatchID, error) {
	d.Client.Env = d.Env

	defaultEndpoint := d.endpoint()
	for _, c := range calls {
		if c.Endpoint == "" {
			c.Endpoint = defaultEndpoint
		}
	}
	return d.Client.Dispatch(ctx, calls)
}

func (d *Dispatch) endpoint() string {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.EndpointUrl == "" {
		d.EndpointUrl, _ = getenv(d.Env, "DISPATCH_ENDPOINT_URL")
	}
	return d.EndpointUrl
}

func (d *Dispatch) verificationKey() (ed25519.PublicKey, error) {
	encodedKey := d.VerificationKey
	if encodedKey == "" {
		encodedKey, _ = getenv(d.Env, "DISPATCH_VERIFICATION_KEY")
	}
	if encodedKey == "" {
		return nil, nil
	}

	// TODO: accept key in PEM format

	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid verification key")
	}
	return ed25519.PublicKey(key), nil
}
