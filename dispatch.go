//go:build !durable

package dispatch

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	_ "unsafe"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/dispatchrun/dispatch-go/internal/auth"
)

// Dispatch is a Dispatch endpoint.
type Dispatch struct {
	endpointUrl     string
	verificationKey string
	serveAddr       string
	env             []string
	opts            []DispatchOption

	client    *Client
	clientErr error

	path    string
	handler http.Handler

	registry FunctionRegistry
}

// New creates a Dispatch endpoint.
func New(opts ...DispatchOption) (*Dispatch, error) {
	d := &Dispatch{
		env:  os.Environ(),
		opts: opts,
	}
	for _, opt := range opts {
		opt.configureDispatch(d)
	}

	// Prepare the endpoint URL.
	var endpointUrlFromEnv bool
	if d.endpointUrl == "" {
		d.endpointUrl = getenv(d.env, "DISPATCH_ENDPOINT_URL")
		endpointUrlFromEnv = true
	}
	if d.endpointUrl == "" {
		return nil, fmt.Errorf("Dispatch endpoint URL has not been set. Use EndpointUrl(..), or set the DISPATCH_ENDPOINT_URL environment variable")
	}
	_, err := url.Parse(d.endpointUrl)
	if err != nil {
		if endpointUrlFromEnv {
			return nil, fmt.Errorf("invalid DISPATCH_ENDPOINT_URL: %v", d.endpointUrl)
		}
		return nil, fmt.Errorf("invalid endpoint URL provided via EndpointUrl(..): %v", d.endpointUrl)
	}

	// Prepare the address to serve on.
	if d.serveAddr == "" {
		d.serveAddr = getenv(d.env, "DISPATCH_ENDPOINT_ADDR")
		if d.serveAddr == "" {
			d.serveAddr = "127.0.0.1:8000"
		}
	}

	// Prepare the verification key.
	var verificationKeyFromEnv bool
	if d.verificationKey == "" {
		d.verificationKey = getenv(d.env, "DISPATCH_VERIFICATION_KEY")
		verificationKeyFromEnv = true
	}
	var verificationKey ed25519.PublicKey
	if d.verificationKey != "" {
		var err error
		verificationKey, err = auth.ParsePublicKey(d.verificationKey)
		if err != nil {
			if verificationKeyFromEnv {
				return nil, fmt.Errorf("invalid DISPATCH_VERIFICATION_KEY: %v", d.verificationKey)
			}
			return nil, fmt.Errorf("invalid verification key provided via VerificationKey(..): %v", d.verificationKey)
		}
	}

	// Setup the gRPC handler.
	validator, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}
	d.path, d.handler = sdkv1connect.NewFunctionServiceHandler(dispatchHandler{d}, connect.WithInterceptors(validator))

	// Setup request signature validation.
	if verificationKey == nil {
		if !strings.HasPrefix(d.endpointUrl, "bridge://") {
			// Don't print this warning when running under the CLI.
			slog.Warn("Dispatch request signature validation is disabled")
		}
	} else {
		verifier := auth.NewVerifier(verificationKey)
		d.handler = verifier.Middleware(d.handler)
	}

	// Optionally attach a client.
	if d.client == nil {
		d.client, d.clientErr = NewClient(Env(d.env...))
	}

	return d, nil
}

// DispatchOption configures a Dispatch endpoint.
type DispatchOption interface {
	configureDispatch(d *Dispatch)
}

type dispatchOptionFunc func(d *Dispatch)

func (fn dispatchOptionFunc) configureDispatch(d *Dispatch) {
	fn(d)
}

// EndpointUrl sets the URL of the Dispatch endpoint.
//
// It defaults to the value of the DISPATCH_ENDPOINT_URL environment
// variable.
func EndpointUrl(endpointUrl string) DispatchOption {
	return dispatchOptionFunc(func(d *Dispatch) { d.endpointUrl = endpointUrl })
}

// VerificationKey sets the verification key to use when verifying
// Dispatch request signatures.
//
// The key should be a PEM or base64-encoded ed25519 public key.
//
// It defaults to the value of the DISPATCH_VERIFICATION_KEY environment
// variable value.
//
// If a verification key is not provided, request signatures will
// not be validated.
func VerificationKey(verificationKey string) DispatchOption {
	return dispatchOptionFunc(func(d *Dispatch) { d.verificationKey = verificationKey })
}

// ServeAddress sets the address that the Dispatch endpoint
// is served on (see Dispatch.Serve).
//
// Note that this is not the same as the endpoint URL, which is the
// URL that this Dispatch endpoint is publicly accessible from.
//
// It defaults to the value of the DISPATCH_ENDPOINT_ADDR environment
// variable, which is automatically set by the Dispatch CLI. If this
// is unset, it defaults to 127.0.0.1:8000.
func ServeAddress(addr string) DispatchOption {
	return dispatchOptionFunc(func(d *Dispatch) { d.serveAddr = addr })
}

// Register registers a function.
func (d *Dispatch) Register(fn AnyFunction) {
	d.registry.Register(fn)

	// Bind the function to this endpoint, so that the function's
	// NewCall and Dispatch methods can be used to build and
	// dispatch calls.
	fn.bind(d)
}

// URL is the URL of the Dispatch endpoint.
func (d *Dispatch) URL() string {
	return d.endpointUrl
}

// Handler returns an HTTP handler for Dispatch, along with the path
// that the handler should be registered at.
func (d *Dispatch) Handler() (string, http.Handler) {
	return d.path, d.handler
}

// Client returns the Client attached to this endpoint.
func (d *Dispatch) Client() (*Client, error) {
	return d.client, d.clientErr
}

// Serve serves the Dispatch endpoint.
func (d *Dispatch) Serve() error {
	mux := http.NewServeMux()
	mux.Handle(d.Handler())

	slog.Info("serving Dispatch endpoint", "addr", d.serveAddr)

	server := &http.Server{Addr: d.serveAddr, Handler: mux}
	return server.ListenAndServe()
}

// The gRPC handler is deliberately unexported. This forces
// the user to access it through Dispatch.Handler, and get
// a handler that has signature verification middleware attached.
type dispatchHandler struct{ dispatch *Dispatch }

func (d dispatchHandler) Run(ctx context.Context, req *connect.Request[sdkv1.RunRequest]) (*connect.Response[sdkv1.RunResponse], error) {
	res := d.dispatch.registry.Run(ctx, newProtoRequest(req.Msg))
	return connect.NewResponse(responseProto(res)), nil
}

//go:linkname newProtoRequest github.com/dispatchrun/dispatch-go/dispatchproto.newProtoRequest
func newProtoRequest(r *sdkv1.RunRequest) dispatchproto.Request

//go:linkname responseProto github.com/dispatchrun/dispatch-go/dispatchproto.responseProto
func responseProto(r dispatchproto.Response) *sdkv1.RunResponse
