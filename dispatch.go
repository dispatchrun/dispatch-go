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
	"sync"
	_ "unsafe"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go/dispatchclient"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/dispatchrun/dispatch-go/internal/auth"
	"github.com/dispatchrun/dispatch-go/internal/env"
)

// Dispatch is a Dispatch endpoint.
type Dispatch struct {
	endpointUrl     string
	verificationKey string
	serveAddr       string
	env             []string
	opts            []Option

	client    *dispatchclient.Client
	clientErr error

	path    string
	handler http.Handler

	functions dispatchproto.FunctionMap
	mu        sync.Mutex
}

// New creates a Dispatch endpoint.
func New(opts ...Option) (*Dispatch, error) {
	d := &Dispatch{
		env:       os.Environ(),
		opts:      opts,
		functions: map[string]dispatchproto.Function{},
	}
	for _, opt := range opts {
		opt(d)
	}

	// Prepare the endpoint URL.
	var endpointUrlFromEnv bool
	if d.endpointUrl == "" {
		d.endpointUrl = env.Get(d.env, "DISPATCH_ENDPOINT_URL")
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
		d.serveAddr = env.Get(d.env, "DISPATCH_ENDPOINT_ADDR")
		if d.serveAddr == "" {
			d.serveAddr = "127.0.0.1:8000"
		}
	}

	// Prepare the verification key.
	var verificationKeyFromEnv bool
	if d.verificationKey == "" {
		d.verificationKey = env.Get(d.env, "DISPATCH_VERIFICATION_KEY")
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
		d.client, d.clientErr = dispatchclient.New(dispatchclient.Env(d.env...))
	}

	return d, nil
}

// Option configures a Dispatch endpoint.
type Option func(d *Dispatch)

// EndpointUrl sets the URL of the Dispatch endpoint.
//
// It defaults to the value of the DISPATCH_ENDPOINT_URL environment
// variable.
func EndpointUrl(endpointUrl string) Option {
	return func(d *Dispatch) { d.endpointUrl = endpointUrl }
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
func VerificationKey(verificationKey string) Option {
	return func(d *Dispatch) { d.verificationKey = verificationKey }
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
func ServeAddress(addr string) Option {
	return func(d *Dispatch) { d.serveAddr = addr }
}

// Env sets the environment variables that a Dispatch endpoint
// parses its default configuration from.
//
// It defaults to os.Environ().
func Env(env ...string) Option {
	return func(d *Dispatch) { d.env = env }
}

// Client sets the client to use when dispatching calls
// from functions registered on the endpoint.
//
// By default the Dispatch endpoint will attempt to construct
// a dispatchclient.Client instance using the DISPATCH_API_KEY
// and optional DISPATCH_API_URL environment variables. If more
// control is required over client configuration, the custom
// client instance can be registered here and used instead.
func Client(client *dispatchclient.Client) Option {
	return func(d *Dispatch) { d.client = client }
}

// Register registers a function.
func (d *Dispatch) Register(fn AnyFunction) {
	d.RegisterPrimitive(fn.Name(), fn.Primitive())

	// Bind the function to this endpoint, so that the function's
	// Dispatch method can be used to dispatch calls.
	fn.register(d)
}

// RegisterPrimitive registers a primitive function.
func (d *Dispatch) RegisterPrimitive(name string, fn dispatchproto.Function) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.functions[name] = fn
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
func (d *Dispatch) Client() (*dispatchclient.Client, error) {
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
	res := d.dispatch.functions.Run(ctx, newProtoRequest(req.Msg))
	return connect.NewResponse(responseProto(res)), nil
}

//go:linkname newProtoRequest github.com/dispatchrun/dispatch-go/dispatchproto.newProtoRequest
func newProtoRequest(r *sdkv1.RunRequest) dispatchproto.Request

//go:linkname responseProto github.com/dispatchrun/dispatch-go/dispatchproto.responseProto
func responseProto(r dispatchproto.Response) *sdkv1.RunResponse
