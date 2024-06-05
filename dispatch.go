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

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go/internal/auth"
)

// Dispatch is a Dispatch endpoint.
type Dispatch struct {
	endpointUrl     string
	verificationKey string
	env             []string

	client     *Client
	clientOpts []ClientOption
	clientErr  error

	path    string
	handler http.Handler

	functions map[string]Function
	mu        sync.Mutex
}

// New creates a Dispatch endpoint.
func New(opts ...DispatchOption) (*Dispatch, error) {
	d := &Dispatch{
		env:       os.Environ(),
		functions: map[string]Function{},
	}
	for _, opt := range opts {
		opt(d)
	}

	// Prepare the endpoint URL.
	var endpointUrlFromEnv string
	if d.endpointUrl == "" {
		d.endpointUrl = getenv(d.env, "DISPATCH_ENDPOINT_URL")
		endpointUrlFromEnv = "DISPATCH_ENDPOINT_URL"
	}
	if d.endpointUrl == "" {
		if endpointAddr := getenv(d.env, "DISPATCH_ENDPOINT_ADDR"); endpointAddr != "" {
			d.endpointUrl = fmt.Sprintf("http://%s", endpointAddr)
			endpointUrlFromEnv = "DISPATCH_ENDPOINT_ADDR"
		}
	}
	if d.endpointUrl == "" {
		return nil, fmt.Errorf("Dispatch endpoint URL has not been set. Use WithEndpointUrl(..), or set the DISPATCH_ENDPOINT_URL environment variable")
	}
	_, err := url.Parse(d.endpointUrl)
	if err != nil {
		if endpointUrlFromEnv != "" {
			return nil, fmt.Errorf("invalid %s: %v", endpointUrlFromEnv, d.endpointUrl)
		}
		return nil, fmt.Errorf("invalid endpoint URL provided via WithEndpointUrl: %v", d.endpointUrl)
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
			return nil, fmt.Errorf("invalid verification key provided via WithVerificationKey: %v", d.verificationKey)
		}
	}

	// Setup the gRPC handler.
	validator, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}
	grpcHandler := &dispatchFunctionServiceHandler{d}
	d.path, d.handler = sdkv1connect.NewFunctionServiceHandler(grpcHandler, connect.WithInterceptors(validator))

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
		var err error
		d.client, err = NewClient(append(d.clientOpts, WithClientEnv(d.env...))...)
		if err != nil {
			slog.Debug("failed to setup client for the Dispatch endpoint", "error", err)
			d.clientErr = err
		}
	}

	return d, nil
}

// DispatchOption configures a Dispatch endpoint.
type DispatchOption func(d *Dispatch)

// WithEndpointUrl sets the URL of the Dispatch endpoint.
//
// It defaults to the value of the DISPATCH_ENDPOINT_URL environment
// variable.
func WithEndpointUrl(endpointUrl string) DispatchOption {
	return func(d *Dispatch) { d.endpointUrl = endpointUrl }
}

// WithVerificationKey sets the verification key to use when verifying
// Dispatch request signatures.
//
// The key should be a PEM or base64-encoded ed25519 public key.
//
// It defaults to the value of the DISPATCH_VERIFICATION_KEY environment
// variable value.
//
// If a verification key is not provided, request signatures will
// not be validated.
func WithVerificationKey(verificationKey string) DispatchOption {
	return func(d *Dispatch) { d.verificationKey = verificationKey }
}

// WithEnv sets the environment variables that a Dispatch endpoint
// parses its default configuration from.
//
// It defaults to os.Environ().
func WithEnv(env ...string) DispatchOption {
	return func(d *Dispatch) { d.env = env }
}

// WithClient binds a Client to a Dispatch endpoint.
//
// Binding a Client allows functions calls to be directly dispatched from
// functions registered with the endpoint, via function.Dispatch(...).
//
// The Dispatch endpoint can alternatively be configured with a set of
// client options, via WithClientOptions.
func WithClient(client *Client) DispatchOption {
	return func(d *Dispatch) { d.client = client }
}

// WithClientOptions sets options for the Client bound to the Dispatch
// endpoint.
//
// Binding a Client allows functions calls to be directly dispatched from
// functions registered with the endpoint, via function.Dispatch(...).
func WithClientOptions(opts ...ClientOption) DispatchOption {
	return func(d *Dispatch) { d.clientOpts = opts }
}

// Register registers a function.
func (d *Dispatch) Register(fn Function) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.functions[fn.Name()] = fn

	// Bind the function to this endpoint, so that the function's
	// NewCall and Dispatch methods can be used to build and
	// dispatch calls.
	fn.bind(d)
}

func (d *Dispatch) lookupFunction(name string) Function {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.functions[name]
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

// The gRPC handler is unexported so that the http.Handler can
// be wrapped in order to validate request signatures.
type dispatchFunctionServiceHandler struct {
	dispatch *Dispatch
}

func (d *dispatchFunctionServiceHandler) Run(ctx context.Context, req *connect.Request[sdkv1.RunRequest]) (*connect.Response[sdkv1.RunResponse], error) {
	var res *sdkv1.RunResponse
	fn := d.dispatch.lookupFunction(req.Msg.Function)
	if fn == nil {
		res = ErrorResponse(fmt.Errorf("%w: function %q not found", ErrNotFound, req.Msg.Function))
	} else {
		res = fn.Run(ctx, req.Msg)
	}
	return connect.NewResponse(res), nil
}

// ListenAndServe serves the Dispatch endpoint on the specified address.
//
// If the address is omitted, the endpoint is served on the address found
// in the DISPATCH_ENDPOINT_ADDR environment variable. If this is unset,
// the endpoint is served at 127.0.0.1:8000.
func (d *Dispatch) ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.Handle(d.Handler())

	if addr == "" {
		addr = getenv(d.env, "DISPATCH_ENDPOINT_ADDR")
	}
	if addr == "" {
		addr = "127.0.0.1:8000"
	}
	slog.Info("serving Dispatching endpoint", "addr", addr)

	server := &http.Server{Addr: addr, Handler: mux}
	return server.ListenAndServe()
}
