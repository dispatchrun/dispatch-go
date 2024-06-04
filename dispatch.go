package dispatch

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"net/http"
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

	// Client is the client to use when dispatching function calls.
	Client

	functions map[string]NamedFunction
	mu        sync.Mutex
}

// Register registers a function.
func (d *Dispatch) Register(fn NamedFunction) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.functions == nil {
		d.functions = map[string]NamedFunction{}
	}
	d.functions[fn.Name()] = fn
}

func (d *Dispatch) lookup(name string) NamedFunction {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.functions[name]
}

// Handler returns an HTTP handler for Dispatch, along with the path
// that the handler should be registered at.
func (d *Dispatch) Handler(opts ...connect.HandlerOption) (string, http.Handler, error) {
	validatingInterceptor, err := validate.NewInterceptor()
	if err != nil {
		return "", nil, err
	}
	path, handler := sdkv1connect.NewFunctionServiceHandler(&dispatchFunctionServiceHandler{d},
		connect.WithInterceptors(validatingInterceptor))

	// Setup request signature verification.
	verificationKey, err := d.verificationKey()
	if err != nil {
		return "", nil, err
	} else if verificationKey == nil {
		if endpoint := d.endpoint(); !strings.HasPrefix(endpoint, "bridge://") {
			// Don't print this warning when running under the CLI.
			slog.Warn("request signature validation is disabled")
		}
		return path, handler, nil
	}
	verifier := auth.NewVerifier(verificationKey)
	handler = verifier.Middleware(handler)

	return path, handler, nil
}

// The gRPC handler is unexported so that the http.Handler can
// be wrapped in order to validate request signatures.
type dispatchFunctionServiceHandler struct {
	dispatch *Dispatch
}

func (d *dispatchFunctionServiceHandler) Run(ctx context.Context, req *connect.Request[sdkv1.RunRequest]) (*connect.Response[sdkv1.RunResponse], error) {
	var res *sdkv1.RunResponse
	fn := d.dispatch.lookup(req.Msg.Function)
	if fn == nil {
		res = ErrorResponse(NotFoundStatus, fmt.Errorf("function %q not found", req.Msg.Function))
	} else {
		res = fn.Run(ctx, req.Msg)
	}
	return connect.NewResponse(res), nil
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
	return auth.ParseKey(encodedKey)
}
