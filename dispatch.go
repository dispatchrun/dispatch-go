package dispatch

import (
	"context"
	"net/http"
	"sync"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
)

// Dispatch is a Dispatch endpoint.
type Dispatch struct {
	// EndpointUrl is the URL that this Dispatch endpoint
	// is accessible from.
	//
	// If omitted, the value of DISPATCH_ENDPOINT_URL is used.
	EndpointUrl string

	// Env are environment variables to parse configuration
	// from. If nil, environment variables are read from
	// os.Environ().
	Env []string

	// Registry is the registry of functions to dispatch calls to.
	Registry

	// Client is the client to use when dispatching function calls.
	Client

	mu sync.Mutex
}

var _ sdkv1connect.FunctionServiceHandler = (*Dispatch)(nil)

// Run runs a function.
//
// Run implements the FunctionServiceHandler interface.
func (d *Dispatch) Run(ctx context.Context, req *connect.Request[sdkv1.RunRequest]) (*connect.Response[sdkv1.RunResponse], error) {
	res := d.Registry.Run(ctx, req.Msg)
	return connect.NewResponse(res), nil
}

// Handler returns an HTTP handler for Dispatch, along with the path
// that the handler should be registered at.
func (d *Dispatch) Handler(opts ...connect.HandlerOption) (string, http.Handler) {
	interceptor, err := validate.NewInterceptor()
	if err != nil {
		panic(err)
	}
	opts = append(opts, connect.WithInterceptors(interceptor))
	return sdkv1connect.NewFunctionServiceHandler(d, opts...)
}

// ListenAndServe serves the Dispatch endpoint on the specified address.
func (d *Dispatch) ListenAndServe(addr string) error {
	handler := http.NewServeMux()
	handler.Handle(d.Handler())

	server := &http.Server{
		Addr:    addr,
		Handler: handler,
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
