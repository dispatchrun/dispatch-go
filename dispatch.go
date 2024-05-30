package dispatch

import (
	"context"
	"net/http"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
)

// Dispatch is a Dispatch endpoint.
type Dispatch struct {
	// Endpoint is the URL that this Dispatch endpoint
	// is accessible from.
	//
	// If omitted
	Endpoint string

	// Env are environment variables to parse configuration
	// from. If nil, environment variables are read from
	// os.Environ().
	Env []string

	// Registry is the registry of functions to dispatch calls to.
	Registry
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
