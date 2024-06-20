package dispatchserver

import (
	"context"
	"net/http"
	_ "unsafe"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Handler handles requests to a Dispatch API server.
type Handler interface {
	Handle(ctx context.Context, header http.Header, calls []dispatchproto.Call) ([]dispatchproto.ID, error)
}

// HandlerFunc creates a Handler from a function.
func HandlerFunc(fn func(context.Context, http.Header, []dispatchproto.Call) ([]dispatchproto.ID, error)) Handler {
	return handlerFunc(fn)
}

type handlerFunc func(context.Context, http.Header, []dispatchproto.Call) ([]dispatchproto.ID, error)

func (h handlerFunc) Handle(ctx context.Context, header http.Header, calls []dispatchproto.Call) ([]dispatchproto.ID, error) {
	return h(ctx, header, calls)
}

// New creates a Server.
func New(handler Handler, opts ...connect.HandlerOption) (*Server, error) {
	validator, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}
	opts = append(opts, connect.WithInterceptors(validator))
	grpcHandler := &dispatchServiceHandler{handler}
	path, httpHandler := sdkv1connect.NewDispatchServiceHandler(grpcHandler, opts...)
	return &Server{
		path:    path,
		handler: httpHandler,
	}, nil
}

// Server is a Dispatch API server.
type Server struct {
	path    string
	handler http.Handler
}

// Handler returns an HTTP handler for the Dispatch API server, along with
// the path that the handler should be registered at.
func (s *Server) Handler() (string, http.Handler) {
	return s.path, s.handler
}

// Serve serves the Server on the specified address.
func (s *Server) Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle(s.Handler())
	server := &http.Server{Addr: addr, Handler: mux}
	return server.ListenAndServe()
}

type dispatchServiceHandler struct{ Handler }

func (d *dispatchServiceHandler) Dispatch(ctx context.Context, req *connect.Request[sdkv1.DispatchRequest]) (*connect.Response[sdkv1.DispatchResponse], error) {
	calls := make([]dispatchproto.Call, len(req.Msg.Calls))
	for i, c := range req.Msg.Calls {
		calls[i] = newProtoCall(c)
	}
	ids, err := d.Handle(ctx, req.Header(), calls)
	if err != nil {
		return nil, err
	}
	if len(ids) != len(calls) {
		panic("invalid handler response")
	}
	dispatchIDs := make([]string, len(ids))
	for i, id := range ids {
		dispatchIDs[i] = string(id)
	}
	return connect.NewResponse(&sdkv1.DispatchResponse{
		DispatchIds: dispatchIDs,
	}), nil
}

//go:linkname newProtoCall github.com/dispatchrun/dispatch-go/dispatchproto.newProtoCall
func newProtoCall(c *sdkv1.Call) dispatchproto.Call
