package dispatchtest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go"
)

// Handler is a handler for dispatched function calls.
type Handler interface {
	Handle(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error)
}

// HandlerFunc creates a Handler from a function.
func HandlerFunc(fn func(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error)) Handler {
	return handlerFunc(fn)
}

type handlerFunc func(context.Context, string, []dispatch.Call) ([]dispatch.ID, error)

func (h handlerFunc) Handle(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error) {
	return h(ctx, apiKey, calls)
}

// NewServer creates a new test Dispatch server.
func NewServer(handler Handler) *httptest.Server {
	mux := http.NewServeMux()
	mux.Handle(sdkv1connect.NewDispatchServiceHandler(&dispatchServiceHandler{handler}))
	return httptest.NewServer(mux)
}

type dispatchServiceHandler struct {
	Handler
}

func (d *dispatchServiceHandler) Dispatch(ctx context.Context, req *connect.Request[sdkv1.DispatchRequest]) (*connect.Response[sdkv1.DispatchResponse], error) {
	auth := req.Header().Get("Authorization")
	apiKey, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("missing or invalid Authorization header: %q", auth))
	}

	calls := make([]dispatch.Call, len(req.Msg.Calls))
	for i, c := range req.Msg.Calls {
		var err error
		calls[i], err = wrapCall(c)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid call %d: %v", i+1, err))
		}
	}

	ids, err := d.Handle(ctx, apiKey, calls)
	if err != nil {
		return nil, err
	}
	if len(ids) != len(calls) {
		panic("invalid handler response")
	}
	return connect.NewResponse(&sdkv1.DispatchResponse{
		DispatchIds: ids,
	}), nil
}

func wrapCall(c *sdkv1.Call) (dispatch.Call, error) {
	input, err := c.Input.UnmarshalNew()
	if err != nil {
		return dispatch.Call{}, err
	}
	return dispatch.NewCall(c.Endpoint, c.Function, input,
		dispatch.WithCorrelationID(c.CorrelationId),
		dispatch.WithExpiration(c.Expiration.AsDuration()),
		dispatch.WithVersion(c.Version))
}

// Recorder is a Handler that captures requests to Dispatch.
type Recorder struct {
	Requests []RecorderRequest
	calls    int
}

type RecorderRequest struct {
	ApiKey string
	Calls  []dispatch.Call
}

func (r *Recorder) Handle(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error) {
	base := r.calls
	r.calls += len(calls)

	r.Requests = append(r.Requests, RecorderRequest{
		ApiKey: apiKey,
		Calls:  calls,
	})

	ids := make([]dispatch.ID, len(calls))
	for i := range calls {
		ids[i] = strconv.Itoa(base + i)
	}
	return ids, nil
}