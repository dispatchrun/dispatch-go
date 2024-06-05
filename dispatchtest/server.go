package dispatchtest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go"
)

// DispatchServerHandler is a handler for a test Dispatch API server.
type DispatchServerHandler interface {
	Handle(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error)
}

// DispatchServerHandlerFunc creates a DispatchServerHandler from a function.
func DispatchServerHandlerFunc(fn func(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error)) DispatchServerHandler {
	return dispatchServerHandlerFunc(fn)
}

type dispatchServerHandlerFunc func(context.Context, string, []dispatch.Call) ([]dispatch.ID, error)

func (h dispatchServerHandlerFunc) Handle(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error) {
	return h(ctx, apiKey, calls)
}

// NewDispatchServer creates a new test Dispatch API server.
func NewDispatchServer(handler DispatchServerHandler) *httptest.Server {
	mux := http.NewServeMux()
	mux.Handle(sdkv1connect.NewDispatchServiceHandler(&dispatchServiceHandler{handler}))
	return httptest.NewServer(mux)
}

type dispatchServiceHandler struct {
	DispatchServerHandler
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
		dispatch.WithCallCorrelationID(c.CorrelationId),
		dispatch.WithCallExpiration(c.Expiration.AsDuration()),
		dispatch.WithCallVersion(c.Version))
}

// CallRecorder is a DispatchServerHandler that captures requests to the Dispatch API.
type CallRecorder struct {
	requests []DispatchRequest
	calls    int
}

// DispatchRequest is a request to the Dispatch API captured by a CallRecorder.
type DispatchRequest struct {
	ApiKey string
	Calls  []dispatch.Call
}

func (r *CallRecorder) Handle(ctx context.Context, apiKey string, calls []dispatch.Call) ([]dispatch.ID, error) {
	base := r.calls
	r.calls += len(calls)

	r.requests = append(r.requests, DispatchRequest{
		ApiKey: apiKey,
		Calls:  calls,
	})

	ids := make([]dispatch.ID, len(calls))
	for i := range calls {
		ids[i] = strconv.Itoa(base + i)
	}
	return ids, nil
}

func (r *CallRecorder) Assert(t *testing.T, want ...DispatchRequest) {
	t.Helper()

	got := r.requests
	if len(got) != len(want) {
		t.Fatalf("unexpected number of requests: got %v, want %v", len(got), len(want))
	}
	for i, req := range got {
		if req.ApiKey != want[i].ApiKey {
			t.Errorf("unexpected API key on request %d: got %v, want %v", i, req.ApiKey, want[i].ApiKey)
		}
		if len(req.Calls) != len(want[i].Calls) {
			t.Errorf("unexpected number of calls in request %d: got %v, want %v", i, len(req.Calls), len(want[i].Calls))
		} else {
			for j, call := range req.Calls {
				if !call.Equal(want[i].Calls[j]) {
					t.Errorf("unexpected request %d call %d: got %v, want %v", i, j, call, want[i].Calls[j])
				}
			}
		}
	}
}
