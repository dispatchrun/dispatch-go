//go:build !durable

package dispatchtest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"
	_ "unsafe"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/dispatchrun/dispatch-go/dispatchserver"
)

// NewServer creates a new test Dispatch API server.
func NewServer(handler dispatchserver.Handler) *httptest.Server {
	s, err := dispatchserver.New(handler)
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle(s.Handler())
	return httptest.NewServer(mux)
}

// CallRecorder is a dispatchserver.Handler that captures requests to the Dispatch API.
type CallRecorder struct {
	requests []DispatchRequest
	calls    int
}

// DispatchRequest is a request to the Dispatch API captured by a CallRecorder.
type DispatchRequest struct {
	Header http.Header
	Calls  []dispatchproto.Call
}

func (r *CallRecorder) Handle(ctx context.Context, header http.Header, calls []dispatchproto.Call) ([]dispatchproto.ID, error) {
	base := r.calls
	r.calls += len(calls)

	r.requests = append(r.requests, DispatchRequest{
		Header: header.Clone(),
		Calls:  slices.Clone(calls),
	})

	ids := make([]dispatchproto.ID, len(calls))
	for i := range calls {
		ids[i] = dispatchproto.ID(strconv.Itoa(base + i))
	}
	return ids, nil
}

// Assert asserts that specific calls were made to the Dispatch API server,
// and that specified headers are present.
//
// When validating request headers, Assert checks that the specified headers
// were present, but allows extra headers on the request. That is, it's not
// checking for an exact match with headers.
func (r *CallRecorder) Assert(t *testing.T, want ...DispatchRequest) {
	t.Helper()

	got := r.requests
	if len(got) != len(want) {
		t.Errorf("unexpected number of requests: got %v, want %v", len(got), len(want))
	}
	for i, req := range got {
		if i >= len(want) {
			break
		}

		// Check headers.
		for name, want := range want[i].Header {
			got, ok := req.Header[name]
			if !ok {
				t.Errorf("missing %s header in request %d", name, i)
			} else if !slices.Equal(got, want) {
				t.Errorf("unexpected %s header in request %d: got %v, want %v", name, i, got, want)
			}
		}

		// Check calls.
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
