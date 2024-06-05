package dispatchtest

import (
	"testing"

	"github.com/dispatchrun/dispatch-go"
)

func AssertCalls(t *testing.T, got, want []dispatch.Call) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("unexpected number of calls: got %v, want %v", len(got), len(want))
	}
	for i, call := range got {
		if !call.Equal(want[i]) {
			t.Errorf("unexpected call %d: got %v, want %v", i, call, want[i])
		}
	}
}

func AssertCall(t *testing.T, got, want dispatch.Call) {
	t.Helper()

	if !got.Equal(want) {
		t.Errorf("unexpected call: got %v, want %v", got, want)
	}
}

func AssertDispatchRequests(t *testing.T, got, want []DispatchRequest) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("unexpected number of requests: got %v, want %v", len(got), len(want))
	}
	for i, req := range got {
		if req.ApiKey != want[i].ApiKey {
			t.Errorf("unexpected API key on request %d: got %v, want %v", i, req.ApiKey, want[i].ApiKey)
		}
		AssertCalls(t, req.Calls, want[i].Calls)
	}
}
