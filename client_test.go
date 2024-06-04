package dispatch_test

import (
	"context"
	"testing"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestClient(t *testing.T) {
	var recorder dispatchtest.Recorder

	server := dispatchtest.NewServer(&recorder)

	client := &dispatch.Client{ApiKey: "foobar", ApiUrl: server.URL}

	input := wrapperspb.Int32(11)
	call, err := dispatch.NewCall("http://example.com", "function1", input)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	if len(recorder.Requests) != 1 {
		t.Fatalf("expected one request to Dispatch, got %v", len(recorder.Requests))
	}
	req := &recorder.Requests[0]
	if req.ApiKey != "foobar" {
		t.Errorf("unexpected API key: %v", req.ApiKey)
	}
	if len(req.Calls) != 1 {
		t.Fatalf("expected one call to Dispatch, got %v", len(req.Calls))
	}
	got := req.Calls[0]
	if got.Endpoint() != call.Endpoint() {
		t.Errorf("unexpected call endpoint: %v", got.Endpoint())
	}
	if got.Function() != call.Function() {
		t.Errorf("unexpected call function: %v", got.Function())
	}
	gotInput, err := got.Input()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(gotInput, input) {
		t.Errorf("unexpected call input: %#v", gotInput)
	}
}

func TestClientEnvConfig(t *testing.T) {
	var recorder dispatchtest.Recorder

	server := dispatchtest.NewServer(&recorder)

	client := &dispatch.Client{Env: []string{
		"DISPATCH_API_KEY=foobar",
		"DISPATCH_API_URL=" + server.URL,
	}}

	input := wrapperspb.Int32(11)
	call, err := dispatch.NewCall("http://example.com", "function1", input)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	if len(recorder.Requests) != 1 {
		t.Fatalf("expected one request to Dispatch, got %v", len(recorder.Requests))
	}
	req := &recorder.Requests[0]
	if req.ApiKey != "foobar" {
		t.Errorf("unexpected API key: %v", req.ApiKey)
	}
}
