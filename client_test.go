package dispatch_test

import (
	"context"
	"testing"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestClient(t *testing.T) {
	var recorder dispatchtest.CallRecorder

	server := dispatchtest.NewDispatchServer(&recorder)

	client := &dispatch.Client{ApiKey: "foobar", ApiUrl: server.URL}

	call, err := dispatch.NewCall("http://example.com", "function1", wrapperspb.Int32(11))
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	dispatchtest.AssertDispatchRequests(t, recorder.Requests, []dispatchtest.DispatchRequest{
		{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{call},
		},
	})
}

func TestClientEnvConfig(t *testing.T) {
	var recorder dispatchtest.CallRecorder

	server := dispatchtest.NewDispatchServer(&recorder)

	client := &dispatch.Client{Env: []string{
		"DISPATCH_API_KEY=foobar",
		"DISPATCH_API_URL=" + server.URL,
	}}

	call, err := dispatch.NewCall("http://example.com", "function1", wrapperspb.Int32(11))
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	dispatchtest.AssertDispatchRequests(t, recorder.Requests, []dispatchtest.DispatchRequest{
		{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{call},
		},
	})
}

func TestClientBatch(t *testing.T) {
	var recorder dispatchtest.CallRecorder

	server := dispatchtest.NewDispatchServer(&recorder)

	client := &dispatch.Client{ApiKey: "foobar", ApiUrl: server.URL}

	call1, err := dispatch.NewCall("http://example.com", "function1", wrapperspb.Int32(11))
	if err != nil {
		t.Fatal(err)
	}
	call2, err := dispatch.NewCall("http://example.com", "function2", wrapperspb.Int32(22))
	if err != nil {
		t.Fatal(err)
	}
	call3, err := dispatch.NewCall("http://example.com", "function3", wrapperspb.Int32(33))
	if err != nil {
		t.Fatal(err)
	}
	call4, err := dispatch.NewCall("http://example2.com", "function4", wrapperspb.Int32(44))
	if err != nil {
		t.Fatal(err)
	}

	batch := client.Batch()
	batch.Add(call1, call2)
	_, err = batch.Dispatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	batch.Reset()
	batch.Add(call3)
	batch.Add(call4)
	_, err = batch.Dispatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	dispatchtest.AssertDispatchRequests(t, recorder.Requests, []dispatchtest.DispatchRequest{
		{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{call1, call2},
		},
		{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{call3, call4},
		},
	})
}
