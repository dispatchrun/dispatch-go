package dispatch_test

import (
	"context"
	"testing"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
)

func TestClient(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewDispatchServer(recorder)

	client, err := dispatch.NewClient(dispatch.WithAPIKey("foobar"), dispatch.WithAPIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	call := dispatch.NewCall("http://example.com", "function1", dispatch.Int(11))

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		ApiKey: "foobar",
		Calls:  []dispatch.Call{call},
	})
}

func TestClientEnvConfig(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewDispatchServer(recorder)

	client, err := dispatch.NewClient(dispatch.WithClientEnv(
		"DISPATCH_API_KEY=foobar",
		"DISPATCH_API_URL="+server.URL,
	))
	if err != nil {
		t.Fatal(err)
	}

	call := dispatch.NewCall("http://example.com", "function1", dispatch.Int(11))

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		ApiKey: "foobar",
		Calls:  []dispatch.Call{call},
	})
}

func TestClientBatch(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewDispatchServer(recorder)

	client, err := dispatch.NewClient(dispatch.WithAPIKey("foobar"), dispatch.WithAPIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	call1 := dispatch.NewCall("http://example.com", "function1", dispatch.Int(11))
	call2 := dispatch.NewCall("http://example.com", "function2", dispatch.Int(22))
	call3 := dispatch.NewCall("http://example.com", "function3", dispatch.Int(33))
	call4 := dispatch.NewCall("http://example2.com", "function4", dispatch.Int(44))

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

	recorder.Assert(t,
		dispatchtest.DispatchRequest{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{call1, call2},
		},
		dispatchtest.DispatchRequest{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{call3, call4},
		})
}

func TestClientNoAPIKey(t *testing.T) {
	_, err := dispatch.NewClient(dispatch.WithClientEnv( /* i.e. no env vars */ ))
	if err == nil {
		t.Fatalf("expected an error")
	} else if err.Error() != "Dispatch API key has not been set. Use WithAPIKey(..), or set the DISPATCH_API_KEY environment variable" {
		t.Errorf("unexpected error: %v", err)
	}
}
