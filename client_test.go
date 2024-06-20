package dispatch_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
)

func TestClient(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewServer(recorder)

	client, err := dispatch.NewClient(dispatch.APIKey("foobar"), dispatch.APIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	call := dispatchproto.NewCall("http://example.com", "function1", dispatchproto.Int(11))

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		Header: http.Header{"Authorization": []string{"Bearer foobar"}},
		Calls:  []dispatchproto.Call{call},
	})
}

func TestClientEnvConfig(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewServer(recorder)

	client, err := dispatch.NewClient(dispatch.Env(
		"DISPATCH_API_KEY=foobar",
		"DISPATCH_API_URL="+server.URL,
	))
	if err != nil {
		t.Fatal(err)
	}

	call := dispatchproto.NewCall("http://example.com", "function1", dispatchproto.Int(11))

	_, err = client.Dispatch(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		Header: http.Header{"Authorization": []string{"Bearer foobar"}},
		Calls:  []dispatchproto.Call{call},
	})
}

func TestClientBatch(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewServer(recorder)

	client, err := dispatch.NewClient(dispatch.APIKey("foobar"), dispatch.APIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	call1 := dispatchproto.NewCall("http://example.com", "function1", dispatchproto.Int(11))
	call2 := dispatchproto.NewCall("http://example.com", "function2", dispatchproto.Int(22))
	call3 := dispatchproto.NewCall("http://example.com", "function3", dispatchproto.Int(33))
	call4 := dispatchproto.NewCall("http://example2.com", "function4", dispatchproto.Int(44))

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
			Header: http.Header{"Authorization": []string{"Bearer foobar"}},
			Calls:  []dispatchproto.Call{call1, call2},
		},
		dispatchtest.DispatchRequest{
			Header: http.Header{"Authorization": []string{"Bearer foobar"}},
			Calls:  []dispatchproto.Call{call3, call4},
		})
}

func TestClientNoAPIKey(t *testing.T) {
	_, err := dispatch.NewClient(dispatch.Env( /* i.e. no env vars */ ))
	if err == nil {
		t.Fatalf("expected an error")
	} else if err.Error() != "Dispatch API key has not been set. Use APIKey(..), or set the DISPATCH_API_KEY environment variable" {
		t.Errorf("unexpected error: %v", err)
	}
}
