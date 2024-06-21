package dispatch_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchclient"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
)

func TestDispatchEndpoint(t *testing.T) {
	signingKey, verificationKey := dispatchtest.KeyPair()

	endpoint, server, err := dispatchtest.NewEndpoint(dispatch.VerificationKey(verificationKey))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := server.Client(dispatchtest.SigningKey(signingKey))
	if err != nil {
		t.Fatal(err)
	}

	endpoint.Register(dispatch.PrimitiveFunc("identity", func(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
		input, ok := req.Input()
		if !ok {
			return dispatchproto.NewResponseErrorf("%w: unexpected request: %v", dispatch.ErrInvalidArgument, req)
		}
		return dispatchproto.NewResponse(input)
	}))

	// Send a request for the identity function, and check that the
	// input was echoed back.
	req := dispatchproto.NewRequest("identity", dispatchproto.Int(11))
	res, err := client.Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	} else if res.Status() != dispatchproto.OKStatus {
		t.Fatalf("unexpected response status: %v", res.Status())
	}
	var output int
	if boxed, ok := res.Output(); !ok {
		t.Fatalf("invalid response: %v (%v)", res, err)
	} else if err := boxed.Unmarshal(&output); err != nil {
		t.Fatalf("invalid output: %v", err)
	} else if output != 11 {
		t.Fatalf("invalid output: %v", output)
	}

	// Try running a function that has not been registered.
	res, err = client.Run(context.Background(), dispatchproto.NewRequest("not_found", dispatchproto.Int(22)))
	if err != nil {
		t.Fatal(err)
	} else if res.Status() != dispatchproto.NotFoundStatus {
		t.Fatalf("unexpected response status: %v", res.Status())
	}

	// Try with a client that does not sign requests. The Dispatch
	// instance should reject the request.
	nonSigningClient, err := server.Client()
	if err != nil {
		t.Fatal(err)
	}
	_, err = nonSigningClient.Run(context.Background(), req)
	if err == nil || connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("expected a permission denied error, got %v", err)
	}
}

func TestDispatchCall(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewServer(recorder)

	client, err := dispatchclient.New(dispatchclient.APIKey("foobar"), dispatchclient.APIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	endpoint, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), dispatch.Client(client))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.PrimitiveFunc("function1", func(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
		panic("not implemented")
	})
	endpoint.Register(fn)

	_, err = fn.Dispatch(context.Background(), dispatchproto.Int(11), dispatchproto.Expiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		Header: http.Header{"Authorization": []string{"Bearer foobar"}},
		Calls: []dispatchproto.Call{
			dispatchproto.NewCall("http://example.com", "function1",
				dispatchproto.Int(11),
				dispatchproto.Expiration(10*time.Second)),
		},
	})
}

func TestDispatchCallEnvConfig(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewServer(recorder)

	endpoint, err := dispatch.New(dispatch.Env(
		"DISPATCH_ENDPOINT_URL=http://example.com",
		"DISPATCH_API_KEY=foobar",
		"DISPATCH_API_URL="+server.URL,
	))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.Func("function2", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})
	endpoint.Register(fn)

	_, err = fn.Dispatch(context.Background(), "foo", dispatchproto.Version("xyzzy"))
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		Header: http.Header{"Authorization": []string{"Bearer foobar"}},
		Calls: []dispatchproto.Call{
			dispatchproto.NewCall("http://example.com", "function2",
				dispatchproto.String("foo"),
				dispatchproto.Version("xyzzy")),
		},
	})
}

func TestDispatchCallsBatch(t *testing.T) {
	var recorder dispatchtest.CallRecorder

	server := dispatchtest.NewServer(&recorder)

	client, err := dispatchclient.New(dispatchclient.APIKey("foobar"), dispatchclient.APIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	endpoint, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), dispatch.Client(client))
	if err != nil {
		t.Fatal(err)
	}
	client = nil

	fn1 := dispatch.PrimitiveFunc("function1", func(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
		panic("not implemented")
	})
	fn2 := dispatch.Func("function2", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})

	endpoint.Register(fn1)
	endpoint.Register(fn2)

	call1, err := fn1.BuildCall(dispatchproto.Int(11), dispatchproto.Expiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	call2, err := fn2.BuildCall("foo", dispatchproto.Version("xyzzy"))
	if err != nil {
		t.Fatal(err)
	}

	client, err = endpoint.Client()
	if err != nil {
		t.Fatal(err)
	}

	batch := client.Batch()
	batch.Add(call1, call2)
	if _, err := batch.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		Header: http.Header{"Authorization": []string{"Bearer foobar"}},
		Calls:  []dispatchproto.Call{call1, call2},
	})
}

func TestDispatchEndpointURL(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		_, err := dispatch.New(dispatch.Env( /* i.e. no env vars */ ))
		if err == nil || err.Error() != "Dispatch endpoint URL has not been set. Use EndpointUrl(..), or set the DISPATCH_ENDPOINT_URL environment variable" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := dispatch.New(dispatch.EndpointUrl(":://::"))
		if err == nil || err.Error() != "invalid endpoint URL provided via EndpointUrl(..): :://::" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid env", func(t *testing.T) {
		_, err := dispatch.New(dispatch.Env(
			"DISPATCH_ENDPOINT_URL=:://::",
		))
		if err == nil || err.Error() != "invalid DISPATCH_ENDPOINT_URL: :://::" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestDispatchVerificationKey(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		// It's not an error to omit the verification key.
		_, err := dispatch.New(dispatch.EndpointUrl("http://example.com"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), dispatch.VerificationKey("foo"))
		if err == nil || err.Error() != "invalid verification key provided via VerificationKey(..): foo" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid env", func(t *testing.T) {
		_, err := dispatch.New(dispatch.Env(
			"DISPATCH_ENDPOINT_URL=http://example.com",
			"DISPATCH_VERIFICATION_KEY=foo",
		))
		if err == nil || err.Error() != "invalid DISPATCH_VERIFICATION_KEY: foo" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
