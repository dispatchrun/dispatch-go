package dispatch_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

	endpoint.Register(dispatch.NewPrimitiveFunction("identity", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		input, ok := req.Input()
		if !ok {
			return dispatch.NewResponseErrorf("%w: unexpected request: %v", dispatch.ErrInvalidArgument, req)
		}
		return dispatch.NewResponse(dispatch.OKStatus, dispatch.Output(input))
	}))

	// Send a request for the identity function, and check that the
	// input was echoed back.
	req := dispatch.NewRequest("identity", dispatch.Input(dispatch.Int(11)))
	res, err := client.Run(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	} else if res.Status() != dispatch.OKStatus {
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
	res, err = client.Run(context.Background(), dispatch.NewRequest("not_found", dispatch.Input(dispatch.Int(22))))
	if err != nil {
		t.Fatal(err)
	} else if res.Status() != dispatch.NotFoundStatus {
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
	server := dispatchtest.NewDispatchServer(recorder)

	client, err := dispatch.NewClient(dispatch.APIKey("foobar"), dispatch.APIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	endpoint, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), client)
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.NewPrimitiveFunction("function1", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		panic("not implemented")
	})
	endpoint.Register(fn)

	_, err = fn.Dispatch(context.Background(), dispatch.Int(11), dispatch.Expiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		ApiKey: "foobar",
		Calls: []dispatch.Call{
			dispatch.NewCall("http://example.com", "function1",
				dispatch.Input(dispatch.Int(11)),
				dispatch.Expiration(10*time.Second)),
		},
	})
}

func TestDispatchCallEnvConfig(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewDispatchServer(recorder)

	endpoint, err := dispatch.New(dispatch.Env(
		"DISPATCH_ENDPOINT_URL=http://example.com",
		"DISPATCH_API_KEY=foobar",
		"DISPATCH_API_URL="+server.URL,
	))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.NewFunction("function2", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		panic("not implemented")
	})
	endpoint.Register(fn)

	_, err = fn.Dispatch(context.Background(), wrapperspb.String("foo"), dispatch.Version("xyzzy"))
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		ApiKey: "foobar",
		Calls: []dispatch.Call{
			dispatch.NewCall("http://example.com", "function2",
				dispatch.Input(dispatch.String("foo")),
				dispatch.Version("xyzzy")),
		},
	})
}

func TestDispatchCallsBatch(t *testing.T) {
	var recorder dispatchtest.CallRecorder

	server := dispatchtest.NewDispatchServer(&recorder)

	client, err := dispatch.NewClient(dispatch.APIKey("foobar"), dispatch.APIUrl(server.URL))
	if err != nil {
		t.Fatal(err)
	}

	endpoint, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), client)
	if err != nil {
		t.Fatal(err)
	}
	client = nil

	fn1 := dispatch.NewPrimitiveFunction("function1", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		panic("not implemented")
	})
	fn2 := dispatch.NewFunction("function2", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		panic("not implemented")
	})

	endpoint.Register(fn1)
	endpoint.Register(fn2)

	call1, err := fn1.NewCall(dispatch.Int(11), dispatch.Expiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	call2, err := fn2.NewCall(wrapperspb.String("foo"), dispatch.Version("xyzzy"))
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
		ApiKey: "foobar",
		Calls:  []dispatch.Call{call1, call2},
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
