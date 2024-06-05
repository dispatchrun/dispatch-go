package dispatch_test

import (
	"context"
	"testing"
	"time"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestDispatchEndpoint(t *testing.T) {
	signingKey, verificationKey := dispatchtest.KeyPair()

	endpoint, server, err := dispatchtest.NewEndpoint(dispatch.WithVerificationKey(verificationKey))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := server.Client(dispatchtest.WithSigningKey(signingKey))
	if err != nil {
		t.Fatal(err)
	}

	endpoint.Register(dispatch.NewPrimitiveFunction("identity", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		input, ok := req.Input()
		if !ok {
			return dispatch.NewErrorfResponse("%w: unexpected request: %v", dispatch.ErrInvalidArgument, req)
		}
		return dispatch.NewOutputResponse(input.Value())
	}))

	// Send a request for the identity function, and check that the
	// input was echoed back.
	const inputValue = 11
	input, err := anypb.New(wrapperspb.Int32(inputValue))
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Run(context.Background(), &sdkv1.RunRequest{
		Function:  "identity",
		Directive: &sdkv1.RunRequest_Input{Input: input},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status() != dispatch.OKStatus {
		t.Fatalf("unexpected response status: %v", res.Status())
	}
	output, ok := res.Output()
	if !ok {
		t.Fatalf("invalid response: %v (%v)", res, err)
	}
	m, err := output.Proto()
	if err != nil {
		t.Fatal(err)
	} else if v, ok := m.(*wrapperspb.Int32Value); !ok || v.Value != inputValue {
		t.Errorf("exit directive result or output was invalid: %v", output)
	}

	// Try running a function that has not been registered.
	res, err = client.Run(context.Background(), &sdkv1.RunRequest{Function: "not_found"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status() != dispatch.NotFoundStatus {
		t.Fatalf("unexpected response status: %v", res.Status())
	}

	// Try with a client that does not sign requests. The Dispatch
	// instance should reject the request.
	nonSigningClient, err := server.Client()
	if err != nil {
		t.Fatal(err)
	}
	_, err = nonSigningClient.Run(context.Background(), &sdkv1.RunRequest{
		Function:  "identity",
		Directive: &sdkv1.RunRequest_Input{Input: input},
	})
	if err == nil || connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("expected a permission denied error, got %v", err)
	}
}

func TestDispatchCall(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewDispatchServer(recorder)

	endpoint, err := dispatch.New(
		dispatch.WithEndpointUrl("http://example.com"),
		dispatch.WithClientOptions(dispatch.WithAPIKey("foobar"), dispatch.WithAPIUrl(server.URL)))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.NewPrimitiveFunction("function1", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		panic("not implemented")
	})
	endpoint.Register(fn)

	_, err = fn.Dispatch(context.Background(), dispatch.Int(11), dispatch.WithExpiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		ApiKey: "foobar",
		Calls: []dispatch.Call{
			dispatch.NewCall("http://example.com", "function1", dispatch.Int(11),
				dispatch.WithExpiration(10*time.Second)),
		},
	})
}

func TestDispatchCallEnvConfig(t *testing.T) {
	recorder := &dispatchtest.CallRecorder{}
	server := dispatchtest.NewDispatchServer(recorder)

	endpoint, err := dispatch.New(dispatch.WithEnv(
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

	_, err = fn.Dispatch(context.Background(), wrapperspb.String("foo"), dispatch.WithVersion("xyzzy"))
	if err != nil {
		t.Fatal(err)
	}

	recorder.Assert(t, dispatchtest.DispatchRequest{
		ApiKey: "foobar",
		Calls: []dispatch.Call{
			dispatch.NewCall("http://example.com", "function2", dispatch.String("foo"), dispatch.WithVersion("xyzzy")),
		},
	})
}

func TestDispatchCallsBatch(t *testing.T) {
	var recorder dispatchtest.CallRecorder

	server := dispatchtest.NewDispatchServer(&recorder)

	endpoint, err := dispatch.New(
		dispatch.WithEndpointUrl("http://example.com"),
		dispatch.WithClientOptions(dispatch.WithAPIKey("foobar"), dispatch.WithAPIUrl(server.URL)))
	if err != nil {
		t.Fatal(err)
	}

	fn1 := dispatch.NewPrimitiveFunction("function1", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		panic("not implemented")
	})
	fn2 := dispatch.NewFunction("function2", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		panic("not implemented")
	})

	endpoint.Register(fn1)
	endpoint.Register(fn2)

	call1, err := fn1.NewCall(dispatch.Int(11), dispatch.WithExpiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	call2, err := fn2.NewCall(wrapperspb.String("foo"), dispatch.WithVersion("xyzzy"))
	if err != nil {
		t.Fatal(err)
	}

	client, err := endpoint.Client()
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
		_, err := dispatch.New(dispatch.WithEnv( /* i.e. no env vars */ ))
		if err == nil || err.Error() != "Dispatch endpoint URL has not been set. Use WithEndpointUrl(..), or set the DISPATCH_ENDPOINT_URL environment variable" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := dispatch.New(dispatch.WithEndpointUrl(":://::"))
		if err == nil || err.Error() != "invalid endpoint URL provided via WithEndpointUrl: :://::" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid env", func(t *testing.T) {
		_, err := dispatch.New(dispatch.WithEnv(
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
		_, err := dispatch.New(dispatch.WithEndpointUrl("http://example.com"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := dispatch.New(dispatch.WithEndpointUrl("http://example.com"), dispatch.WithVerificationKey("foo"))
		if err == nil || err.Error() != "invalid verification key provided via WithVerificationKey: foo" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid env", func(t *testing.T) {
		_, err := dispatch.New(dispatch.WithEnv(
			"DISPATCH_ENDPOINT_URL=http://example.com",
			"DISPATCH_VERIFICATION_KEY=foo",
		))
		if err == nil || err.Error() != "invalid DISPATCH_VERIFICATION_KEY: foo" {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
