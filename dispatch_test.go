package dispatch_test

import (
	"context"
	"fmt"
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

	endpoint.Register(dispatch.NewPrimitiveFunction("identity", func(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
		var input *anypb.Any
		switch d := req.Directive.(type) {
		case *sdkv1.RunRequest_Input:
			input = d.Input
		default:
			return dispatch.ErrorResponse(fmt.Errorf("%w: unexpected run directive: %T", dispatch.ErrInvalidArgument, d))
		}
		return &sdkv1.RunResponse{
			Status: sdkv1.Status_STATUS_OK,
			Directive: &sdkv1.RunResponse_Exit{
				Exit: &sdkv1.Exit{
					Result: &sdkv1.CallResult{
						Output: input,
					},
				},
			},
		}
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
	if res.Status != sdkv1.Status_STATUS_OK {
		t.Fatalf("unexpected response status: %v", res.Status)
	}
	if d, ok := res.Directive.(*sdkv1.RunResponse_Exit); !ok {
		t.Errorf("unexpected response directive: %T", res.Directive)
	} else if output := d.Exit.GetResult().GetOutput(); output == nil {
		t.Error("exit directive result or output was nil")
	} else if message, err := output.UnmarshalNew(); err != nil {
		t.Errorf("exit directive result or output was invalid: %v", output)
	} else if v, ok := message.(*wrapperspb.Int32Value); !ok || v.Value != inputValue {
		t.Errorf("exit directive result or output was invalid: %v", v)
	}

	// Try running a function that has not been registered.
	res, err = client.Run(context.Background(), &sdkv1.RunRequest{Function: "not_found"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != sdkv1.Status_STATUS_NOT_FOUND {
		t.Fatalf("unexpected response status: %v", res.Status)
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

	fn := dispatch.NewPrimitiveFunction("function1", func(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
		panic("not implemented")
	})
	endpoint.Register(fn)

	_, err = fn.Dispatch(context.Background(), wrapperspb.Int32(11), dispatch.WithExpiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	wantCall, err := dispatch.NewCall("http://example.com", "function1", wrapperspb.Int32(11), dispatch.WithExpiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	dispatchtest.AssertDispatchRequests(t, recorder.Requests, []dispatchtest.DispatchRequest{
		{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{wantCall},
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

	fn := dispatch.NewPrimitiveFunction("function1", func(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
		panic("not implemented")
	})
	endpoint.Register(fn)

	_, err = fn.Dispatch(context.Background(), wrapperspb.Int32(11), dispatch.WithExpiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	wantCall, err := dispatch.NewCall("http://example.com", "function1", wrapperspb.Int32(11), dispatch.WithExpiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	dispatchtest.AssertDispatchRequests(t, recorder.Requests, []dispatchtest.DispatchRequest{
		{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{wantCall},
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

	fn1 := dispatch.NewPrimitiveFunction("function1", func(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
		panic("not implemented")
	})
	fn2 := dispatch.NewFunction("function2", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		panic("not implemented")
	})

	endpoint.Register(fn1)
	endpoint.Register(fn2)

	call1, err := fn1.BuildCall(wrapperspb.Int32(11), dispatch.WithExpiration(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	call2, err := fn2.BuildCall(wrapperspb.String("foo"), dispatch.WithVersion("xyzzy"))
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

	dispatchtest.AssertDispatchRequests(t, recorder.Requests, []dispatchtest.DispatchRequest{
		{
			ApiKey: "foobar",
			Calls:  []dispatch.Call{call1, call2},
		},
	})
}
