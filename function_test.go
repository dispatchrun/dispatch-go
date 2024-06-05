package dispatch_test

import (
	"context"
	"errors"
	"testing"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"github.com/dispatchrun/dispatch-go"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestFunctionRunInvalidCoroutineType(t *testing.T) {
	fn := dispatch.NewFunction("foo", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})

	res := fn.Run(context.Background(), &sdkv1.RunRequest{})
	if err := res.GetExit().GetResult().GetError(); err == nil || err.Message != "InvalidArgument: unsupported coroutine directive: <nil>" {
		t.Fatalf("unexpected error: %#v", err)
	}
}

func TestFunctionRunError(t *testing.T) {
	oops := errors.New("oops")

	fn := dispatch.NewFunction("foo", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, oops
	})

	input, err := anypb.New(wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}

	res := fn.Run(context.Background(), &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{
			Input: input,
		},
	})

	switch coro := res.Directive.(type) {
	case *sdkv1.RunResponse_Exit:
		err := coro.Exit.GetResult().GetError()
		if err.Type != "errorString" {
			t.Fatalf("unexpected coroutine error type: %s", err.Type)
		}
		if err.Message != "oops" {
			t.Fatalf("unexpected coroutine error message: %s", err.Message)
		}
	default:
		t.Fatalf("unexpected coroutine response type: %T", coro)
	}
}

func TestFunctionRunResult(t *testing.T) {
	fn := dispatch.NewFunction("foo", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("world"), nil
	})

	input, err := anypb.New(wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}

	res := fn.Run(context.Background(), &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{
			Input: input,
		},
	})

	switch coro := res.Directive.(type) {
	case *sdkv1.RunResponse_Exit:
		out := coro.Exit.GetResult().GetOutput()
		if out.TypeUrl != "type.googleapis.com/google.protobuf.StringValue" {
			t.Fatalf("unexpected coroutine output type: %s", out.TypeUrl)
		}
		var output wrapperspb.StringValue
		if err := out.UnmarshalTo(&output); err != nil {
			t.Fatal(err)
		}
		if output.Value != "world" {
			t.Fatalf("unexpected coroutine output value: %s", output.Value)
		}
	default:
		t.Fatalf("unexpected coroutine response type: %T", coro)
	}
}

func TestPrimitiveFunctionNewCallAndDispatchWithoutEndpoint(t *testing.T) {
	fn := dispatch.NewPrimitiveFunction("foo", func(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
		panic("not implemented")
	})

	wantErr := "cannot build function call: function has not been registered with a Dispatch endpoint"

	_, err := fn.NewCall(wrapperspb.String("bar"))
	if err == nil || err.Error() != wantErr {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = fn.Dispatch(context.Background(), wrapperspb.String("bar"))
	if err == nil || err.Error() != wantErr {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFunctionNewCallAndDispatchWithoutEndpoint(t *testing.T) {
	fn := dispatch.NewFunction("foo", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		panic("not implemented")
	})

	wantErr := "cannot build function call: function has not been registered with a Dispatch endpoint"

	_, err := fn.NewCall(wrapperspb.String("bar"))
	if err == nil || err.Error() != wantErr {
		t.Fatalf("unexpected error: %v", err)
	}
	_, err = fn.Dispatch(context.Background(), wrapperspb.String("bar"))
	if err == nil || err.Error() != wantErr {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPrimitiveFunctionDispatchWithoutClient(t *testing.T) {
	// It's not necessary to have valid Client configuration when
	// creating a Dispatch endpoint. In this case, there's no
	// Dispatch API key available.
	endpoint, err := dispatch.New(dispatch.WithEndpointUrl("http://example.com"), dispatch.WithEnv( /* i.e. no env vars */ ))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.NewPrimitiveFunction("foo", func(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
		panic("not implemented")
	})
	endpoint.Register(fn)

	// It's possible to create a call since an endpoint URL is available.
	if _, err := fn.NewCall(wrapperspb.String("bar")); err != nil {
		t.Fatal(err)
	}

	// However, a client is not available.
	_, err = fn.Dispatch(context.Background(), wrapperspb.String("bar"))
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "cannot dispatch function call: Dispatch API key has not been set. Use WithAPIKey(..), or set the DISPATCH_API_KEY environment variable" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestFunctionDispatchWithoutClient(t *testing.T) {
	// It's not necessary to have valid Client configuration when
	// creating a Dispatch endpoint. In this case, there's no
	// Dispatch API key available.
	endpoint, err := dispatch.New(dispatch.WithEndpointUrl("http://example.com"), dispatch.WithEnv( /* i.e. no env vars */ ))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.NewFunction("foo", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		panic("not implemented")
	})
	endpoint.Register(fn)

	// It's possible to create a call since an endpoint URL is available.
	if _, err := fn.NewCall(wrapperspb.String("bar")); err != nil {
		t.Fatal(err)
	}

	// However, a client is not available.
	_, err = fn.Dispatch(context.Background(), wrapperspb.String("bar"))
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "cannot dispatch function call: Dispatch API key has not been set. Use WithAPIKey(..), or set the DISPATCH_API_KEY environment variable" {
		t.Errorf("unexpected error: %v", err)
	}
}
