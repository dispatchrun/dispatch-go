package dispatch_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
	if err := res.GetExit().GetResult().GetError(); err == nil || err.Message != "unsupported coroutine directive: <nil>" {
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

func TestFunctionRunSleep(t *testing.T) {
	const sleep = 20 * time.Millisecond

	fn := dispatch.NewFunction("sleeper", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		dispatch.Sleep(sleep)
		return req, nil
	})

	start := time.Now()
	res := fn.Run(context.Background(), &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{},
	})
	result := res.GetExit().GetResult()
	if err := result.GetError(); err != nil {
		t.Fatalf("unexpected error: %s %s", err.Type, err.Message)
	}
	if delay := time.Since(start); delay < sleep {
		t.Fatalf("expected coroutine to sleep for at least %s, slept for %s", sleep, delay)
	}
}

func TestFunctionRunCancel(t *testing.T) {
	fn := dispatch.NewFunction("sleeper", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		dispatch.Sleep(10 * time.Second) // won't wait for that long beccause the context is canceled
		return req, nil
	})

	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("oops")
	cancel(cause)

	res := fn.Run(ctx, &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{},
	})
	if err := res.GetExit().GetResult().GetError(); err == nil || err.Message != "oops" {
		t.Fatalf("unexpected error: %#v", err)
	}
}
