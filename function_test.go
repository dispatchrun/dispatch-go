package dispatch_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sdkv1 "github.com/stealthrocket/dispatch/gen/go/dispatch/sdk/v1"
	"github.com/stealthrocket/dispatch/sdk/dispatch-go"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestFunctionRunInvalidCoroutineType(t *testing.T) {
	f := dispatch.Func(func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})

	_, err := f.Run(context.Background(), &sdkv1.RunRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "unsupported coroutine type: <nil>" {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestFunctionRunError(t *testing.T) {
	oops := errors.New("oops")

	f := dispatch.Func(func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, oops
	})

	input, err := anypb.New(wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}

	r, err := f.Run(context.Background(), &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{
			Input: input,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	switch coro := r.Directive.(type) {
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
	f := dispatch.Func(func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("world"), nil
	})

	input, err := anypb.New(wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}

	r, err := f.Run(context.Background(), &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{
			Input: input,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	switch coro := r.Directive.(type) {
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

	f := dispatch.Func(func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		dispatch.Sleep(sleep)
		return req, nil
	})

	start := time.Now()
	_, err := f.Run(context.Background(), &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if delay := time.Since(start); delay < sleep {
		t.Fatalf("expected coroutine to sleep for at least %s, slept for %s", sleep, delay)
	}
}

func TestFunctionRunCancel(t *testing.T) {
	f := dispatch.Func(func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		dispatch.Sleep(10 * time.Second) // won't wait for that long beccause the context is canceled
		return req, nil
	})

	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("oops")
	cancel(cause)

	_, err := f.Run(ctx, &sdkv1.RunRequest{
		Directive: &sdkv1.RunRequest_Input{},
	})
	if !errors.Is(err, cause) {
		t.Fatalf("expected coroutine to return an error: %v", err)
	}
}
