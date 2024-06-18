package dispatch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dispatchrun/dispatch-go"
)

func TestFunctionRunError(t *testing.T) {
	fn := dispatch.NewFunction("foo", func(ctx context.Context, input string) (string, error) {
		return "", errors.New("oops")
	})

	req := dispatch.NewRequest("foo", dispatch.String("hello"))
	res := fn.Run(context.Background(), req)
	error, ok := res.Error()
	if !ok {
		t.Fatalf("invalid response: %v", res)
	}
	if error.Type() != "errorString" {
		t.Errorf("unexpected coroutine error type: %s", error.Type())
	}
	if error.Message() != "oops" {
		t.Errorf("unexpected coroutine error message: %s", error.Message())
	}
}

func TestFunctionRunResult(t *testing.T) {
	fn := dispatch.NewFunction("foo", func(ctx context.Context, input string) (string, error) {
		return "world", nil
	})

	req := dispatch.NewRequest("foo", dispatch.String("hello"))
	res := fn.Run(context.Background(), req)
	if error, ok := res.Error(); ok {
		t.Fatalf("unexpected response error: %v", error)
	}
	var output string
	if boxed, ok := res.Output(); !ok {
		t.Fatalf("invalid response: %v", res)
	} else if err := boxed.Unmarshal(&output); err != nil {
		t.Fatalf("unexpected output: %v", err)
	} else if output != "world" {
		t.Errorf("unexpected output: %s", output)
	}
}

func TestPrimitiveFunctionNewCallAndDispatchWithoutEndpoint(t *testing.T) {
	fn := dispatch.NewPrimitiveFunction("foo", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		panic("not implemented")
	})

	_, err := fn.NewCall(dispatch.String("bar")) // allowed
	if err != nil {
		t.Fatal(err)
	}
	_, err = fn.Dispatch(context.Background(), dispatch.String("bar"))
	if err == nil || err.Error() != "cannot dispatch function call: function has not been registered with a Dispatch endpoint" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFunctionNewCallAndDispatchWithoutEndpoint(t *testing.T) {
	fn := dispatch.NewFunction("foo", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})

	_, err := fn.NewCall("bar") // allowed
	if err != nil {
		t.Fatal(err)
	}
	_, err = fn.Dispatch(context.Background(), "bar")
	if err == nil || err.Error() != "cannot dispatch function call: function has not been registered with a Dispatch endpoint" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPrimitiveFunctionDispatchWithoutClient(t *testing.T) {
	// It's not necessary to have valid Client configuration when
	// creating a Dispatch endpoint. In this case, there's no
	// Dispatch API key available.
	endpoint, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), dispatch.Env( /* i.e. no env vars */ ))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.NewPrimitiveFunction("foo", func(ctx context.Context, req dispatch.Request) dispatch.Response {
		panic("not implemented")
	})
	endpoint.Register(fn)

	// It's possible to create a call since an endpoint URL is available.
	if _, err := fn.NewCall(dispatch.String("bar")); err != nil {
		t.Fatal(err)
	}

	// However, a client is not available.
	_, err = fn.Dispatch(context.Background(), dispatch.String("bar"))
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "cannot dispatch function call: Dispatch API key has not been set. Use APIKey(..), or set the DISPATCH_API_KEY environment variable" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestFunctionDispatchWithoutClient(t *testing.T) {
	// It's not necessary to have valid Client configuration when
	// creating a Dispatch endpoint. In this case, there's no
	// Dispatch API key available.
	endpoint, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), dispatch.Env( /* i.e. no env vars */ ))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.NewFunction("foo", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})
	endpoint.Register(fn)

	// It's possible to create a call since an endpoint URL is available.
	if _, err := fn.NewCall("bar"); err != nil {
		t.Fatal(err)
	}

	// However, a client is not available.
	_, err = fn.Dispatch(context.Background(), "bar")
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "cannot dispatch function call: Dispatch API key has not been set. Use APIKey(..), or set the DISPATCH_API_KEY environment variable" {
		t.Errorf("unexpected error: %v", err)
	}
}
