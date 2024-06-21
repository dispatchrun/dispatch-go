package dispatch_test

import (
	"context"
	"testing"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

func TestPrimitiveFunctionNewCallAndDispatchWithoutEndpoint(t *testing.T) {
	fn := dispatch.PrimitiveFunc("foo", func(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
		panic("not implemented")
	})

	_, err := fn.BuildCall(dispatchproto.String("bar")) // allowed
	if err != nil {
		t.Fatal(err)
	}
	_, err = fn.Dispatch(context.Background(), dispatchproto.String("bar"))
	if err == nil || err.Error() != "cannot dispatch function call: function has not been registered with a Dispatch endpoint" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFunctionNewCallAndDispatchWithoutEndpoint(t *testing.T) {
	fn := dispatch.Func("foo", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})

	_, err := fn.BuildCall("bar") // allowed
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

	fn := dispatch.PrimitiveFunc("foo", func(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
		panic("not implemented")
	})
	endpoint.Register(fn)

	if _, err := fn.BuildCall(dispatchproto.String("bar")); err != nil { // allowed
		t.Fatal(err)
	}

	// However, a client is not available.
	_, err = fn.Dispatch(context.Background(), dispatchproto.String("bar"))
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

	fn := dispatch.Func("foo", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})
	endpoint.Register(fn)

	if _, err := fn.BuildCall("bar"); err != nil { // allowed
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
