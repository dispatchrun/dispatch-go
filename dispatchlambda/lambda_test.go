package dispatchlambda_test

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchlambda"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestHandlerEmptyPayload(t *testing.T) {
	fn := dispatch.Func("handler", func(ctx context.Context, input string) (string, error) {
		return "", nil
	})
	h := dispatchlambda.Handler(fn)
	_, err := h.Invoke(context.Background(), nil)
	assertInvokeError(t, err, "Bad Request", "empty payload")
}

func TestHandlerShortPayload(t *testing.T) {
	fn := dispatch.Func("handler", func(ctx context.Context, input string) (string, error) {
		return "", nil
	})
	h := dispatchlambda.Handler(fn)
	_, err := h.Invoke(context.Background(), []byte(`@`))
	assertInvokeError(t, err, "Bad Request", "payload is too short")
}

func TestHandlerNonBase64Payload(t *testing.T) {
	fn := dispatch.Func("handler", func(ctx context.Context, input string) (string, error) {
		return "", nil
	})
	h := dispatchlambda.Handler(fn)
	_, err := h.Invoke(context.Background(), []byte(`"not base64"`))
	assertInvokeError(t, err, "Bad Request", "payload is not base64 encoded")
}

func TestHandlerInvokePayloadNotProtobufMessage(t *testing.T) {
	fn := dispatch.Func("handler", func(ctx context.Context, input string) (string, error) {
		return "", nil
	})
	h := dispatchlambda.Handler(fn)
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: "arn:aws:lambda:us-east-1:123456789012:function:my-function:1",
	})
	_, err := h.Invoke(ctx, []byte(`"aW52b2tlZDovL2Z1bmN0aW9uOg=="`))
	assertInvokeError(t, err, "Bad Request", "raw payload did not contain a protobuf encoded execution request")
}

func TestHandlerInvokeError(t *testing.T) {
	fn := dispatch.Func("handler", func(ctx context.Context, input string) (string, error) {
		return "", errors.New("invoke error")
	})
	h := dispatchlambda.Handler(fn)
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: "arn:aws:lambda:us-east-1:123456789012:function:my-function:1",
	})

	input, err := anypb.New(&wrapperspb.StringValue{Value: "input"})
	if err != nil {
		t.Fatalf("unexpected error creating input: %v", err)
	}

	req := &sdkv1.RunRequest{
		Function: "handler",
		Directive: &sdkv1.RunRequest_Input{
			Input: input,
		},
	}
	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("unexpected error marshaling request: %v", err)
	}

	payload := make([]byte, 2+base64.StdEncoding.EncodedLen(len(b)))
	payload[0] = '"'
	payload[len(payload)-1] = '"'
	base64.StdEncoding.Encode(payload[1:len(payload)-1], b)

	b, err = h.Invoke(ctx, payload)
	if err != nil {
		t.Fatalf("unexpected error invoking function: %v", err)
	}

	payload = make([]byte, base64.StdEncoding.DecodedLen(len(b)-2))
	n, err := base64.StdEncoding.Decode(payload, b[1:len(b)-1])
	if err != nil {
		t.Fatalf("unexpected error decoding payload: %v", err)
	}

	res := new(sdkv1.RunResponse)
	if err := proto.Unmarshal(payload[:n], res); err != nil {
		t.Fatalf("unexpected error unmarshaling result: %v", err)
	}
	switch coro := res.Directive.(type) {
	case *sdkv1.RunResponse_Exit:
		err := coro.Exit.GetResult().GetError()
		if err.Type != "errorString" {
			t.Errorf("expected coroutine to return an invoke error, got %q", err.Type)
		}
		if err.Message != "invoke error" {
			t.Errorf("expected coroutine to return an invoke error with message %q, got %q", "invoke error", err.Message)
		}
	default:
		t.Errorf("expected coroutine to return an error, got %T", coro)
	}
}

func TestHandlerInvokeFunction(t *testing.T) {
	fn := dispatch.Func("handler", func(ctx context.Context, input string) (string, error) {
		return input + "output", nil
	})
	h := dispatchlambda.Handler(fn)

	const (
		functionVersion        = "1"
		unqualifiedFunctionARN = "arn:aws:lambda:us-east-1:123456789012:function:my-function"
		qualifiedFunctionARN   = unqualifiedFunctionARN + ":" + functionVersion
	)

	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: qualifiedFunctionARN,
	})

	input, err := anypb.New(&wrapperspb.StringValue{Value: "input"})
	if err != nil {
		t.Fatalf("unexpected error creating input: %v", err)
	}

	req := &sdkv1.RunRequest{
		Function: "handler",
		Directive: &sdkv1.RunRequest_Input{
			Input: input,
		},
	}
	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("unexpected error marshaling request: %v", err)
	}

	payload := make([]byte, 2+base64.StdEncoding.EncodedLen(len(b)))
	payload[0] = '"'
	payload[len(payload)-1] = '"'
	base64.StdEncoding.Encode(payload[1:len(payload)-1], b)

	b, err = h.Invoke(ctx, payload)
	if err != nil {
		t.Fatalf("unexpected error invoking function: %v", err)
	}

	payload = make([]byte, base64.StdEncoding.DecodedLen(len(b)-2))
	n, err := base64.StdEncoding.Decode(payload, b[1:len(b)-1])
	if err != nil {
		t.Fatalf("unexpected error decoding payload: %v", err)
	}

	res := new(sdkv1.RunResponse)
	if err := proto.Unmarshal(payload[:n], res); err != nil {
		t.Fatalf("unexpected error unmarshaling result: %v", err)
	}

	if res.Status != sdkv1.Status_STATUS_OK {
		t.Errorf("expected coroutine to return status %q, got %q", sdkv1.Status_STATUS_OK, res.Status)
	}

	switch coro := res.Directive.(type) {
	case *sdkv1.RunResponse_Exit:
		out := coro.Exit.GetResult().GetOutput()
		if out.GetTypeUrl() != "type.googleapis.com/google.protobuf.StringValue" {
			t.Errorf("expected coroutine to return an output of type %q, got %q", "type.googleapis.com/google.protobuf.StringValue", out.GetTypeUrl())
		}
		var output wrapperspb.StringValue
		if err := out.UnmarshalTo(&output); err != nil {
			t.Fatalf("unexpected error unmarshaling output: %v", err)
		}
		if output.Value != "inputoutput" {
			t.Errorf("expected coroutine to return an output with value %q, got %q", "inputoutput", output.Value)
		}
	default:
		t.Errorf("expected coroutine to return an error, got %T", coro)
	}
}

func assertInvokeError(t *testing.T, err error, typ, msg string) {
	t.Helper()

	var invokeErr messages.InvokeResponse_Error
	if !errors.As(err, &invokeErr) {
		t.Errorf("expected InvokeResponse_Error, got %T", err)
		return
	}

	if invokeErr.Type != typ {
		t.Errorf("expected error type %q, got %q", typ, invokeErr.Type)
		return
	}

	if invokeErr.Message != msg {
		t.Errorf("expected error message %q, got %q", msg, invokeErr.Message)
		return
	}
}
