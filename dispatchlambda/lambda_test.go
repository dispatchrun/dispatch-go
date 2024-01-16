package dispatchlambda_test

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/stealthrocket/dispatch/sdk/dispatch-go/dispatchlambda"
	coroutinev1 "github.com/stealthrocket/ring/proto/go/ring/coroutine/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestHandlerEmptyPayload(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})
	_, err := h.Invoke(context.Background(), nil)
	assertInvokeError(t, err, "Bad Request", "empty payload")
}

func TestHandlerNonBase64Payload(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})
	_, err := h.Invoke(context.Background(), []byte(`"not base64"`))
	assertInvokeError(t, err, "Bad Request", "payload is not base64 encoded")
}

func TestHandlerMissingFunctionARN(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})
	_, err := h.Invoke(context.Background(), []byte(`"aW52b2tlZA=="`))
	assertInvokeError(t, err, "Bad Request", "missing function ARN")
}

func TestHandlerMalformedFunctionARN(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: "not an ARN",
	})
	_, err := h.Invoke(ctx, []byte(`"aW52b2tlZDovL2Z1bmN0aW9uOg=="`))
	assertInvokeError(t, err, "Bad Request", "malformed function ARN")
}

func TestHandlerNonLambdaFunctionARN(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: "arn:aws:lambda:us-east-1:123456789012:whatever:my-function",
	})
	_, err := h.Invoke(ctx, []byte(`"aW52b2tlZDovL2Z1bmN0aW9uOg=="`))
	assertInvokeError(t, err, "Bad Request", "function ARN is not a Lambda function ARN: invalid prefix: arn:aws:lambda:us-east-1:123456789012:whatever:my-function")
}

func TestHandlerMissingFunctionVersion(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: "arn:aws:lambda:us-east-1:123456789012:function:my-function",
	})
	_, err := h.Invoke(ctx, []byte(`"aW52b2tlZDovL2Z1bmN0aW9uOg=="`))
	assertInvokeError(t, err, "Bad Request", "function ARN is not a Lambda function ARN: missing version: arn:aws:lambda:us-east-1:123456789012:function:my-function")
}

func TestHandlerInvokePayloadNotProtobufMessage(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	})
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: "arn:aws:lambda:us-east-1:123456789012:function:my-function:1",
	})
	_, err := h.Invoke(ctx, []byte(`"aW52b2tlZDovL2Z1bmN0aW9uOg=="`))
	assertInvokeError(t, err, "Bad Request", "raw payload did not contain a protobuf encoded execution request")
}

func TestHandlerInvokeError(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, errors.New("invoke error")
	})
	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: "arn:aws:lambda:us-east-1:123456789012:function:my-function:1",
	})

	input, err := anypb.New(&wrapperspb.StringValue{Value: "input"})
	if err != nil {
		t.Fatalf("unexpected error creating input: %v", err)
	}

	req := &coroutinev1.ExecuteRequest{
		Coroutine: &coroutinev1.ExecuteRequest_Input{
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

	res := new(coroutinev1.ExecuteResponse)
	if err := proto.Unmarshal(payload[:n], res); err != nil {
		t.Fatalf("unexpected error unmarshaling result: %v", err)
	}
	switch coro := res.Directive.(type) {
	case *coroutinev1.ExecuteResponse_Exit:
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

func TestHandlerInvokeQualifiedFunctionARN(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("output"), nil
	})

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

	req := &coroutinev1.ExecuteRequest{
		Coroutine: &coroutinev1.ExecuteRequest_Input{
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

	res := new(coroutinev1.ExecuteResponse)
	if err := proto.Unmarshal(payload[:n], res); err != nil {
		t.Fatalf("unexpected error unmarshaling result: %v", err)
	}

	if res.CoroutineUri != unqualifiedFunctionARN {
		t.Errorf("expected coroutine to return a result with coroutine URI %q, got %q", unqualifiedFunctionARN, res.CoroutineUri)
	}
	if res.CoroutineVersion != functionVersion {
		t.Errorf("expected coroutine to return a result with coroutine version %q, got %q", functionVersion, res.CoroutineVersion)
	}

	switch coro := res.Directive.(type) {
	case *coroutinev1.ExecuteResponse_Exit:
		out := coro.Exit.GetResult().GetOutput()
		if out.TypeUrl != "type.googleapis.com/google.protobuf.StringValue" {
			t.Errorf("expected coroutine to return an output of type %q, got %q", "type.googleapis.com/google.protobuf.StringValue", out.TypeUrl)
		}
		var output wrapperspb.StringValue
		if err := out.UnmarshalTo(&output); err != nil {
			t.Fatalf("unexpected error unmarshaling output: %v", err)
		}
		if output.Value != "output" {
			t.Errorf("expected coroutine to return an output with value %q, got %q", "output", output.Value)
		}
	default:
		t.Errorf("expected coroutine to return an error, got %T", coro)
	}
}

func TestHandlerInvokeUnqualifiedFunctionARN(t *testing.T) {
	h := dispatchlambda.Handler(func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("output"), nil
	})

	const (
		functionVersion        = "1"
		unqualifiedFunctionARN = "arn:aws:lambda:us-east-1:123456789012:function:my-function"
		qualifiedFunctionARN   = unqualifiedFunctionARN + ":" + functionVersion
	)

	dispatchlambda.SetDispatchCoroutineVersion(functionVersion)

	ctx := lambdacontext.NewContext(context.Background(), &lambdacontext.LambdaContext{
		InvokedFunctionArn: unqualifiedFunctionARN,
	})

	input, err := anypb.New(&wrapperspb.StringValue{Value: "input"})
	if err != nil {
		t.Fatalf("unexpected error creating input: %v", err)
	}

	req := &coroutinev1.ExecuteRequest{
		Coroutine: &coroutinev1.ExecuteRequest_Input{
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

	res := new(coroutinev1.ExecuteResponse)
	if err := proto.Unmarshal(payload[:n], res); err != nil {
		t.Fatalf("unexpected error unmarshaling result: %v", err)
	}

	if res.CoroutineUri != unqualifiedFunctionARN {
		t.Errorf("expected coroutine to return a result with coroutine URI %q, got %q", unqualifiedFunctionARN, res.CoroutineUri)
	}
	if res.CoroutineVersion != functionVersion {
		t.Errorf("expected coroutine to return a result with coroutine version %q, got %q", functionVersion, res.CoroutineVersion)
	}

	switch coro := res.Directive.(type) {
	case *coroutinev1.ExecuteResponse_Exit:
		out := coro.Exit.GetResult().GetOutput()
		if out.TypeUrl != "type.googleapis.com/google.protobuf.StringValue" {
			t.Errorf("expected coroutine to return an output of type %q, got %q", "type.googleapis.com/google.protobuf.StringValue", out.TypeUrl)
		}
		var output wrapperspb.StringValue
		if err := out.UnmarshalTo(&output); err != nil {
			t.Fatalf("unexpected error unmarshaling output: %v", err)
		}
		if output.Value != "output" {
			t.Errorf("expected coroutine to return an output with value %q, got %q", "output", output.Value)
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
