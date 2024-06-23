package dispatchlambda

import (
	"context"
	"encoding/base64"
	_ "unsafe"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"google.golang.org/protobuf/proto"
)

// Start is a shortcut to start a Lambda function handler executing the given
// Dispatch functions when invoked.
func Start(functions ...dispatch.AnyFunction) {
	lambda.Start(Handler(functions...))
}

// Handler creates a lambda function handler executing the given
// Dispatch functions when invoked.
func Handler(functions ...dispatch.AnyFunction) lambda.Handler {
	handler := &handler{functions: dispatchproto.FunctionMap{}}
	for _, fn := range functions {
		name, primitive := fn.Register(nil)
		handler.functions[name] = primitive
	}
	return handler
}

type handler struct {
	functions dispatchproto.FunctionMap
}

func (h *handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, badRequest("empty payload")
	}
	if len(payload) < 2 {
		return nil, badRequest("payload is too short")
	}
	if payload[0] != '"' || payload[len(payload)-1] != '"' {
		return nil, badRequest("payload is not a string")
	}
	payload = payload[1 : len(payload)-1]

	rawPayload := make([]byte, base64.StdEncoding.DecodedLen(len(payload)))
	n, err := base64.StdEncoding.Decode(rawPayload, payload)
	if err != nil {
		return nil, badRequest("payload is not base64 encoded")
	}

	req := new(sdkv1.RunRequest)
	if err := proto.Unmarshal(rawPayload[:n], req); err != nil {
		return nil, badRequest("raw payload did not contain a protobuf encoded execution request")
	}

	res := h.functions.Run(ctx, newProtoRequest(req))

	rawResponse, err := proto.Marshal(responseProto(res))
	if err != nil {
		return nil, err
	}

	rawPayload = make([]byte, 2+base64.StdEncoding.EncodedLen(len(rawResponse)))
	i := len(rawPayload) - 1
	rawPayload[0] = '"'
	rawPayload[i] = '"'
	base64.StdEncoding.Encode(rawPayload[1:i], rawResponse)
	return rawPayload, nil
}

func badRequest(msg string) messages.InvokeResponse_Error {
	return messages.InvokeResponse_Error{
		Type:    "Bad Request",
		Message: msg,
	}
}

//go:linkname newProtoRequest github.com/dispatchrun/dispatch-go/dispatchproto.newProtoRequest
func newProtoRequest(r *sdkv1.RunRequest) dispatchproto.Request

//go:linkname responseProto github.com/dispatchrun/dispatch-go/dispatchproto.responseProto
func responseProto(r dispatchproto.Response) *sdkv1.RunResponse
