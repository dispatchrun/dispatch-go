package dispatchlambda

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/stealthrocket/dispatch/sdk/dispatch-go"
	coroutinev1 "github.com/stealthrocket/ring/proto/go/ring/coroutine/v1"
	"google.golang.org/protobuf/proto"
)

// Handler creates a lambda function handler executing the given dispatch
// function when invoked.
func Handler[Input, Output proto.Message](f dispatch.Function[Input, Output]) lambda.Handler {
	return handlerFunc[Input, Output](f)
}

type handlerFunc[Input, Output proto.Message] dispatch.Function[Input, Output]

func (h handlerFunc[Input, Output]) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
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

	lambdaContext, ok := lambdacontext.FromContext(ctx)
	if !ok {
		lambdaContext = new(lambdacontext.LambdaContext)
	}
	if lambdaContext.InvokedFunctionArn == "" {
		return nil, badRequest("missing function ARN")
	}
	functionArn, err := arn.Parse(lambdaContext.InvokedFunctionArn)
	if err != nil {
		return nil, badRequest("malformed function ARN")
	}
	if !strings.HasPrefix(functionArn.Resource, "function:") {
		return nil, badRequest("function ARN is not a Lambda function ARN: invalid prefix: " + functionArn.String())
	}
	functionName := strings.TrimPrefix(functionArn.Resource, "function:")
	_, functionVersion, ok := strings.Cut(functionName, ":")
	if !ok {
		// Special case used when running the function as a local docker image.
		//
		// TODO: this is somewhat hacky, we should find a better way to do it.
		if functionName != "test_function" {
			return nil, badRequest("function ARN is not a Lambda function ARN: missing version: " + functionArn.String())
		}
	}

	req := new(coroutinev1.ExecuteRequest)
	if err := proto.Unmarshal(rawPayload[:n], req); err != nil {
		return nil, badRequest("raw payload did not contain a protobuf encoded execution request")
	}

	// Those fields are ignored in the lambda dispatch handler, the Lambda
	// function is the source of thruth defining the coroutine ID and version.
	req.CoroutineVersion = functionVersion
	req.CoroutineId = functionArn.String()
	req.CoroutineId = strings.TrimSuffix(req.CoroutineId, functionVersion)
	req.CoroutineId = strings.TrimSuffix(req.CoroutineId, ":")

	r, err := dispatch.Function[Input, Output](h).Execute(ctx, req)
	if err != nil {
		r = &coroutinev1.ExecuteResponse{
			Coroutine: &coroutinev1.ExecuteResponse_Error{
				Error: &coroutinev1.Error{
					Type:    "invoke",
					Message: err.Error(),
				},
			},
		}
	}

	// We don't need to pass these values back in the response, they are already
	// known by the invoking client: the coroutine ID is the Lambda function ARN
	// and the version is returned in the ExecutedVersion field of the response.
	r.CoroutineId, r.CoroutineVersion = "", ""

	rawResponse, err := proto.Marshal(r)
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
