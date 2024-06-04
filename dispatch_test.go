package dispatch_test

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestDispatch(t *testing.T) {
	verificationKey, signingKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	d := &dispatch.Dispatch{
		VerificationKey: base64.StdEncoding.EncodeToString(verificationKey[:]),
	}

	d.Register(dispatch.NewPrimitiveFunction("identity", func(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
		var input *anypb.Any
		switch d := req.Directive.(type) {
		case *sdkv1.RunRequest_Input:
			input = d.Input
		default:
			return dispatch.ErrorResponse(dispatch.InvalidArgumentStatus, fmt.Errorf("unexpected run directive: %T", d))
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

	// Setup the server that serves the Dispatch handler.
	path, handler, err := d.Handler()
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewUnstartedServer(mux)
	defer server.Close()
	server.Start()

	d.EndpointUrl = server.URL

	client := dispatchtest.NewEndpointClient(server.URL, dispatchtest.WithSigningKey(signingKey))

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
	nonSigningClient := dispatchtest.NewEndpointClient(server.URL)
	_, err = nonSigningClient.Run(context.Background(), &sdkv1.RunRequest{
		Function:  "identity",
		Directive: &sdkv1.RunRequest_Input{Input: input},
	})
	if err == nil || connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("expected a permission denied error, got %v", err)
	}
}
