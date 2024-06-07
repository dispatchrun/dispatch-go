package dispatchserver

import (
	"context"
	"crypto/ed25519"
	"net/http"
	_ "unsafe"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/internal/auth"
)

// EndpointClient is a client for a Dispatch endpoint.
//
// Note that this is not the same as dispatch.Client, which
// is a client for the Dispatch API. The client here is
// useful when testing a Dispatch endpoint.
type EndpointClient struct {
	httpClient connect.HTTPClient
	signingKey ed25519.PrivateKey

	client sdkv1connect.FunctionServiceClient
}

// NewEndpointClient creates an EndpointClient.
func NewEndpointClient(endpointUrl string, opts ...EndpointClientOption) (*EndpointClient, error) {
	c := &EndpointClient{}
	for _, opt := range opts {
		opt(c)
	}

	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}

	// Setup request signing.
	if c.signingKey != nil {
		signer := auth.NewSigner(c.signingKey)
		c.httpClient = signer.Client(c.httpClient)
	}

	// Setup the gRPC client.
	validator, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}
	c.client = sdkv1connect.NewFunctionServiceClient(c.httpClient, endpointUrl, connect.WithInterceptors(validator))

	return c, nil
}

// EndpointClientOption configures an EndpointClient.
type EndpointClientOption func(*EndpointClient)

// SigningKey sets the signing key to use when signing requests bound
// for the endpoint.
//
// By default the EndpointClient does not sign requests to the endpoint.
func SigningKey(signingKey ed25519.PrivateKey) EndpointClientOption {
	return func(c *EndpointClient) { c.signingKey = signingKey }
}

// HTTPClient sets the HTTP client to use when making requests to the endpoint.
//
// By default http.DefaultClient is used.
func HTTPClient(client connect.HTTPClient) EndpointClientOption {
	return func(c *EndpointClient) { c.httpClient = client }
}

// Run sends a RunRequest and returns a RunResponse.
func (c *EndpointClient) Run(ctx context.Context, req dispatch.Request) (dispatch.Response, error) {
	res, err := c.client.Run(ctx, connect.NewRequest(requestProto(req)))
	if err != nil {
		return dispatch.Response{}, err
	}
	return newProtoResponse(res.Msg), nil
}

//go:linkname newProtoResponse github.com/dispatchrun/dispatch-go.newProtoResponse
func newProtoResponse(r *sdkv1.RunResponse) dispatch.Response

//go:linkname requestProto github.com/dispatchrun/dispatch-go.requestProto
func requestProto(r dispatch.Request) *sdkv1.RunRequest
