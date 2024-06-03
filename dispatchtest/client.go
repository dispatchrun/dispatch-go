package dispatchtest

import (
	"context"
	"crypto/ed25519"
	"net/http"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go/internal/auth"
)

// EndpointClient is a client for a Dispatch endpoint.
//
// Note that this is not the same as dispatch.Client, which
// is a client for the Dispatch API. The client here is
// useful when testing an endpoint.
type EndpointClient struct {
	httpClient connect.HTTPClient
	signingKey ed25519.PrivateKey

	client sdkv1connect.FunctionServiceClient
}

// NewEndpointClient creates an EndpointClient.
func NewEndpointClient(baseURL string, opts ...endpointClientOption) *EndpointClient {
	c := &EndpointClient{}
	for _, opt := range opts {
		opt(c)
	}

	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}
	if c.signingKey != nil {
		signer := auth.NewSigner(c.signingKey)
		c.httpClient = signer.Client(c.httpClient)
	}
	c.client = sdkv1connect.NewFunctionServiceClient(c.httpClient, baseURL)
	return c
}

type endpointClientOption func(*EndpointClient)

// WithClient configures an EndpointClient to make HTTP
// requests using the specified HTTP client.
//
// By default, http.DefaultClient is used.
func WithClient(client *http.Client) endpointClientOption {
	return endpointClientOption(func(c *EndpointClient) { c.httpClient = client })
}

// WithSigningKey configures an EndpointClient to sign
// requests in the same way that Dispatch would, using
// the specified ed25519 private key.
//
// By default, requests are not signed.
func WithSigningKey(signingKey ed25519.PrivateKey) endpointClientOption {
	return endpointClientOption(func(c *EndpointClient) { c.signingKey = signingKey })
}

// Run sends a RunRequest and returns a RunResponse.
func (c *EndpointClient) Run(ctx context.Context, req *sdkv1.RunRequest) (*sdkv1.RunResponse, error) {
	res, err := c.client.Run(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}
