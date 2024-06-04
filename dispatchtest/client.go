package dispatchtest

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"sync"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go/internal/auth"
)

// EndpointClient is a client for a Dispatch endpoint.
//
// Note that this is not the same as dispatch.Client, which
// is a client for the Dispatch API. The client here is
// useful when testing an endpoint.
type EndpointClient struct {
	// EndpointUrl is the URL of the endpoint to connect to.
	EndpointUrl string

	// Client is the client to use when making HTTP requets.
	// By default, http.DefaultClient is used.
	Client connect.HTTPClient

	// SigningKey is an optional signing key to use when signing
	// outbound HTTP requests. If the key is omitted, requests are
	// not signed.
	SigningKey ed25519.PrivateKey

	client sdkv1connect.FunctionServiceClient
	err    error
	mu     sync.Mutex
}

// Run sends a RunRequest and returns a RunResponse.
func (c *EndpointClient) Run(ctx context.Context, req *sdkv1.RunRequest) (*sdkv1.RunResponse, error) {
	client, err := c.endpointClient()
	if err != nil {
		return nil, err
	}
	res, err := client.Run(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

func (c *EndpointClient) endpointClient() (sdkv1connect.FunctionServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return nil, c.err
	}
	if c.client != nil {
		return c.client, nil
	}

	httpClient := c.Client
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if c.SigningKey != nil {
		signer := auth.NewSigner(c.SigningKey)
		httpClient = signer.Client(httpClient)
	}

	validatingInterceptor, err := validate.NewInterceptor()
	if err != nil {
		c.err = err
		return nil, err
	}
	c.client = sdkv1connect.NewFunctionServiceClient(httpClient, c.EndpointUrl,
		connect.WithInterceptors(validatingInterceptor))
	return c.client, nil
}
