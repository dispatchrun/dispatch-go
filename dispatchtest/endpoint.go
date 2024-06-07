package dispatchtest

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	_ "unsafe"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/internal/auth"
)

// NewEndpoint creates a Dispatch endpoint, like dispatch.New.
//
// Unlike dispatch.New, it starts a test server that serves the endpoint
// and automatically sets the endpoint URL.
func NewEndpoint(opts ...dispatch.DispatchOption) (*dispatch.Dispatch, *EndpointServer, error) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)

	opts = append(opts, dispatch.EndpointUrl(server.URL))
	endpoint, err := dispatch.New(opts...)
	if err != nil {
		server.Close()
		return nil, nil, err
	}

	mux.Handle(endpoint.Handler())

	return endpoint, &EndpointServer{server}, nil
}

// EndpointServer is a server serving a Dispatch endpoint.
type EndpointServer struct {
	server *httptest.Server
}

// Client returns a client that can be used to interact with the
// Dispatch endpoint.
func (e *EndpointServer) Client(opts ...EndpointClientOption) (*EndpointClient, error) {
	return NewEndpointClient(e.server.URL, opts...)
}

// URL is the URL of the server.
func (e *EndpointServer) URL() string {
	return e.server.URL
}

// Close closes the server.
func (e *EndpointServer) Close() {
	e.server.Close()
}

// EndpointClient is a client for a Dispatch endpoint.
//
// Note that this is not the same as dispatch.Client, which
// is a client for the Dispatch API. The client here is
// useful when testing a Dispatch endpoint.
type EndpointClient struct {
	signingKey string

	client sdkv1connect.FunctionServiceClient
}

// NewEndpointClient creates an EndpointClient.
func NewEndpointClient(endpointUrl string, opts ...EndpointClientOption) (*EndpointClient, error) {
	c := &EndpointClient{}
	for _, opt := range opts {
		opt(c)
	}

	// Setup request signing.
	var httpClient connect.HTTPClient = http.DefaultClient
	if c.signingKey != "" {
		privateKey, err := base64.StdEncoding.DecodeString(c.signingKey)
		if err != nil || len(privateKey) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("invalid signing key: %v", c.signingKey)
		}
		signer := auth.NewSigner(ed25519.PrivateKey(privateKey))
		httpClient = signer.Client(httpClient)
	}

	// Setup the gRPC client.
	validator, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}
	c.client = sdkv1connect.NewFunctionServiceClient(httpClient, endpointUrl, connect.WithInterceptors(validator))

	return c, nil
}

// EndpointClientOption configures an EndpointClient.
type EndpointClientOption func(*EndpointClient)

// SigningKey sets the signing key to use when signing requests bound
// for the endpoint.
//
// The signing key should be a base64-encoded ed25519.PrivateKey, e.g.
// one provided by the KeyPair helper function.
//
// By default the EndpointClient does not sign requests to the endpoint.
func SigningKey(signingKey string) EndpointClientOption {
	return func(c *EndpointClient) { c.signingKey = signingKey }
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
