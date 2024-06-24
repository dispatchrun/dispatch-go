//go:build !durable

package dispatchtest

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchserver"
)

// NewEndpoint creates a Dispatch endpoint, like dispatch.New.
//
// Unlike dispatch.New, it starts a test server that serves the endpoint
// and automatically sets the endpoint URL.
func NewEndpoint(opts ...dispatch.Option) (*dispatch.Dispatch, *EndpointServer, error) {
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
func (e *EndpointServer) Client(opts ...dispatchserver.EndpointClientOption) (*dispatchserver.EndpointClient, error) {
	return dispatchserver.NewEndpointClient(e.server.URL, opts...)
}

// URL is the URL of the server.
func (e *EndpointServer) URL() string {
	return e.server.URL
}

// Close closes the server.
func (e *EndpointServer) Close() {
	e.server.Close()
}

// SigningKey sets the signing key to use when signing requests bound
// for the endpoint.
//
// The signing key should be a base64-encoded ed25519.PrivateKey, e.g.
// one provided by the KeyPair helper function.
func SigningKey(signingKey string) dispatchserver.EndpointClientOption {
	pk, err := base64.StdEncoding.DecodeString(signingKey)
	if err != nil || len(pk) != ed25519.PrivateKeySize {
		panic(fmt.Errorf("invalid signing key: %v", signingKey))
	}
	return dispatchserver.SigningKey(pk)
}
