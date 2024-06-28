//go:build !durable

package dispatchhttp

import (
	"bytes"
	"context"
	"net/http"
)

// Client wraps an http.Client to accept Request instances
// and return Response instances.
type Client struct{ Client *http.Client }

// DefaultClient is the default client.
var DefaultClient = &Client{Client: http.DefaultClient}

// Get makes an HTTP GET request to the specified URL and returns
// its Response.
func (c *Client) Get(ctx context.Context, url string) (*Response, error) {
	req := &Request{Method: "GET", URL: url}
	return c.Do(ctx, req)
}

// Get makes an HTTP GET request to the specified URL and returns
// its Response.
func Get(ctx context.Context, url string) (*Response, error) {
	return DefaultClient.Get(ctx, url)
}

// Do makes a HTTP Request and returns its Response.
func (c *Client) Do(ctx context.Context, r *Request) (*Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, r.Method, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}
	copyHeader(httpReq.Header, r.Header)

	httpRes, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	return FromResponse(httpRes)
}
