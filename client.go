package dispatch

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
)

// Client is a client for the Dispatch API.
//
// The Client can be used to dispatch function calls.
type Client struct {
	apiKey     string
	apiUrl     string
	env        []string
	httpClient *http.Client

	client sdkv1connect.DispatchServiceClient
}

// NewClient creates a Client.
func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{env: os.Environ()}
	for _, opt := range opts {
		opt(c)
	}

	if c.apiKey == "" {
		c.apiKey, _ = getenv(c.env, "DISPATCH_API_KEY")
	}
	if c.apiKey == "" {
		return nil, fmt.Errorf("API key has not been set. Use WithAPIKey(..), or set the DISPATCH_API_KEY environment variable")
	}

	if c.apiUrl == "" {
		c.apiUrl, _ = getenv(c.env, "DISPATCH_API_URL")
	}
	if c.apiUrl == "" {
		c.apiUrl = DefaultApiUrl
	}

	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}

	authenticatingInterceptor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		authorization := "Bearer " + c.apiKey
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Add("Authorization", authorization)
			return next(ctx, req)
		}
	})

	validatingInterceptor, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}

	c.client = sdkv1connect.NewDispatchServiceClient(c.httpClient, c.apiUrl,
		connect.WithInterceptors(validatingInterceptor, authenticatingInterceptor))

	return c, nil
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithAPIKey sets the Dispatch API key to use for authentication when
// dispatching function calls through a Client.
//
// It defaults to the value of the DISPATCH_API_KEY environment variable.
func WithAPIKey(apiKey string) ClientOption {
	return func(c *Client) { c.apiKey = apiKey }
}

// WithAPIUrl sets the URL of the Dispatch API.
//
// It defaults to the value of the DISPATCH_API_URL environment variable,
// or DefaultApiUrl if DISPATCH_API_URL is unset.
func WithAPIUrl(apiUrl string) ClientOption {
	return func(c *Client) { c.apiUrl = apiUrl }
}

// DefaultApiUrl is the default Dispatch API URL.
const DefaultApiUrl = "https://api.dispatch.run"

// WithClientEnv sets the environment variables that a Client parses
// default configuration from.
//
// It defaults to os.Environ().
func WithClientEnv(env []string) ClientOption {
	return func(c *Client) { c.env = env }
}

// Dispatch dispatches a function call.
func (c *Client) Dispatch(ctx context.Context, call Call) (ID, error) {
	batch := c.Batch()
	batch.Add(call)
	ids, err := batch.Dispatch(ctx)
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

// Batch creates a Batch.
func (c *Client) Batch() Batch {
	return Batch{client: c.client}
}

// Batch is used to submit a batch of function calls to Dispatch.
type Batch struct {
	client sdkv1connect.DispatchServiceClient

	calls []*sdkv1.Call
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.calls = b.calls[:0]
}

// Add adds calls to the batch.
func (b *Batch) Add(calls ...Call) {
	for i := range calls {
		b.calls = append(b.calls, calls[i].proto())
	}
}

// Dispatch dispatches the batch of function calls.
func (b *Batch) Dispatch(ctx context.Context) ([]ID, error) {
	req := connect.NewRequest(&sdkv1.DispatchRequest{Calls: b.calls})
	res, err := b.client.Dispatch(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Msg.DispatchIds, nil
}

func getenv(env []string, name string) (string, bool) {
	if env == nil {
		env = os.Environ()
	}
	for _, s := range env {
		n, v, ok := strings.Cut(s, "=")
		if ok && n == name {
			return v, true
		}
	}
	return "", false
}
