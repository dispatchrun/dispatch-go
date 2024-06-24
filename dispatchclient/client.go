//go:build !durable

package dispatchclient

import (
	"context"
	"fmt"
	"net/http"
	"os"
	_ "unsafe"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/dispatchrun/dispatch-go/internal/env"
)

const defaultApiUrl = "https://api.dispatch.run"

// Client is a client for the Dispatch API.
//
// The Client can be used to dispatch function calls.
type Client struct {
	apiKey        string
	apiKeyFromEnv bool
	apiUrl        string
	env           []string
	httpClient    *http.Client
	opts          []Option

	client sdkv1connect.DispatchServiceClient
}

// New creates a Client.
func New(opts ...Option) (*Client, error) {
	c := &Client{
		env:  os.Environ(),
		opts: opts,
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.apiKey == "" {
		c.apiKey = env.Get(c.env, "DISPATCH_API_KEY")
		c.apiKeyFromEnv = true
	}
	if c.apiKey == "" {
		return nil, fmt.Errorf("Dispatch API key has not been set. Use APIKey(..), or set the DISPATCH_API_KEY environment variable")
	}

	if c.apiUrl == "" {
		c.apiUrl = env.Get(c.env, "DISPATCH_API_URL")
	}
	if c.apiUrl == "" {
		c.apiUrl = defaultApiUrl
	}

	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}

	authenticator := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		authorization := "Bearer " + c.apiKey
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Add("Authorization", authorization)
			return next(ctx, req)
		}
	})

	validator, err := validate.NewInterceptor()
	if err != nil {
		return nil, err
	}

	c.client = sdkv1connect.NewDispatchServiceClient(c.httpClient, c.apiUrl,
		connect.WithInterceptors(validator, authenticator))

	return c, nil
}

// Option configures a Client.
type Option func(*Client)

// APIKey sets the Dispatch API key to use for authentication when
// dispatching function calls through a Client.
//
// It defaults to the value of the DISPATCH_API_KEY environment variable.
func APIKey(apiKey string) Option {
	return func(c *Client) { c.apiKey = apiKey }
}

// APIUrl sets the URL of the Dispatch API.
//
// It defaults to the value of the DISPATCH_API_URL environment variable,
// or the default API URL (https://api.dispatch.run) if DISPATCH_API_URL
// is unset.
func APIUrl(apiUrl string) Option {
	return func(c *Client) { c.apiUrl = apiUrl }
}

// Env sets the environment variables that a Client parses its
// default configuration from.
//
// It defaults to os.Environ().
func Env(env ...string) Option {
	return func(c *Client) { c.env = env }
}

// Dispatch dispatches a function call.
func (c *Client) Dispatch(ctx context.Context, call dispatchproto.Call) (dispatchproto.ID, error) {
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
	return Batch{client: c}
}

// Batch is used to submit a batch of function calls to Dispatch.
type Batch struct {
	client *Client

	calls []*sdkv1.Call
}

// Reset resets the batch.
func (b *Batch) Reset() {
	clear(b.calls)
	b.calls = b.calls[:0]
}

// Add adds calls to the batch.
func (b *Batch) Add(calls ...dispatchproto.Call) {
	for i := range calls {
		b.calls = append(b.calls, callProto(calls[i]))
	}
}

//go:linkname callProto github.com/dispatchrun/dispatch-go/dispatchproto.callProto
func callProto(r dispatchproto.Call) *sdkv1.Call

// Dispatch dispatches the batch of function calls.
func (b *Batch) Dispatch(ctx context.Context) ([]dispatchproto.ID, error) {
	req := connect.NewRequest(&sdkv1.DispatchRequest{Calls: b.calls})
	res, err := b.client.client.Dispatch(ctx, req)
	if err != nil {
		if connect.CodeOf(err) == connect.CodeUnauthenticated {
			if b.client.apiKeyFromEnv {
				return nil, fmt.Errorf("invalid DISPATCH_API_KEY: %s", redactAPIKey(b.client.apiKey))
			}
			return nil, fmt.Errorf("invalid Dispatch API key provided with APIKey(..): %s", redactAPIKey(b.client.apiKey))
		}
		return nil, err
	}
	ids := make([]dispatchproto.ID, len(res.Msg.DispatchIds))
	for i, id := range res.Msg.DispatchIds {
		ids[i] = dispatchproto.ID(id)
	}
	return ids, nil
}

func redactAPIKey(s string) string {
	if len(s) <= 3 {
		// Don't redact the string if it's this short. It's not a valid API
		// key if so, and even if it was it would be easy to brute force and so
		// redaction would not serve a purpose. The idea is that we show a bit
		// of the API key to help the user fix an issue.
		return s
	}
	return s[:3] + "********"
}
