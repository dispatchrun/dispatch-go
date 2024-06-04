package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"

	"buf.build/gen/go/stealthrocket/dispatch-proto/connectrpc/go/dispatch/sdk/v1/sdkv1connect"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"connectrpc.com/connect"
	"connectrpc.com/validate"
)

// DefaultApiUrl is the default Dispatch API URL.
const DefaultApiUrl = "https://api.dispatch.run"

// Client is a client for Dispatch.
type Client struct {
	// ApiKey is the Dispatch API key to use for authentication when
	// dispatching calls to functions. If omitted, the the value of the
	// DISPATCH_API_KEY environment variable is used.
	ApiKey string

	// ApiUrl is the URL of the Dispatch API to use when dispatching calls
	// to functions. If omitted, the value of the DISPATCH_API_URL
	// environment variable is used. If both are unset/empty, the default URL
	// (DefaultApiUrl) is used.
	ApiUrl string

	// Env are environment variables to parse configuration from.
	// If nil, environment variables are read from os.Environ().
	Env []string

	// Client is the HTTP client to use when making requests to Dispatch.
	// If nil, http.DefaultClient is used.
	Client *http.Client

	client sdkv1connect.DispatchServiceClient
	err    error
	mu     sync.Mutex
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
	return Batch{client: c}
}

func (c *Client) dispatchClient() (sdkv1connect.DispatchServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return nil, c.err
	}
	if c.client != nil {
		return c.client, nil
	}

	apiKey := c.ApiKey
	if apiKey == "" {
		envApiKey, ok := getenv(c.Env, "DISPATCH_API_KEY")
		if !ok || envApiKey == "" {
			c.err = fmt.Errorf("Dispatch API key not found. Check DISPATCH_API_KEY")
			return nil, c.err
		}
		apiKey = envApiKey
	}

	apiUrl := c.ApiUrl
	if apiUrl == "" {
		envApiUrl, ok := getenv(c.Env, "DISPATCH_API_URL")
		if ok && envApiUrl != "" {
			apiUrl = envApiUrl
		} else {
			apiUrl = DefaultApiUrl
		}
	}

	slog.Info("configuring Dispatch client", "api_url", apiUrl, "api_key", redact(apiKey))

	authenticatingInterceptor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Add("Authorization", "Bearer "+apiKey)
			return next(ctx, req)
		}
	})

	validatingInterceptor, err := validate.NewInterceptor()
	if err != nil {
		c.err = err
		return nil, err
	}

	c.client = sdkv1connect.NewDispatchServiceClient(c.httpClient(), apiUrl,
		connect.WithInterceptors(validatingInterceptor, authenticatingInterceptor))

	return c.client, nil
}

func (c *Client) httpClient() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
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

func redact(s string) string {
	if len(s) < 4 {
		return s
	}
	return s[:4] + strings.Repeat("*", len(s)-4)
}

// Batch is used to submit a batch of function calls to Dispatch.
type Batch struct {
	client *Client

	calls []*sdkv1.Call
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.calls = b.calls[:0]
}

// Add adds a Call to the batch.
func (b *Batch) Add(call Call) {
	b.calls = append(b.calls, call.proto())
}

// Dispatch dispatches the batch of function calls.
func (b *Batch) Dispatch(ctx context.Context) ([]ID, error) {
	client, err := b.client.dispatchClient()
	if err != nil {
		return nil, err
	}
	req := connect.NewRequest(&sdkv1.DispatchRequest{Calls: b.calls})
	res, err := client.Dispatch(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Msg.DispatchIds, nil
}
