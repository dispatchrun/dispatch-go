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

	// ApiUrl is the URL of Dispatch. If omitted,
	//
	// ApiURL is the URL of the Dispatch API to use when dispatching calls
	// to functions. If omitted, the value of the DISPATCH_API_URL
	// environment variable is used. If that is unset, the default URL
	// (DefaultApiUrl) is used.
	ApiUrl string

	// Env are environment variables to parse configuration
	// from. If nil, environment variables are read from
	// os.Environ().
	Env []string

	// Client is the HTTP client to use when making requests to Dispatch.
	// If nil, http.DefaultClient is used instead.
	Client *http.Client

	client sdkv1connect.DispatchServiceClient
	err    error
	mu     sync.Mutex
}

// Dispatch dispatches a batch of function calls.
func (c *Client) Dispatch(ctx context.Context, calls []*sdkv1.Call) ([]DispatchID, error) {
	client, err := c.dispatchClient()
	if err != nil {
		return nil, err
	}
	req := connect.NewRequest(&sdkv1.DispatchRequest{Calls: calls})
	res, err := client.Dispatch(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Msg.DispatchIds, nil
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
