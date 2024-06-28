//go:build !durable

package dispatchhttp

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Response is an HTTP response.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// FromResponse creates a Response from an http.Response.
//
// The http.Response.Body is consumed and closed by this
// operation.
func FromResponse(r *http.Response) (*Response, error) {
	if r == nil {
		return nil, nil
	}

	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	return &Response{
		StatusCode: r.StatusCode,
		Header:     cloneHeader(r.Header),
		Body:       b,
	}, nil
}

func (r *Response) MarshalJSON() ([]byte, error) {
	// Indirection is required to avoid an infinite loop.
	return json.Marshal(jsonResponse{
		StatusCode: r.StatusCode,
		Header:     r.Header,
		Body:       r.Body,
	})
}

func (r *Response) UnmarshalJSON(b []byte) error {
	var jr jsonResponse
	if err := json.Unmarshal(b, &jr); err != nil {
		return err
	}
	r.StatusCode = jr.StatusCode
	r.Header = jr.Header
	r.Body = jr.Body
	return nil
}

type jsonResponse struct {
	StatusCode int         `json:"status_code,omitempty"`
	Header     http.Header `json:"header,omitempty"`
	Body       []byte      `json:"body,omitempty"`
}

// Status is the status for the response.
func (r *Response) Status() dispatchproto.Status {
	return statusCodeStatus(r.StatusCode)
}

func statusCodeStatus(statusCode int) dispatchproto.Status {
	// Keep in sync with https://github.com/dispatchrun/dispatch-py/blob/main/src/dispatch/integrations/http.py
	switch statusCode {
	case http.StatusBadRequest: // 400
		return dispatchproto.InvalidArgumentStatus
	case http.StatusUnauthorized: // 401
		return dispatchproto.UnauthenticatedStatus
	case http.StatusForbidden: // 403
		return dispatchproto.PermissionDeniedStatus
	case http.StatusNotFound: // 404
		return dispatchproto.NotFoundStatus
	case http.StatusRequestTimeout: // 408
		return dispatchproto.TimeoutStatus
	case http.StatusTooManyRequests: // 429
		return dispatchproto.ThrottledStatus
	case http.StatusNotImplemented: // 501
		return dispatchproto.PermanentErrorStatus
	}

	switch statusCode / 100 {
	case 1: // 1xx informational
		return dispatchproto.PermanentErrorStatus
	case 2: // 2xx success
		return dispatchproto.OKStatus
	case 3: // 3xx redirect
		return dispatchproto.PermanentErrorStatus
	case 4: // 4xx client error
		return dispatchproto.PermanentErrorStatus
	case 5: // 5xx server error
		return dispatchproto.TemporaryErrorStatus
	}

	return dispatchproto.UnspecifiedStatus
}
