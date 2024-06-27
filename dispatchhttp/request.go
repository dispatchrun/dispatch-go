package dispatchhttp

import (
	"encoding/json"
	"net/http"
)

// Request is an HTTP request.
type Request struct {
	Method string
	URL    string
	Header http.Header
	Body   []byte
}

func (r *Request) MarshalJSON() ([]byte, error) {
	// Indirection is required to avoid an infinite loop.
	return json.Marshal(jsonRequest{
		Method: r.Method,
		URL:    r.URL,
		Header: r.Header,
		Body:   r.Body,
	})
}

func (r *Request) UnmarshalJSON(b []byte) error {
	var jr jsonRequest
	if err := json.Unmarshal(b, &jr); err != nil {
		return err
	}
	r.Method = jr.Method
	r.URL = jr.URL
	r.Header = jr.Header
	r.Body = jr.Body
	return nil
}

type jsonRequest struct {
	Method string      `json:"method,omitempty"`
	URL    string      `json:"url,omitempty"`
	Header http.Header `json:"header,omitempty"`
	Body   []byte      `json:"body,omitempty"`
}
