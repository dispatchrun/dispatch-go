//go:build !durable

package dispatchhttp

import (
	"net/http"
	"slices"
)

func cloneHeader(h http.Header) http.Header {
	c := make(http.Header, len(h))
	copyHeader(c, h)
	return c
}

func copyHeader(dst, src http.Header) {
	for name, values := range src {
		dst[name] = slices.Clone(values)
	}
}
