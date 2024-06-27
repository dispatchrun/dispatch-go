package dispatchhttp_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/dispatchrun/dispatch-go/dispatchhttp"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/google/go-cmp/cmp"
)

func TestSerializable(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		req := &dispatchhttp.Request{
			Method: "GET",
			URL:    "http://example.com",
			Header: http.Header{"X-Foo": []string{"bar"}},
			Body:   []byte("abc"),
		}
		boxed, err := dispatchproto.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		var req2 *dispatchhttp.Request
		if err := boxed.Unmarshal(&req2); err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(req, req2); diff != "" {
			t.Errorf("invalid request: %v", diff)
		}
	})

	t.Run("response", func(t *testing.T) {
		res := &dispatchhttp.Response{
			StatusCode: 200,
			Header:     http.Header{"X-Foo": []string{"bar"}},
			Body:       []byte("abc"),
		}
		boxed, err := dispatchproto.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		var res2 *dispatchhttp.Response
		if err := boxed.Unmarshal(&res2); err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(res, res2); diff != "" {
			t.Errorf("invalid response: %v", diff)
		}
	})
}

func TestStatusCodeStatus(t *testing.T) {
	for _, test := range []struct {
		code int
		want dispatchproto.Status
	}{
		// 1xx
		{
			code: http.StatusContinue,
			want: dispatchproto.PermanentErrorStatus,
		},

		// 2xx
		{
			code: http.StatusOK,
			want: dispatchproto.OKStatus,
		},
		{
			code: http.StatusAccepted,
			want: dispatchproto.OKStatus,
		},
		{
			code: http.StatusCreated,
			want: dispatchproto.OKStatus,
		},

		// 3xx
		{
			code: http.StatusTemporaryRedirect,
			want: dispatchproto.PermanentErrorStatus,
		},
		{
			code: http.StatusPermanentRedirect,
			want: dispatchproto.PermanentErrorStatus,
		},

		// 4xx
		{
			code: http.StatusBadRequest,
			want: dispatchproto.InvalidArgumentStatus,
		},
		{
			code: http.StatusUnauthorized,
			want: dispatchproto.UnauthenticatedStatus,
		},
		{
			code: http.StatusForbidden,
			want: dispatchproto.PermissionDeniedStatus,
		},
		{
			code: http.StatusNotFound,
			want: dispatchproto.NotFoundStatus,
		},
		{
			code: http.StatusMethodNotAllowed,
			want: dispatchproto.PermanentErrorStatus,
		},
		{
			code: http.StatusRequestTimeout,
			want: dispatchproto.TimeoutStatus,
		},
		{
			code: http.StatusTooManyRequests,
			want: dispatchproto.ThrottledStatus,
		},

		// 5xx
		{
			code: http.StatusInternalServerError,
			want: dispatchproto.TemporaryErrorStatus,
		},
		{
			code: http.StatusNotImplemented,
			want: dispatchproto.PermanentErrorStatus,
		},
		{
			code: http.StatusBadGateway,
			want: dispatchproto.TemporaryErrorStatus,
		},
		{
			code: http.StatusServiceUnavailable,
			want: dispatchproto.TemporaryErrorStatus,
		},
		{
			code: http.StatusGatewayTimeout,
			want: dispatchproto.TemporaryErrorStatus,
		},

		// invalid
		{
			code: 0,
			want: dispatchproto.UnspecifiedStatus,
		},
		{
			code: 9999,
			want: dispatchproto.UnspecifiedStatus,
		},
	} {
		t.Run(strconv.Itoa(test.code), func(t *testing.T) {
			res := &dispatchhttp.Response{StatusCode: test.code}
			got := dispatchproto.StatusOf(res)
			if got != test.want {
				t.Errorf("unexpected status for code %d: got %v, want %v", test.code, got, test.want)
			}
		})
	}
}
