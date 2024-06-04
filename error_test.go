package dispatch

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorStatusOf(t *testing.T) {
	tests := []struct {
		error  error
		status Status
	}{
		{nil, OKStatus},
		{ErrTimeout, TimeoutStatus},
		{ErrThrottled, ThrottledStatus},
		{ErrInvalidArgument, InvalidArgumentStatus},
		{ErrInvalidResponse, InvalidResponseStatus},
		{ErrTemporary, TemporaryErrorStatus},
		{ErrPermanent, PermanentErrorStatus},
		{ErrIncompatibleState, IncompatibleStateStatus},
		{ErrDNS, DNSErrorStatus},
		{ErrTCP, TCPErrorStatus},
		{ErrTLS, TLSErrorStatus},
		{ErrHTTP, HTTPErrorStatus},
		{ErrUnauthenticated, UnauthenticatedStatus},
		{ErrPermissionDenied, PermissionDeniedStatus},
		{ErrNotFound, NotFoundStatus},
		{temporaryError{}, TemporaryErrorStatus},
		{timeoutError{}, TimeoutStatus},
		{errors.New("permanent"), PermanentErrorStatus},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.error), func(t *testing.T) {
			if status := errorStatusOf(test.error); status != test.status {
				t.Errorf("errorStatusOf(%q) = %v, want %v", test.error, status, test.status)
			}
		})
	}
}

type temporaryError struct{}

func (temporaryError) Error() string   { return "temporary" }
func (temporaryError) Temporary() bool { return true }

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Temporary() bool { return true }
func (timeoutError) Timeout() bool   { return true }
