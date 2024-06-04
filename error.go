package dispatch

import (
	"errors"
	"reflect"
	"strings"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
)

var (
	// ErrTimeout indicates an operation failed due to a timeout.
	ErrTimeout error = statusError(TimeoutStatus)

	// ErrTimeout indicates an operation failed due to throttling.
	ErrThrottled error = statusError(ThrottledStatus)

	// ErrInvalidArgument indicates an operation failed due to an invalid argument.
	ErrInvalidArgument error = statusError(InvalidArgumentStatus)

	// ErrInvalidResponse indicates an operation failed due to an invalid response.
	ErrInvalidResponse error = statusError(InvalidResponseStatus)

	// ErrTemporary indicates an operation failed with a temporary error.
	ErrTemporary error = statusError(TemporaryErrorStatus)

	// ErrPermanent indicates an operation failed with a permanent error.
	ErrPermanent error = statusError(PermanentErrorStatus)

	// ErrIncompatibleStatus indicates that a coroutine's serialized state is incompatible.
	ErrIncompatibleState error = statusError(IncompatibleStateStatus)

	// ErrDNS indicates an operation failed with a DNS error.
	ErrDNS error = statusError(DNSErrorStatus)

	// ErrTCP indicates an operation failed with a TCP error.
	ErrTCP error = statusError(TCPErrorStatus)

	// ErrTLS indicates an operation failed with a TLS error.
	ErrTLS error = statusError(TLSErrorStatus)

	// ErrHTTP indicates an operation failed with a HTTP error.
	ErrHTTP error = statusError(HTTPErrorStatus)

	// ErrUnauthenticated indicates an operation failed or was not attempted
	// because the caller did not authenticate correctly.
	ErrUnauthenticated error = statusError(UnauthenticatedStatus)

	// ErrPermissionDenied indicates an operation failed or was not attempted
	// because the caller did not have permission.
	ErrPermissionDenied error = statusError(PermissionDeniedStatus)

	// ErrNotFound indicates an operation failed because a resource could not be found.
	ErrNotFound error = statusError(NotFoundStatus)
)

type statusError Status

func (e statusError) Status() Status {
	return Status(e)
}

func (e statusError) Error() string {
	return e.Status().String()
}

func errorTypeOf(err error) string {
	if err == nil {
		return ""
	}
	typ := reflect.TypeOf(err)
	if name := typ.Name(); name != "" {
		return name
	}
	str := typ.String()
	if i := strings.LastIndexByte(str, '.'); i >= 0 {
		return str[i+1:]
	}
	return str
}

func errorStatusOf(err error) Status {
	if err == nil {
		return OKStatus
	}
	if status := statusOf(err); status != UnspecifiedStatus {
		return status
	}
	if isTimeout(err) {
		return TimeoutStatus
	}
	if isTemporary(err) {
		return TemporaryErrorStatus
	}
	return PermanentErrorStatus
}

func isTemporary(err error) bool {
	if ok, found := errorIsTemporary(err); found {
		return ok
	} else {
		return isTimeout(err)
	}
}

func isTimeout(err error) bool {
	ok, _ := errorIsTimeout(err)
	return ok
}

func errorIsTemporary(err error) (ok, found bool) {
	var t temporary
	if errors.As(err, &t) {
		return t.Temporary(), true
	}
	return
}

func errorIsTimeout(err error) (ok, found bool) {
	var t timeout
	if errors.As(err, &t) {
		return t.Timeout(), true
	}
	return
}

type temporary interface {
	Temporary() bool
}

type timeout interface {
	Timeout() bool
}

// ErrorResponse creates a RunResponse for the specified error.
func ErrorResponse(err error) *sdkv1.RunResponse {
	return &sdkv1.RunResponse{
		Status: errorStatusOf(err).proto(),
		Directive: &sdkv1.RunResponse_Exit{
			Exit: &sdkv1.Exit{
				Result: &sdkv1.CallResult{
					Error: &sdkv1.Error{
						Type:    errorTypeOf(err),
						Message: err.Error(),
					},
				},
			},
		},
	}
}
