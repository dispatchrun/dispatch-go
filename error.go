//go:build !durable

package dispatch

import "github.com/dispatchrun/dispatch-go/dispatchproto"

var (
	// ErrTimeout indicates an operation failed due to a timeout.
	ErrTimeout error = dispatchproto.StatusError(dispatchproto.TimeoutStatus)

	// ErrTimeout indicates an operation failed due to throttling.
	ErrThrottled error = dispatchproto.StatusError(dispatchproto.ThrottledStatus)

	// ErrInvalidArgument indicates an operation failed due to an invalid argument.
	ErrInvalidArgument error = dispatchproto.StatusError(dispatchproto.InvalidArgumentStatus)

	// ErrInvalidResponse indicates an operation failed due to an invalid response.
	ErrInvalidResponse error = dispatchproto.StatusError(dispatchproto.InvalidResponseStatus)

	// ErrTemporary indicates an operation failed with a temporary error.
	ErrTemporary error = dispatchproto.StatusError(dispatchproto.TemporaryErrorStatus)

	// ErrPermanent indicates an operation failed with a permanent error.
	ErrPermanent error = dispatchproto.StatusError(dispatchproto.PermanentErrorStatus)

	// ErrIncompatibleStatus indicates that a function's serialized state is incompatible.
	ErrIncompatibleState error = dispatchproto.StatusError(dispatchproto.IncompatibleStateStatus)

	// ErrDNS indicates an operation failed with a DNS error.
	ErrDNS error = dispatchproto.StatusError(dispatchproto.DNSErrorStatus)

	// ErrTCP indicates an operation failed with a TCP error.
	ErrTCP error = dispatchproto.StatusError(dispatchproto.TCPErrorStatus)

	// ErrTLS indicates an operation failed with a TLS error.
	ErrTLS error = dispatchproto.StatusError(dispatchproto.TLSErrorStatus)

	// ErrHTTP indicates an operation failed with a HTTP error.
	ErrHTTP error = dispatchproto.StatusError(dispatchproto.HTTPErrorStatus)

	// ErrUnauthenticated indicates an operation failed or was not attempted
	// because the caller did not authenticate correctly.
	ErrUnauthenticated error = dispatchproto.StatusError(dispatchproto.UnauthenticatedStatus)

	// ErrPermissionDenied indicates an operation failed or was not attempted
	// because the caller did not have permission.
	ErrPermissionDenied error = dispatchproto.StatusError(dispatchproto.PermissionDeniedStatus)

	// ErrNotFound indicates an operation failed because a resource could not be found.
	ErrNotFound error = dispatchproto.StatusError(dispatchproto.NotFoundStatus)
)
