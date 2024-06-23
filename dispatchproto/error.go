package dispatchproto

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"

	"connectrpc.com/connect"
	"golang.org/x/sys/unix"
)

// StatusError is a Status as an error.
type StatusError Status

func (e StatusError) Status() Status {
	return Status(e)
}

func (e StatusError) Error() string {
	return e.Status().String()
}

// ErrorStatus categorizes an error to return a Status code.
func ErrorStatus(err error) Status { return errorStatus(err, 0) }

func errorStatus(err error, depth int) Status {
	if depth++; depth == 16 {
		return UnspecifiedStatus
	}

	switch err {
	case nil:
		return OKStatus

	case context.Canceled:
		return TemporaryErrorStatus

	case context.DeadlineExceeded:
		return TimeoutStatus

	case fs.ErrInvalid:
		return InvalidArgumentStatus

	case fs.ErrPermission:
		return PermissionDeniedStatus

	case fs.ErrNotExist:
		return NotFoundStatus

	case fs.ErrClosed:
		return TemporaryErrorStatus

	case net.ErrClosed:
		return TemporaryErrorStatus

	case http.ErrNotSupported,
		http.ErrMissingBoundary,
		http.ErrNotMultipart:
		return HTTPErrorStatus
	}

	if isIOError(err) {
		return TemporaryErrorStatus
	}

	switch e := err.(type) {
	case unix.Errno: // alias for syscall.Errno
		return errnoStatus(e)

	case *fs.PathError:
		// Path errors indicate an operation that occured on the file system,
		// in which case we translate status codes assuming a protocol error
		// into a more generic temporary error.
		status := errorStatus(e.Err, depth)
		if status == TCPErrorStatus {
			status = TemporaryErrorStatus
		}
		return status

	case *os.SyscallError:
		return errorStatus(e.Err, depth)

	case *url.Error:
		// URL errors tend to be reported by the net/http package and when they
		// embed an I/O error, this is usually due to an issue at the TCP layer.
		if e.Err == io.ErrUnexpectedEOF {
			return InvalidResponseStatus
		}
		if isMalformedHTTPResponse(e.Err) {
			return InvalidResponseStatus
		}
		if isIOError(e.Err) {
			return TCPErrorStatus
		}
		return errorStatus(e.Err, depth)

	case *net.OpError:
		return errorStatus(e.Err, depth)

	case *net.DNSError:
		return DNSErrorStatus

	case *tls.CertificateVerificationError:
		return TLSErrorStatus

	case *tls.RecordHeaderError:
		return TLSErrorStatus

	case *connect.Error:
		return connectErrorStatus(e)

	case status:
		return e.Status()

	case unwrapper:
		status := UnspecifiedStatus

		for _, innerError := range e.Unwrap() {
			if innerStatus := errorStatus(innerError, depth); status == UnspecifiedStatus {
				status = innerStatus
			} else if status != innerStatus {
				return UnspecifiedStatus
			}
		}

		return status
	}

	if e, ok := err.(timeout); ok && e.Timeout() {
		return TimeoutStatus
	}

	if e, ok := err.(temporary); ok && e.Temporary() {
		return TemporaryErrorStatus
	}

	if e := errors.Unwrap(err); e != nil {
		return errorStatus(e, depth)
	}

	return PermanentErrorStatus
}

func errnoStatus(errno unix.Errno) Status {
	switch errno {
	case unix.ECONNREFUSED,
		unix.ECONNRESET,
		unix.ECONNABORTED,
		unix.EPIPE,
		unix.ENETDOWN,
		unix.ENETUNREACH,
		unix.ENETRESET,
		unix.EHOSTDOWN,
		unix.EHOSTUNREACH,
		unix.EADDRNOTAVAIL:
		return TCPErrorStatus

	case unix.ETIMEDOUT:
		return TimeoutStatus

	case unix.EPERM:
		return PermissionDeniedStatus

	case unix.EAGAIN,
		unix.EINTR,
		unix.EMFILE,
		unix.ENFILE:
		return TemporaryErrorStatus

	default:
		return PermanentErrorStatus
	}
}

func connectErrorStatus(err *connect.Error) Status {
	switch err.Code() {
	case connect.CodeCanceled: // 408 Request Timeout
		return TimeoutStatus
	case connect.CodeUnknown: // 500 Internal Server Error
		return TemporaryErrorStatus
	case connect.CodeInvalidArgument: // 400 Bad Request
		return InvalidArgumentStatus
	case connect.CodeDeadlineExceeded: // 408 Request Timeout
		return TimeoutStatus
	case connect.CodeNotFound: // 404 Not Found
		return NotFoundStatus
	case connect.CodeAlreadyExists: // 409 Conflict
		return PermanentErrorStatus
	case connect.CodePermissionDenied: // 403 Forbidden
		return PermissionDeniedStatus
	case connect.CodeResourceExhausted: // 429 Too Many Requests
		return ThrottledStatus
	case connect.CodeFailedPrecondition: // 412 Precondition Failed
		return PermanentErrorStatus
	case connect.CodeAborted: // 409 Conflict
		return PermanentErrorStatus
	case connect.CodeOutOfRange: // 400 Bad Request
		return InvalidArgumentStatus
	case connect.CodeUnimplemented: // 404 Not Found
		return NotFoundStatus
	case connect.CodeInternal: // 500 Internal Server Error
		return TemporaryErrorStatus
	case connect.CodeUnavailable: // 503 Service Unavailable
		return TemporaryErrorStatus
	case connect.CodeDataLoss: // 500 Internal Server Error
		return PermanentErrorStatus
	case connect.CodeUnauthenticated: // 401 Unauthorized
		return UnauthenticatedStatus
	default:
		return PermanentErrorStatus
	}
}

func isIOError(err error) bool {
	switch err {
	case io.EOF,
		io.ErrClosedPipe,
		io.ErrNoProgress,
		io.ErrShortBuffer,
		io.ErrShortWrite,
		io.ErrUnexpectedEOF:
		return true
	default:
		return false
	}
}

func isMalformedHTTPResponse(err error) bool {
	return err != nil &&
		strings.Contains(baseError(err).Error(), "malformed HTTP response")
}

func baseError(err error) error {
	for {
		if e := errors.Unwrap(err); e != nil {
			err = e
		} else {
			return err
		}
	}
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

type temporary interface {
	Temporary() bool
}

type timeout interface {
	Timeout() bool
}

type status interface {
	Status() Status
}

type unwrapper interface {
	Unwrap() []error // implemented by error values returned by errors.Join
}
