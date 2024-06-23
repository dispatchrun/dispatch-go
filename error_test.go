package dispatch_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

func TestErrorStatus(t *testing.T) {
	tests := []struct {
		scenario string
		error    func(*testing.T) error
		status   dispatchproto.Status
	}{
		// Testing for the nil error ensures that if for any reasons some of the
		// tests will return nil, they will fail because we know that the status
		// will be OK.

		{
			scenario: "nil",
			error:    func(*testing.T) error { return nil },
			status:   dispatchproto.OKStatus,
		},

		// In some cases, the user may want to manually categorize an error. Err{Status}
		// errors are provided for this use case.

		{
			scenario: "when the error is due to a timeout",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrTimeout) },
			status:   dispatchproto.TimeoutStatus,
		},

		{
			scenario: "when the error is due to throttling",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrThrottled) },
			status:   dispatchproto.ThrottledStatus,
		},

		{
			scenario: "when the error is due to an invalid argument",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrInvalidArgument) },
			status:   dispatchproto.InvalidArgumentStatus,
		},

		{
			scenario: "when the error is due to an invalid response",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrInvalidResponse) },
			status:   dispatchproto.InvalidResponseStatus,
		},

		{
			scenario: "when the error is due to a temporary error",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrTemporary) },
			status:   dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "when the error is due to a permanent error",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrPermanent) },
			status:   dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "when the error is due to incompatible state",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrIncompatibleState) },
			status:   dispatchproto.IncompatibleStateStatus,
		},

		{
			scenario: "when the error is due to DNS",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrDNS) },
			status:   dispatchproto.DNSErrorStatus,
		},

		{
			scenario: "when the error is due to TCP",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrTCP) },
			status:   dispatchproto.TCPErrorStatus,
		},

		{
			scenario: "when the error is due to TLS",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrTLS) },
			status:   dispatchproto.TLSErrorStatus,
		},

		{
			scenario: "when the error is due to HTTP",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrHTTP) },
			status:   dispatchproto.HTTPErrorStatus,
		},

		{
			scenario: "when the error is due to authentication",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrUnauthenticated) },
			status:   dispatchproto.UnauthenticatedStatus,
		},

		{
			scenario: "when the error is due to permissions",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrPermissionDenied) },
			status:   dispatchproto.PermissionDeniedStatus,
		},

		{
			scenario: "when the error is due to not found",
			error:    func(*testing.T) error { return fmt.Errorf("error: %w", dispatch.ErrNotFound) },
			status:   dispatchproto.NotFoundStatus,
		},

		// Error values may have a Status() Status method to override the
		// default behavior of the ErrorStatus function.
		//
		// We use the same construct with two status codes to ensure that the
		// value is properly propagated.

		{
			scenario: "when the error value has a Status method it is used to determine the status (INVALID_ARGUMENT)",
			error: func(*testing.T) error {
				return fmt.Errorf("error: %w", dispatchproto.StatusError(dispatchproto.InvalidArgumentStatus))
			},
			status: dispatchproto.InvalidArgumentStatus,
		},

		{
			scenario: "when the error value has a Status method it is used to determine the status (INVALID_RESPONSE)",
			error: func(*testing.T) error {
				return fmt.Errorf("error: %w", dispatchproto.StatusError(dispatchproto.InvalidResponseStatus))
			},
			status: dispatchproto.InvalidResponseStatus,
		},

		// Error values returned by errors.Join have a special Unwrap() []error
		// method that we use to inspect all inner errors in search for a status
		// code.
		//
		// For now, we default to UNSPECIFIED, unless all error values have the
		// same status.

		{
			scenario: "errors.Join with a single permanent error",
			error:    func(*testing.T) error { return errors.Join(permanent{}) },
			status:   dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "errors.Join with a single temporary error",
			error:    func(*testing.T) error { return errors.Join(temporary{}) },
			status:   dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "errors.Join with a single timeout error",
			error:    func(*testing.T) error { return errors.Join(timeout{}) },
			status:   dispatchproto.TimeoutStatus,
		},

		{
			scenario: "errors.Join with multiple permanent error",
			error:    func(*testing.T) error { return errors.Join(permanent{}, permanent{}, permanent{}) },
			status:   dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "errors.Join with multiple temporary error",
			error:    func(*testing.T) error { return errors.Join(temporary{}, temporary{}, temporary{}) },
			status:   dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "errors.Join with multiple timeout error",
			error:    func(*testing.T) error { return errors.Join(timeout{}, timeout{}, timeout{}) },
			status:   dispatchproto.TimeoutStatus,
		},

		{
			scenario: "errors.Join with a permanent error and a temporary error",
			error: func(*testing.T) error {
				return errors.Join(errors.Join(permanent{}), errors.Join(temporary{}))
			},
			status: dispatchproto.UnspecifiedStatus,
		},

		{
			scenario: "errors.Join with a permanent error and timeout error",
			error: func(*testing.T) error {
				return errors.Join(errors.Join(permanent{}), errors.Join(timeout{}))
			},
			status: dispatchproto.UnspecifiedStatus,
		},

		{
			scenario: "errors.Join with a temporary error and timeout error",
			error: func(*testing.T) error {
				return errors.Join(errors.Join(temporary{}), errors.Join(timeout{}))
			},
			status: dispatchproto.UnspecifiedStatus,
		},

		// Context errors are often dependent on the context's state, or the
		// inner cause that was used to cancel the context.

		{
			scenario: "context cancellation",
			error: func(*testing.T) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx.Err()
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "context cancellation with cause as permanent error",
			error: func(*testing.T) error {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(permanent{})
				return context.Cause(ctx)
			},
			status: dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "context cancellation with cause as temporary error",
			error: func(*testing.T) error {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(temporary{})
				return context.Cause(ctx)
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "context cancellation with cause as timeout error",
			error: func(*testing.T) error {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(timeout{})
				return context.Cause(ctx)
			},
			status: dispatchproto.TimeoutStatus,
		},

		// Connection issuses should generally be treated as temporary errors
		// in distributed systems; these are usually transient states. There are
		// cases where they happen due to misconfiguration, but those should be
		// handled by the AIMD backpressure to avoid overloading the system.

		{
			scenario: "connecting to an address that does not exit",
			error: func(*testing.T) error {
				_, err := net.Dial("tcp", "127.0.0.1:0")
				return err
			},
			status: dispatchproto.TCPErrorStatus,
		},

		{
			scenario: "connecting to an address where no server is listening",
			error: func(*testing.T) error {
				_, err := net.Dial("tcp", "127.0.0.1:56789")
				return err
			},
			status: dispatchproto.TCPErrorStatus,
		},

		{
			scenario: "sending a request to an address that does not exist",
			error: func(*testing.T) error {
				r, err := http.Get("http://127.0.0.1:0")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.TCPErrorStatus,
		},

		{
			scenario: "sending a request to an address where no server is listening",
			error: func(*testing.T) error {
				r, err := http.Get("http://127.0.0.1:56789")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.TCPErrorStatus,
		},

		{
			scenario: "connection closed while sending a http request",
			error: func(*testing.T) error {
				l, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()

				done := make(chan struct{})
				defer func() { <-done }()

				go func() {
					defer close(done)
					conn, err := l.Accept()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
				}()

				client := &http.Client{}
				r, err := client.Get("http://" + l.Addr().String() + "/")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.TCPErrorStatus,
		},

		{
			scenario: "connection closed while sending a https request",
			error: func(*testing.T) error {
				l, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()

				done := make(chan struct{})
				defer func() { <-done }()

				cert, err := tls.X509KeyPair(certPem, keyPem)
				if err != nil {
					t.Fatal(err)
				}

				go func() {
					defer close(done)
					conn, err := l.Accept()
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()

					tlsConn := tls.Server(conn, &tls.Config{
						GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
							return &cert, nil
						},
					})

					if err := tlsConn.Handshake(); err != nil {
						t.Error(err)
					}
				}()

				transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
				defer transport.CloseIdleConnections()

				client := &http.Client{Transport: transport}
				r, err := client.Get("https://" + l.Addr().String() + "/")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.TCPErrorStatus,
		},

		// fs package errors often indicate an invalid state in the application
		// logic, probably better to treat as permanent errors so we don't retry
		// indefinitely.

		{
			scenario: "fs.ErrInvalid",
			error:    func(*testing.T) error { return fs.ErrInvalid },
			status:   dispatchproto.InvalidArgumentStatus,
		},

		{
			scenario: "fs.ErrPermission",
			error:    func(*testing.T) error { return fs.ErrPermission },
			status:   dispatchproto.PermissionDeniedStatus,
		},

		{
			scenario: "fs.ErrExist",
			error:    func(*testing.T) error { return fs.ErrExist },
			status:   dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "fs.ErrNotExist",
			error:    func(*testing.T) error { return fs.ErrNotExist },
			status:   dispatchproto.NotFoundStatus,
		},

		// read/write on closed connections or files is often a sign of a race
		// condition; non-deterministic errors are likely good to to interpret
		// as temporary so they get retried.

		{
			scenario: "reading from a closed connection",
			error: func(*testing.T) error {
				c1, c2 := net.Pipe()
				c1.Close()
				c2.Close()
				_, err := c1.Read([]byte{0})
				return err
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "reading from a closed io pipe",
			error: func(*testing.T) error {
				r, w := io.Pipe()
				r.Close()
				w.Close()
				_, err := r.Read([]byte{0})
				return err
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "reading from a closed system pipe",
			error: func(*testing.T) error {
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				r.Close()
				w.Close()
				_, err = r.Read([]byte{0})
				return err
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "writing to a closed connection",
			error: func(*testing.T) error {
				c1, c2 := net.Pipe()
				c1.Close()
				defer c2.Close()
				_, err := c2.Write([]byte("hello"))
				return err
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "writing to a closed io pipe",
			error: func(*testing.T) error {
				r, w := io.Pipe()
				r.Close()
				defer w.Close()
				_, err := w.Write([]byte("hello"))
				return err
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "writing to a closed system pipe",
			error: func(*testing.T) error {
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				r.Close()
				defer w.Close()
				_, err = w.Write([]byte("hello"))
				return err
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "waiting for a http response on a closed connection",
			error: func(*testing.T) error {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
				}))
				defer server.Close()

				client := &http.Client{}
				r, err := client.Get(server.URL + "/")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.TCPErrorStatus,
		},

		// Timeouts have a dedicated category, make sure that various conditions
		// that trigger timeouts are handled as expected.

		{
			scenario: "timeout",
			error:    func(*testing.T) error { return timeout{} },
			status:   dispatchproto.TimeoutStatus,
		},

		{
			scenario: "timeout reading from a connection",
			error: func(*testing.T) error {
				c1, c2 := net.Pipe()
				defer c1.Close()
				defer c2.Close()
				_ = c2.SetReadDeadline(time.Now().Add(-time.Second))
				_, err := c2.Read([]byte{0})
				return err
			},
			status: dispatchproto.TimeoutStatus,
		},

		{
			scenario: "timeout reading from a system pipe",
			error: func(*testing.T) error {
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				defer w.Close()
				_ = r.SetReadDeadline(time.Now().Add(-time.Second))
				_, err = r.Read([]byte{0})
				return err
			},
			status: dispatchproto.TimeoutStatus,
		},

		{
			scenario: "timeout waiting for a http response",
			error: func(*testing.T) error {
				wait := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					<-wait
				}))
				defer server.Close()
				defer close(wait)

				client := &http.Client{Timeout: 50 * time.Millisecond}
				r, err := client.Get(server.URL + "/")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.TimeoutStatus,
		},

		// Problems with DNS resolution are very common, we want to retry those
		// as they are often transient (e.g., UDP is lossy, DNS servers cache
		// records, etc...).

		{
			scenario: "DNS resolution error",
			error:    func(*testing.T) error { return &net.DNSError{Err: "no such host"} },
			status:   dispatchproto.DNSErrorStatus,
		},

		{
			scenario: "DNS resolution error with temporary flag",
			error:    func(*testing.T) error { return &net.DNSError{Err: "no such host", IsTemporary: true} },
			status:   dispatchproto.DNSErrorStatus,
		},

		{
			scenario: "DNS resolution error with timeout flag",
			error:    func(*testing.T) error { return &net.DNSError{Err: "no such host", IsTimeout: true} },
			status:   dispatchproto.DNSErrorStatus,
		},

		{
			scenario: "sending a http request to a hostname that does not exist",
			error: func(*testing.T) error {
				r, err := http.Get("http://nowhere/")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.DNSErrorStatus,
		},

		// Errors coming from the TLS stack should be categorized as such.
		// This is helpful to quickly identify the root cause of these
		// issues. In general, these are caused by misconfigured TLS
		// certificates.

		{
			scenario: "sending a https request to a server with a self-signed certificate",
			error: func(*testing.T) error {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
				server.Config = &http.Server{ErrorLog: log.New(io.Discard, "", 0)}
				server.StartTLS()
				defer server.Close()

				client := &http.Client{}
				r, err := client.Get(server.URL + "/")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.TLSErrorStatus,
		},

		// HTTP protocol errors should only occur in case of misconfiguration
		// where the server is not actually speaking HTTP.

		{
			scenario: "http.ErrNotSupported",
			error:    func(*testing.T) error { return http.ErrNotSupported },
			status:   dispatchproto.HTTPErrorStatus,
		},

		{
			scenario: "http.ErrMissingBoundary",
			error:    func(*testing.T) error { return http.ErrMissingBoundary },
			status:   dispatchproto.HTTPErrorStatus,
		},

		{
			scenario: "http.ErrNotMultipart",
			error:    func(*testing.T) error { return http.ErrNotMultipart },
			status:   dispatchproto.HTTPErrorStatus,
		},

		{
			scenario: "sending a http request to a server that does not speak HTTP",
			error: func(*testing.T) error {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, rw, _ := w.(http.Hijacker).Hijack()
					defer conn.Close()

					_, _ = rw.WriteString("hello\n")
					rw.Flush()
				}))
				defer server.Close()

				client := &http.Client{}
				r, err := client.Get(server.URL + "/")
				if err != nil {
					return err
				}
				r.Body.Close()
				return nil
			},
			status: dispatchproto.InvalidResponseStatus,
		},

		{
			scenario: "receiving a http request where the body is missing",
			error: func(*testing.T) error {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, rw, _ := w.(http.Hijacker).Hijack()
					_, _ = rw.WriteString("HTTP/1.1 200 OK\r\n")
					_, _ = rw.WriteString("Content-Length: 5\r\n")
					_, _ = rw.WriteString("\r\n")
					rw.Flush()
					conn.Close()
				}))
				defer server.Close()

				client := &http.Client{}
				r, err := client.Get(server.URL + "/")
				if err != nil {
					t.Fatal(err)
				}
				defer r.Body.Close()
				if _, err = io.ReadAll(r.Body); err != nil {
					err = &url.Error{Op: "Get", URL: server.URL, Err: err}
				}
				return err
			},
			status: dispatchproto.InvalidResponseStatus,
		},

		// The SDK uses the connect library when remotely interacting with functions.
		// Connect uses gRPC error codes. Check that the correct Dispatch status is
		// derived from these error codes.

		{
			scenario: "connect.CodeCanceled",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeCanceled, errors.New("the request was canceled"))
			},
			status: dispatchproto.TimeoutStatus,
		},

		{
			scenario: "connect.CodeUnknown",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeUnknown, errors.New("unknown"))
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "connect.CodeInvalidArgument",
			error: func(*testing.T) error {
				underlying := connect.NewError(connect.CodeInvalidArgument, errors.New("invalid argument"))
				return fmt.Errorf("something went wrong: %w", underlying)
			},
			status: dispatchproto.InvalidArgumentStatus,
		},

		{
			scenario: "connect.CodeDeadlineExceeded",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeDeadlineExceeded, errors.New("deadline exceeded"))
			},
			status: dispatchproto.TimeoutStatus,
		},

		{
			scenario: "connect.CodeNotFound",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeNotFound, errors.New("not found"))
			},
			status: dispatchproto.NotFoundStatus,
		},

		{
			scenario: "connect.CodeAlreadyExists",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeAlreadyExists, errors.New("already exists"))
			},
			status: dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "connect.CodePermissionDenied",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodePermissionDenied, errors.New("permission denied"))
			},
			status: dispatchproto.PermissionDeniedStatus,
		},

		{
			scenario: "connect.CodeResourceExhausted",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeResourceExhausted, errors.New("resource exhausted"))
			},
			status: dispatchproto.ThrottledStatus,
		},

		{
			scenario: "connect.CodeFailedPrecondition",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeFailedPrecondition, errors.New("failed precondition"))
			},
			status: dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "connect.CodeAborted",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeAborted, errors.New("aborted"))
			},
			status: dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "connect.CodeOutOfRange",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeOutOfRange, errors.New("out of range"))
			},
			status: dispatchproto.InvalidArgumentStatus,
		},

		{
			scenario: "connect.CodeUnimplemented",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
			},
			status: dispatchproto.NotFoundStatus,
		},

		{
			scenario: "connect.CodeInternal",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeInternal, errors.New("internal"))
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "connect.CodeUnavailable",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
			},
			status: dispatchproto.TemporaryErrorStatus,
		},

		{
			scenario: "connect.CodeDataLoss",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeDataLoss, errors.New("data loss"))
			},
			status: dispatchproto.PermanentErrorStatus,
		},

		{
			scenario: "connect.CodeUnauthenticated",
			error: func(*testing.T) error {
				return connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated"))
			},
			status: dispatchproto.UnauthenticatedStatus,
		},

		{
			scenario: "connect.CodeUnauthenticated",
			error: func(*testing.T) error {
				return connect.NewError(connect.Code(9999), errors.New("unknown"))
			},
			status: dispatchproto.PermanentErrorStatus,
		},

		// The default behavior is to assume permanent errors, but we still want
		// to validate that a few common cases are handled as expected.
		//
		// (TODO)
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			err := test.error(t)
			if status := dispatchproto.ErrorStatus(err); status != test.status {
				s := new(strings.Builder)
				inspectErrorChain(s, err, 0)
				t.Errorf("%T: %s: expected %s, got %s\n%s", err, err, test.status, status, s.String())
			}
		})
	}
}

type permanent struct{}

func (permanent) Error() string   { return "permanent" }
func (permanent) Temporary() bool { return false }

type temporary struct{}

func (temporary) Error() string   { return "temporary" }
func (temporary) Temporary() bool { return true }

type timeout struct{}

func (timeout) Error() string   { return "timeout" }
func (timeout) Temporary() bool { return true }
func (timeout) Timeout() bool   { return true }

// inspectErrorChain is a helper function to print the error chain for debugging
// in the error status tests.
func inspectErrorChain(s *strings.Builder, err error, indent int) {
	for {
		if err == nil {
			return
		}
		for i := 0; i < indent; i++ {
			s.WriteByte(' ')
		}
		fmt.Fprintf(s, "+ %T: %s\n", err, err)
		indent += 2
		err = errors.Unwrap(err)
	}
}

var certPem = []byte(`-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`)

var keyPem = []byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`)
