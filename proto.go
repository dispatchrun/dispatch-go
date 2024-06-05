package dispatch

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Call is a function call.
type Call struct {
	proto *sdkv1.Call
}

// NewCall creates a Call.
func NewCall(endpoint, function string, input proto.Message, opts ...CallOption) (Call, error) {
	inputAny, err := anypb.New(input)
	if err != nil {
		return Call{}, fmt.Errorf("cannot serialize call input: %w", err)
	}

	call := Call{&sdkv1.Call{
		Endpoint: endpoint,
		Function: function,
		Input:    inputAny,
	}}
	for _, opt := range opts {
		opt(&call)
	}
	return call, nil
}

// CallOption configures a call.
type CallOption func(*Call)

// WithExpiration sets a function call expiration.
func WithExpiration(expiration time.Duration) CallOption {
	return CallOption(func(call *Call) { call.proto.Expiration = durationpb.New(expiration) })
}

// WithCorrelationID sets a function call correlation ID.
func WithCorrelationID(correlationID uint64) CallOption {
	return CallOption(func(call *Call) { call.proto.CorrelationId = correlationID })
}

// WithVersion sets a function call version.
func WithVersion(version string) CallOption {
	return CallOption(func(call *Call) { call.proto.Version = version })
}

// Endpoint is the URL of the service where the function resides.
func (c Call) Endpoint() string {
	return c.proto.GetEndpoint()
}

// Function is the name of the function to call.
func (c Call) Function() string {
	return c.proto.GetFunction()
}

// Input is input to the function.
func (c Call) Input() (proto.Message, error) {
	input := c.proto.GetInput()
	if input == nil {
		return nil, errors.New("no input")
	}
	return c.proto.Input.UnmarshalNew()
}

// Expiration is the maximum time the function is allowed to run.
func (c Call) Expiration() time.Duration {
	return c.proto.GetExpiration().AsDuration()
}

// Version of the application to select during execution.
// The version is an optional field and not supported by all platforms.
func (c Call) Version() string {
	return c.proto.GetVersion()
}

// CorrelationID is an opaque value that gets repeated in CallResult to
// correlate asynchronous calls with their results.
func (c Call) CorrelationID() uint64 {
	return c.proto.GetCorrelationId()
}

// String is the string representation of the call.
func (c Call) String() string {
	return fmt.Sprintf("Call(%s)", c.proto.String())
}

// Equal is true if the call is equal to another.
func (c Call) Equal(other Call) bool {
	if c.proto == nil || other.proto == nil {
		return false
	}
	if c.Endpoint() != other.Endpoint() {
		return false
	}
	if c.Function() != other.Function() {
		return false
	}
	if c.CorrelationID() != other.CorrelationID() {
		return false
	}
	if c.Expiration() != other.Expiration() {
		return false
	}
	if c.Version() != other.Version() {
		return false
	}
	input, _ := c.Input()
	otherInput, _ := other.Input()
	return input != nil && otherInput != nil && proto.Equal(input, otherInput)
}

// Error is an error that occurred during execution of a function.
type Error struct {
	proto *sdkv1.Error
}

// NewError creates an error.
func NewError(typ, message string, opts ...ErrorOption) Error {
	err := Error{&sdkv1.Error{
		Type:    typ,
		Message: message,
	}}
	for _, opt := range opts {
		opt(&err)
	}
	return err
}

// ErrorOption configures an Error.
type ErrorOption func(*Error)

// WithErrorValue sets the language-specific representation of the error.
//
// This is used to enable propagation of the error value between
// instances of a program, by encoding information allowing the error
// value to be reconstructed.
func WithErrorValue(value []byte) ErrorOption {
	return func(e *Error) { e.proto.Value = value }
}

// WithErrorTraceback sets the encoded stack trace for the error.
//
// The format is language-specific, encoded in the standard format used by
// each programming language to represent stack traces. Not all languages have
// stack traces for errors, so in some cases the value might be omitted.
func WithErrorTraceback(traceback []byte) ErrorOption {
	return func(e *Error) { e.proto.Traceback = traceback }
}

// Type is the type of error that occurred.
//
// This value is language and application specific. It is is used to provide
// debugging information to the user.
func (e Error) Type() string {
	return e.proto.GetType()
}

// Message is a human-readable message providing more details about the error.
func (e Error) Message() string {
	return e.proto.GetMessage()
}

// Value is the language-specific representation of the error.
func (e Error) Value() []byte {
	return e.proto.GetValue()
}

// Traceback is the encoded stack trace for the error.
func (e Error) Traceback() []byte {
	return e.proto.GetTraceback()
}

// String is the string representation of the call.
func (e Error) String() string {
	return fmt.Sprintf("Error(%s)", e.proto.String())
}

// Equal is true if the error is equal to another.
func (e Error) Equal(other Error) bool {
	if e.proto == nil || other.proto == nil {
		return false
	}
	return e.Type() == other.Type() &&
		e.Message() == other.Message() &&
		bytes.Equal(e.Value(), other.Value()) &&
		bytes.Equal(e.Traceback(), other.Traceback())
}
