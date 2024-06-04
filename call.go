package dispatch

import (
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
	message *sdkv1.Call
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
	return CallOption(func(call *Call) { call.message.Expiration = durationpb.New(expiration) })
}

// WithCorrelationID sets a function call correlation ID.
func WithCorrelationID(correlationID uint64) CallOption {
	return CallOption(func(call *Call) { call.message.CorrelationId = correlationID })
}

// WithVersion sets a function call version.
func WithVersion(version string) CallOption {
	return CallOption(func(call *Call) { call.message.Version = version })
}

// Endpoint is the URL of the service where the function resides.
func (c Call) Endpoint() string {
	return c.message.GetEndpoint()
}

// Function is the name of the function to call.
func (c Call) Function() string {
	return c.message.GetFunction()
}

// Input is input to the function.
func (c Call) Input() (proto.Message, error) {
	input := c.message.GetInput()
	if input == nil {
		return nil, errors.New("no input")
	}
	return c.message.Input.UnmarshalNew()
}

// Expiration is the maximum time the function is allowed to run.
func (c Call) Expiration() time.Duration {
	return c.message.GetExpiration().AsDuration()
}

// Version of the application to select during execution.
// The version is an optional field and not supported by all platforms.
func (c Call) Version() string {
	return c.message.GetVersion()
}

// CorrelationID is an opaque value that gets repeated in CallResult to
// correlate asynchronous calls with their results.
func (c Call) CorrelationID() uint64 {
	return c.message.GetCorrelationId()
}

func (c Call) proto() *sdkv1.Call {
	return c.message
}
