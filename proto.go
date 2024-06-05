package dispatch

import (
	"bytes"
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

// WithCallExpiration sets a function call expiration.
func WithCallExpiration(expiration time.Duration) CallOption {
	return func(call *Call) { call.proto.Expiration = durationpb.New(expiration) }
}

// WithCallCorrelationID sets a function call correlation ID.
func WithCallCorrelationID(correlationID uint64) CallOption {
	return func(call *Call) { call.proto.CorrelationId = correlationID }
}

// WithCallVersion sets a function call version.
func WithCallVersion(version string) CallOption {
	return func(call *Call) { call.proto.Version = version }
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
		return nil, fmt.Errorf("no input")
	}
	return input.UnmarshalNew()
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
	input, err := c.Input()
	if err != nil {
		return false
	}
	otherInput, err := other.Input()
	if err != nil {
		return false
	}
	return input != nil && otherInput != nil && proto.Equal(input, otherInput)
}

// CallResult is a function call result.
type CallResult struct {
	proto  *sdkv1.CallResult
	output proto.Message
}

// NewCallResult creates a CallResult.
func NewCallResult(opts ...CallResultOption) (CallResult, error) {
	result := CallResult{proto: &sdkv1.CallResult{}}
	for _, opt := range opts {
		opt(&result)
	}

	if result.output != nil {
		outputAny, err := anypb.New(result.output)
		if err != nil {
			return CallResult{}, fmt.Errorf("cannot serialize call output: %w", err)
		}
		result.proto.Output = outputAny
	}

	return result, nil
}

// CallResultOption configures a CallResult.
type CallResultOption func(*CallResult)

// WithCallResultCorrelationID sets a function call result correlation ID.
func WithCallResultCorrelationID(correlationID uint64) CallResultOption {
	return func(result *CallResult) { result.proto.CorrelationId = correlationID }
}

// WithCallResultOutput sets the output from the function call.
func WithCallResultOutput(output proto.Message) CallResultOption {
	return func(result *CallResult) { result.output = output }
}

// WithCallResultError sets the error from the function call.
func WithCallResultError(err Error) CallResultOption {
	return func(result *CallResult) { result.proto.Error = err.proto }
}

// WithCallResultID sets the opaque identifier for the function call.
func WithCallResultID(id ID) CallResultOption {
	return func(result *CallResult) { result.proto.DispatchId = id }
}

// CorrelationID is the value that was originally passed in the Call message.
//
// This field is intended to be used by the function to correlate the result
// with the original call.
func (r CallResult) CorrelationID() uint64 {
	return r.proto.GetCorrelationId()
}

// Output is output from the function.
func (r CallResult) Output() (proto.Message, error) {
	output := r.proto.GetOutput()
	if output == nil {
		return nil, nil
	}
	return output.UnmarshalNew()
}

// Error is the error that occurred during execution of the function.
//
// It is valid to have both an output and an error, in which case the output
// might contain a partial result.
func (r CallResult) Error() (Error, bool) {
	e := r.proto.GetError()
	return Error{e}, e != nil
}

// ID is the opaque identifier for the function call.
func (r CallResult) ID() ID {
	return r.proto.GetDispatchId()
}

// String is the string representation of the function call result.
func (r CallResult) String() string {
	return fmt.Sprintf("CallResult(%s)", r.proto)
}

// Equal is true if the call result is equal to another.
func (r CallResult) Equal(other CallResult) bool {
	if r.proto == nil || other.proto == nil {
		return false
	}
	if r.CorrelationID() != other.CorrelationID() {
		return false
	}
	if r.ID() != other.ID() {
		return false
	}
	output, err := r.Output()
	if err != nil {
		return false
	}
	otherOutput, err := other.Output()
	if err != nil {
		return false
	}
	if (output == nil) != (otherOutput == nil) {
		return false
	}
	if output != nil && !proto.Equal(output, otherOutput) {
		return false
	}
	if (r.proto.GetError() == nil) != (other.proto.GetError() == nil) {
		return false
	}
	if error, ok := r.Error(); ok {
		if otherError, _ := other.Error(); !error.Equal(otherError) {
			return false
		}
	}
	return true
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
func WithErrorValue(value []byte) ErrorOption {
	return func(e *Error) { e.proto.Value = value }
}

// WithErrorTraceback sets the encoded stack trace for the error.
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
//
// This is used to enable propagation of the error value between
// instances of a program, by encoding information allowing the error
// value to be reconstructed.
func (e Error) Value() []byte {
	return e.proto.GetValue()
}

// Traceback is the encoded stack trace for the error.
//
// The format is language-specific, encoded in the standard format used by
// each programming language to represent stack traces. Not all languages have
// stack traces for errors, so in some cases the value might be omitted.
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

// Exit is a directive that terminates a function call.
type Exit struct {
	proto *sdkv1.Exit
}

// NewExit creates an Exit directive.
func NewExit(opts ...ExitOption) Exit {
	exit := Exit{&sdkv1.Exit{
		Result: &sdkv1.CallResult{},
	}}
	for _, opt := range opts {
		opt(&exit)
	}
	return exit
}

// ExitOption configures an Exit directive.
type ExitOption func(*Exit)

// WithExitResult sets the result of the function call.
func WithExitResult(result CallResult) ExitOption {
	return func(e *Exit) { e.proto.Result = result.proto }
}

// WithTailCall sets the tail call.
func WithTailCall(tailCall Call) ExitOption {
	return func(e *Exit) { e.proto.TailCall = tailCall.proto }
}

// Result is the function call result the exit directive carries.
func (e Exit) Result() (CallResult, bool) {
	r := e.proto.GetResult()
	return CallResult{proto: r}, r != nil
}

// TailCall is the tail call the exit directive carries.
func (e Exit) TailCall() (Call, bool) {
	c := e.proto.GetTailCall()
	return Call{proto: c}, c != nil
}

// String is the string representation of the Exit directive.
func (e Exit) String() string {
	return fmt.Sprintf("Exit(%s)", e.proto)
}

// Equal is true if an Exit directive is equal to another.
func (e Exit) Equal(other Exit) bool {
	result, ok := e.Result()
	otherResult, otherOk := other.Result()
	if ok != otherOk {
		return false
	} else if ok && !result.Equal(otherResult) {
		return false
	}
	tailCall, ok := e.TailCall()
	otherTailCall, otherOk := other.TailCall()
	if ok != otherOk {
		return false
	} else if ok && !tailCall.Equal(otherTailCall) {
		return false
	}
	return true
}

// Poll is a general purpose directive used to spawn
// function calls and wait for their results, and/or
// to implement sleep/timer functionality.
type Poll struct {
	proto *sdkv1.Poll
}

// NewPoll creates a Poll directive.
func NewPoll(minResults, maxResults int32, maxWait time.Duration, opts ...PollOption) Poll {
	poll := Poll{&sdkv1.Poll{
		MinResults: int32(minResults),
		MaxResults: int32(maxResults),
		MaxWait:    durationpb.New(maxWait),
	}}
	for _, opt := range opts {
		opt(&poll)
	}
	return poll
}

// PollOption configures a Poll directive.
type PollOption func(*Poll)

// WithPollCoroutineState sets the coroutine state.
func WithPollCoroutineState(state []byte) PollOption {
	return func(p *Poll) { p.proto.CoroutineState = state }
}

// WithPollCalls adds calls to a Poll directive.
func WithPollCalls(calls ...Call) PollOption {
	return func(p *Poll) {
		for i := range calls {
			p.proto.Calls = append(p.proto.Calls, calls[i].proto)
		}
	}
}

// MinResults is the minimum number of call results to wait for before the
// function is resumed.
//
// The function will be suspended until either MinResults are available,
// or the MaxWait timeout is reached, whichever comes first.
func (p Poll) MinResults() int32 {
	return p.proto.GetMinResults()
}

// MaxResults is the maximum number of call results to deliver in the
// PollResult.
func (p Poll) MaxResults() int32 {
	return p.proto.GetMaxResults()
}

// MaxWait is the maximum amount of time the function should be suspended for
// while waiting for call results.
func (p Poll) MaxWait() time.Duration {
	return p.proto.GetMaxWait().AsDuration()
}

// CoroutineState is a snapshot of the function's state.
//
// It's passed back in the PollResult when the function is resumed.
func (p Poll) CoroutineState() []byte {
	return p.proto.GetCoroutineState()
}

// Calls are the function calls attached to the poll directive.
func (p Poll) Calls() []Call {
	raw := p.proto.GetCalls()
	if len(raw) == 0 {
		return nil
	}
	calls := make([]Call, len(raw))
	for i, proto := range raw {
		calls[i] = Call{proto}
	}
	return calls
}

// String is the string representation of the poll directive.
func (p Poll) String() string {
	return fmt.Sprintf("Poll(%s)", p.proto)
}

// Equal is true if the poll directive is equal to another.
func (p Poll) Equal(other Poll) bool {
	calls := p.Calls()
	otherCalls := other.Calls()
	if len(calls) != len(otherCalls) {
		return false
	}
	for i := range calls {
		if !calls[i].Equal(otherCalls[i]) {
			return false
		}
	}
	return p.MinResults() == other.MinResults() &&
		p.MaxResults() == other.MaxResults() &&
		p.MaxWait() == other.MaxWait() &&
		bytes.Equal(p.CoroutineState(), other.CoroutineState())
}
