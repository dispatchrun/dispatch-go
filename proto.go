package dispatch

import (
	"fmt"
	"time"
	_ "unsafe"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Call is a function call.
type Call struct {
	proto *sdkv1.Call
}

// NewCall creates a Call.
func NewCall(endpoint, function string, opts ...CallOption) Call {
	call := Call{&sdkv1.Call{
		Endpoint: endpoint,
		Function: function,
	}}
	for _, opt := range opts {
		opt.configureCall(&call)
	}
	return call
}

// CallOption configures a Call.
type CallOption interface{ configureCall(*Call) }

type callOptionFunc func(*Call)

func (fn callOptionFunc) configureCall(c *Call) { fn(c) }

// Input sets the output from a function call or Response.
func Input(input Any) interface {
	CallOption
	RequestOption
} {
	return inputOption(input)
}

type inputOption Any

func (i inputOption) configureCall(r *Call) { r.proto.Input = i.proto }

func (i inputOption) configureRequest(r *Request) {
	r.proto.Directive = &sdkv1.RunRequest_Input{Input: i.proto}
}

// Expiration sets a function call expiration.
func Expiration(expiration time.Duration) CallOption {
	return callOptionFunc(func(c *Call) { c.proto.Expiration = durationpb.New(expiration) })
}

// CorrelationID sets the correlation ID on a function call or result.
func CorrelationID(correlationID uint64) interface {
	CallOption
	CallResultOption
} {
	return correlationIDOption(correlationID)
}

type correlationIDOption uint64

func (id correlationIDOption) configureCall(c *Call)             { c.proto.CorrelationId = uint64(id) }
func (id correlationIDOption) configureCallResult(r *CallResult) { r.proto.CorrelationId = uint64(id) }

// Version sets a function call version.
func Version(version string) CallOption {
	return callOptionFunc(func(c *Call) { c.proto.Version = version })
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
func (c Call) Input() Any {
	input := c.proto.GetInput()
	return Any{input}
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
	return proto.Equal(c.proto, other.proto)
}

// CallResult is a function call result.
type CallResult struct {
	proto *sdkv1.CallResult
}

// NewCallResult creates a CallResult.
func NewCallResult(opts ...CallResultOption) CallResult {
	result := CallResult{&sdkv1.CallResult{}}
	for _, opt := range opts {
		opt.configureCallResult(&result)
	}
	return result
}

// CallResultOption configures a CallResult.
type CallResultOption interface{ configureCallResult(*CallResult) }

// Output sets the output from a function call or Response.
func Output(output Any) interface {
	CallResultOption
	ExitOption
	ResponseOption
} {
	return outputOption(output)
}

type outputOption Any

func (o outputOption) configureCallResult(r *CallResult) { r.proto.Output = o.proto }

func (o outputOption) configureExit(x *Exit) {
	if x.proto.Result == nil {
		x.proto.Result = &sdkv1.CallResult{}
	}
	x.proto.Result.Output = o.proto
}

func (o outputOption) configureResponse(r *Response) { ensureResponseExitResult(r).Output = o.proto }

// DispatchID sets the opaque identifier for the function call.
func DispatchID(id ID) interface {
	CallResultOption
	RequestOption
} {
	return dispatchIDOption(id)
}

type dispatchIDOption ID

func (id dispatchIDOption) configureCallResult(r *CallResult) { r.proto.DispatchId = string(id) }
func (id dispatchIDOption) configureRequest(r *Request)       { r.proto.DispatchId = string(id) }

// CorrelationID is the value that was originally passed in the Call message.
//
// This field is intended to be used by the function to correlate the result
// with the original call.
func (r CallResult) CorrelationID() uint64 {
	return r.proto.GetCorrelationId()
}

// Output is output from the function.
func (r CallResult) Output() (Any, bool) {
	output := r.proto.GetOutput()
	return Any{output}, output != nil
}

// Error is the error that occurred during execution of the function.
//
// It is valid to have both an output and an error, in which case the output
// might contain a partial result.
func (r CallResult) Error() (Error, bool) {
	proto := r.proto.GetError()
	return Error{proto}, proto != nil
}

// DispatchID is the opaque identifier for the function call.
func (r CallResult) DispatchID() ID {
	return ID(r.proto.GetDispatchId())
}

// String is the string representation of the function call result.
func (r CallResult) String() string {
	return fmt.Sprintf("CallResult(%s)", r.proto)
}

// Equal is true if the call result is equal to another.
func (r CallResult) Equal(other CallResult) bool {
	return proto.Equal(r.proto, other.proto)
}

func (r CallResult) configureExit(e *Exit) {
	e.proto.Result = r.proto
}

// Error is an error that occurred during execution of a function.
type Error struct {
	proto *sdkv1.Error
}

// NewError creates an Error from a Go error.
func NewError(err error) Error {
	// TODO: use ErrorValue / Traceback
	return NewErrorMessage(errorTypeOf(err), err.Error())
}

// NewErrorMessage creates an Error.
func NewErrorMessage(typ, message string, opts ...ErrorOption) Error {
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

// ErrorValue sets the language-specific representation of the error.
func ErrorValue(value []byte) ErrorOption {
	return func(e *Error) { e.proto.Value = value }
}

// Traceback sets the encoded stack trace for the error.
func Traceback(traceback []byte) ErrorOption {
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
	return proto.Equal(e.proto, other.proto)
}

func (e Error) configureCallResult(r *CallResult) {
	r.proto.Error = e.proto
}

func (e Error) configurePollResult(p *PollResult) {
	p.proto.Error = e.proto
}

func (e Error) configureExit(x *Exit) {
	if x.proto.Result == nil {
		x.proto.Result = &sdkv1.CallResult{}
	}
	x.proto.Result.Error = e.proto
}

func (e Error) configureResponse(r *Response) {
	ensureResponseExitResult(r).Error = e.proto
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
		opt.configureExit(&exit)
	}
	return exit
}

// ExitOption configures an Exit directive.
type ExitOption interface{ configureExit(*Exit) }

type exitOptionFunc func(*Exit)

func (fn exitOptionFunc) configureExit(e *Exit) { fn(e) }

// TailCall sets the tail call.
func TailCall(tailCall Call) ExitOption {
	return exitOptionFunc(func(e *Exit) { e.proto.TailCall = tailCall.proto })
}

// Result is the function call result the exit directive carries.
func (e Exit) Result() (CallResult, bool) {
	proto := e.proto.GetResult()
	return CallResult{proto}, proto != nil
}

// Error is the error from the function call result the
// exit directive carries.
func (e Exit) Error() (Error, bool) {
	result, ok := e.Result()
	if !ok {
		return Error{}, false
	}
	return result.Error()
}

// Output is the output from the function call result the
// exit directive carries.
func (e Exit) Output() (Any, bool) {
	result, ok := e.Result()
	if !ok {
		return Any{}, false
	}
	return result.Output()
}

// TailCall is the tail call the exit directive carries.
func (e Exit) TailCall() (Call, bool) {
	proto := e.proto.GetTailCall()
	return Call{proto}, proto != nil
}

// String is the string representation of the Exit directive.
func (e Exit) String() string {
	return fmt.Sprintf("Exit(%s)", e.proto)
}

// Equal is true if an Exit directive is equal to another.
func (e Exit) Equal(other Exit) bool {
	return proto.Equal(e.proto, other.proto)
}

func (e Exit) configureResponse(r *Response) {
	r.proto.Directive = &sdkv1.RunResponse_Exit{Exit: e.proto}
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
		opt.configurePoll(&poll)
	}
	return poll
}

// PollOption configures a Poll directive.
type PollOption interface{ configurePoll(*Poll) }

type pollOptionFunc func(*Poll)

func (fn pollOptionFunc) configurePoll(p *Poll) { fn(p) }

// CoroutineState sets the coroutine state.
func CoroutineState(state Any) interface {
	PollOption
	PollResultOption
	ResponseOption
} {
	return coroutineStateOption(state)
}

type coroutineStateOption Any

func (s coroutineStateOption) configurePoll(p *Poll) {
	p.proto.State = &sdkv1.Poll_TypedCoroutineState{TypedCoroutineState: s.proto}
}

func (s coroutineStateOption) configurePollResult(r *PollResult) {
	r.proto.State = &sdkv1.PollResult_TypedCoroutineState{TypedCoroutineState: s.proto}
}

func (s coroutineStateOption) configureResponse(r *Response) {
	switch d := r.proto.GetDirective().(type) {
	case *sdkv1.RunResponse_Poll:
		d.Poll.State = &sdkv1.Poll_TypedCoroutineState{TypedCoroutineState: s.proto}
	} // noop otherwise
}

// Calls adds calls to a Poll directive.
func Calls(calls ...Call) PollOption {
	return pollOptionFunc(func(p *Poll) {
		for i := range calls {
			p.proto.Calls = append(p.proto.Calls, calls[i].proto)
		}
	})
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
func (p Poll) CoroutineState() Any {
	return Any{p.proto.GetTypedCoroutineState()}
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
	return proto.Equal(p.proto, other.proto)
}

func (p Poll) configureResponse(r *Response) {
	r.proto.Directive = &sdkv1.RunResponse_Poll{Poll: p.proto}
}

// PollResult is the result of a poll operation.
type PollResult struct {
	proto *sdkv1.PollResult
}

// NewPollResult creates a PollResult
func NewPollResult(opts ...PollResultOption) PollResult {
	result := PollResult{&sdkv1.PollResult{}}
	for _, opt := range opts {
		opt.configurePollResult(&result)
	}
	return result
}

// PollResultOption configures a PollResult.
type PollResultOption interface{ configurePollResult(*PollResult) }

type pollResultOptionFunc func(*PollResult)

func (fn pollResultOptionFunc) configurePollResult(r *PollResult) { fn(r) }

// CallResults sets the call results for the poll operation.
func CallResults(results ...CallResult) PollResultOption {
	return pollResultOptionFunc(func(r *PollResult) {
		for i := range results {
			r.proto.Results = append(r.proto.Results, results[i].proto)
		}
	})
}

// CoroutineState is the state recorded when the function was
// suspended while polling.
func (r PollResult) CoroutineState() Any {
	return Any{r.proto.GetTypedCoroutineState()}
}

// Results are the function call results attached to the poll result.
func (r PollResult) Results() []CallResult {
	raw := r.proto.GetResults()
	if len(raw) == 0 {
		return nil
	}
	results := make([]CallResult, len(raw))
	for i, proto := range raw {
		results[i] = CallResult{proto}
	}
	return results
}

// Error is an error that occured while processing a Poll directive.
//
// An error indicates that none of the calls were dispatched, and must be
// resubmitted after the error cause has been resolved.
func (r PollResult) Error() (Error, bool) {
	proto := r.proto.GetError()
	return Error{proto}, proto != nil
}

// String is the string representation of the poll result.
func (r PollResult) String() string {
	return fmt.Sprintf("PollResult(%s)", r.proto)
}

// Equal is true if the poll result is equal to another.
func (r PollResult) Equal(other PollResult) bool {
	return proto.Equal(r.proto, other.proto)
}

func (r PollResult) configureRequest(req *Request) {
	req.proto.Directive = &sdkv1.RunRequest_PollResult{PollResult: r.proto}
}

// Request is a request from Dispatch to run a function.
//
// The Request carries a "directive", to either start execution
// with input (Input), or to resume execution with the results
// of a previous Response directive (e.g. PollResult).
type Request struct {
	proto *sdkv1.RunRequest
}

// NewRequest creates a Request.
func NewRequest(function string, opts ...RequestOption) Request {
	request := Request{&sdkv1.RunRequest{
		Function: function,
	}}
	for _, opt := range opts {
		opt.configureRequest(&request)
	}
	return request
}

// RequestOption configures a Request.
type RequestOption interface{ configureRequest(*Request) }

type requestOptionFunc func(*Request)

func (fn requestOptionFunc) configureRequest(r *Request) { fn(r) }

// ParentDispatchID sets the opaque identifier of the parent function call.
func ParentDispatchID(id ID) RequestOption {
	return requestOptionFunc(func(r *Request) { r.proto.ParentDispatchId = string(id) })
}

// ParentDispatchID sets the opaque identifier of the root function call.
func RootDispatchID(id ID) RequestOption {
	return requestOptionFunc(func(r *Request) { r.proto.RootDispatchId = string(id) })
}

// CreationTime sets the creation time for the function call.
func CreationTime(timestamp time.Time) RequestOption {
	return requestOptionFunc(func(r *Request) { r.proto.CreationTime = timestamppb.New(timestamp) })
}

// ExpirationTime sets the expiration time for the function call.
func ExpirationTime(timestamp time.Time) RequestOption {
	return requestOptionFunc(func(r *Request) { r.proto.ExpirationTime = timestamppb.New(timestamp) })
}

// Function is the identifier of the function to run.
func (r Request) Function() string {
	return r.proto.GetFunction()
}

// Input is input to the function, along with a boolean
// flag that indicates whether the request carries a directive
// to start the function with the input.
func (r Request) Input() (Any, bool) {
	proto := r.proto.GetInput()
	return Any{proto}, proto != nil
}

// PollResult is the poll result, along with a boolean
// flag that indicates whether the request carries a directive
// to resume a function with poll results.
func (r Request) PollResult() (PollResult, bool) {
	proto := r.proto.GetPollResult()
	return PollResult{proto}, proto != nil
}

// DispatchID is the opaque identifier for the function call.
func (r Request) DispatchID() ID {
	return ID(r.proto.GetDispatchId())
}

// ParentID is the opaque identifier for the parent function call.
//
// Functions can call other functions via Poll. If this function call
// has a parent function call, the identifier of the parent can be found
// here. If the function call does not have a parent, the field will
// be empty.
func (r Request) ParentID() ID {
	return ID(r.proto.GetParentDispatchId())
}

// RootID is the opaque identifier for the root function call.
//
// When functions call other functions, an additional level on the call
// hierarchy tree is created. This field carries the identifier of the
// root function call in the tree.
func (r Request) RootID() ID {
	return ID(r.proto.GetRootDispatchId())
}

// CreationTime is the creation time of the function call.
func (r Request) CreationTime() (time.Time, bool) {
	return r.optionalTimestamp(r.proto.GetCreationTime())
}

// ExpirationTime is the expiration time of the function call.
func (r Request) ExpirationTime() (time.Time, bool) {
	return r.optionalTimestamp(r.proto.GetExpirationTime())
}

func (r Request) optionalTimestamp(ts *timestamppb.Timestamp) (time.Time, bool) {
	if ts != nil {
		t := ts.AsTime()
		return t, ts.IsValid() && !t.IsZero()
	}
	return time.Time{}, false
}

// String is the string representation of the request.
func (r Request) String() string {
	return fmt.Sprintf("Request(%s)", r.proto)
}

// Equal is true if the request is equal to another.
func (r Request) Equal(other Request) bool {
	return proto.Equal(r.proto, other.proto)
}

// Response is a response to Dispatch after a function has run.
//
// The Response carries a "directive" to either terminate execution
// (Exit), or to suspend the function while waiting and/or performing
// operations on the Dispatch side (e.g. Poll).
type Response struct {
	proto *sdkv1.RunResponse
}

// ResponseOption configures a Response.
type ResponseOption interface{ configureResponse(*Response) }

// NewResponse creates a Response.
func NewResponse(status Status, opts ...ResponseOption) Response {
	response := Response{&sdkv1.RunResponse{
		Status: sdkv1.Status(status),
	}}
	for _, opt := range opts {
		opt.configureResponse(&response)
	}
	if response.proto.Directive == nil {
		ensureResponseExitResult(&response)
	}
	return response
}

// NewResponseError creates a Response from the specified error.
func NewResponseError(err error) Response {
	return NewResponse(ErrorStatus(err), NewError(err))
}

// NewResponseErrorf creates a Response from the specified error message
// and args.
func NewResponseErrorf(msg string, args ...any) Response {
	return NewResponseError(fmt.Errorf(msg, args...))
}

// Status is the response status.
func (r Response) Status() Status {
	return Status(r.proto.GetStatus())
}

// Exit is the exit directive on the response.
func (r Response) Exit() (Exit, bool) {
	proto := r.proto.GetExit()
	return Exit{proto}, proto != nil
}

// Error is the error from the exit directive attached to the response.
func (r Response) Error() (Error, bool) {
	exit, ok := r.Exit()
	if !ok {
		return Error{}, false
	}
	return exit.Error()
}

// Output is the output from an exit directive attached to the response.
func (r Response) Output() (Any, bool) {
	exit, ok := r.Exit()
	if !ok {
		return Any{}, false
	}
	return exit.Output()
}

// Poll is the poll directive on the response.
func (r Response) Poll() (Poll, bool) {
	proto := r.proto.GetPoll()
	return Poll{proto}, proto != nil
}

// String is the string representation of the response.
func (r Response) String() string {
	return fmt.Sprintf("Response(%s)", r.proto)
}

// Equal is true if the response is equal to another.
func (r Response) Equal(other Response) bool {
	return proto.Equal(r.proto, other.proto)
}

// Marshal marshals the response.
func (r Response) Marshal() ([]byte, error) {
	return proto.Marshal(r.proto)
}

func ensureResponseExitResult(r *Response) *sdkv1.CallResult {
	var d *sdkv1.RunResponse_Exit
	d, ok := r.proto.Directive.(*sdkv1.RunResponse_Exit)
	if !ok {
		d = &sdkv1.RunResponse_Exit{}
		r.proto.Directive = d
	}
	if d.Exit == nil {
		d.Exit = &sdkv1.Exit{}
	}
	if d.Exit.Result == nil {
		d.Exit.Result = &sdkv1.CallResult{}
	}
	return d.Exit.Result
}

// RequestDirective is a Dispatch request directive, used by Dispatch
// as it yields control to a coroutine.
type RequestDirective interface {
	RequestOption

	requestDirective()
}

func (PollResult) requestDirective() {}

// ResponseDirective is a Dispatch response directive, used by coroutines
// as they yield control to Dispatch.
type ResponseDirective interface {
	ResponseOption

	responseDirective()
}

func (Exit) responseDirective() {}
func (Poll) responseDirective() {}

// These are hooks used by the dispatchlambda and dispatchtest
// package that let us avoid exposing proto messages. Exposing
// the underlying proto messages complicates the API and opens
// up new failure modes.

//go:linkname newProtoCall
func newProtoCall(proto *sdkv1.Call) Call { //nolint
	return Call{proto}
}

//go:linkname newProtoResponse
func newProtoResponse(proto *sdkv1.RunResponse) Response { //nolint
	return Response{proto}
}

//go:linkname newProtoRequest
func newProtoRequest(proto *sdkv1.RunRequest) Request { //nolint
	return Request{proto}
}

//go:linkname requestProto
func requestProto(r Request) *sdkv1.RunRequest { //nolint
	return r.proto
}

//go:linkname responseProto
func responseProto(r Response) *sdkv1.RunResponse { //nolint
	return r.proto
}
