//go:build !durable

package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dispatchrun/coroutine"
)

const durableCoroutineStateTypeUrl = "buf.build/stealthrocket/coroutine/coroutine.v1.State"

// NewFunction creates a Dispatch Function.
func NewFunction[I, O any](name string, fn func(context.Context, I) (O, error)) *Coroutine[I, O] {
	return &Coroutine[I, O]{
		PrimitiveFunction: PrimitiveFunction{name: name},
		fn:                fn,
	}
}

// Coroutine is a Dispatch Function that accepts any input and returns any output,
// and that can be suspended during execution.
type Coroutine[I, O any] struct {
	PrimitiveFunction

	fn func(ctx context.Context, input I) (O, error)

	instances map[coroutineID]dispatchCoroutine
	nextID    coroutineID
	mu        sync.Mutex
}

// coroutineID is an identifier for a coroutine instance.
// "Instances" are only applicable when coroutines are running
// in volatile mode, since we must be keep suspended coroutines in
// memory while they're polling. In durable mode, there's no need
// to keep "instances" around, since we can serialize the state of
// each coroutine and send it back and forth to Dispatch. In durable
// mode the GenericCoroutine is stateless.
type coroutineID = int64

// dispatchCoroutine is the flavour of coroutine we support here.
type dispatchCoroutine = coroutine.Coroutine[Response, Request]

// Run runs the function.
func (c *Coroutine[I, O]) Run(ctx context.Context, req Request) Response {
	if name := req.Function(); name != c.name {
		return NewResponseErrorf("%w: function %q received call for function %q", ErrInvalidArgument, c.name, name)
	}

	// Create or deserialize the coroutine (depending on the type of request).
	id, coro, err := c.setUp(req)
	if err != nil {
		return NewResponseError(err)
	}
	defer c.tearDown(id, coro)

	// Send results from Dispatch to the coroutine (if applicable).
	coro.Send(req)

	// Run the coroutine until it yields or returns.
	if returned := !coro.Next(); returned {
		return coro.Result()
	}
	yield := coro.Recv()

	// If the coroutine explicitly exited, stop it before returning to Dispatch.
	// There's no need to serialize the coroutine state in this case; it's done.
	if _, exit := yield.Exit(); exit {
		coro.Stop()
		coro.Next()
		return yield
	}

	// For all other response directives, serialize the coroutine state before
	// yielding to Dispatch so that the coroutine can be resumed from the yield
	// point.
	state, err := c.serialize(id, coro)
	if err != nil {
		return NewResponseError(err)
	}
	return yield.With(CoroutineState(state))
}

// NewCall creates a Call for the function.
func (f *Coroutine[I, O]) NewCall(input I, opts ...CallOption) (Call, error) {
	boxedInput, err := NewAny(input)
	if err != nil {
		return Call{}, fmt.Errorf("cannot serialize input: %v", err)
	}
	return f.PrimitiveFunction.NewCall(boxedInput, opts...)
}

// Dispatch dispatches a Call to the function.
func (f *Coroutine[I, O]) Dispatch(ctx context.Context, input I, opts ...CallOption) (ID, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return "", err
	}
	return f.dispatchCall(ctx, call)
}

func (c *Coroutine[I, O]) setUp(req Request) (coroutineID, dispatchCoroutine, error) {
	// If the request carries a poll result, find (in a volatile mode) or
	// deserialize (in durable mode) the coroutine.
	if pollResult, ok := req.PollResult(); ok {
		return c.deserialize(pollResult.CoroutineState())
	}

	// If the request carries input, create a coroutine.
	var coro dispatchCoroutine
	var input I
	boxedInput, ok := req.Input()
	if !ok {
		return 0, coro, fmt.Errorf("%w: unsupported request: %v", ErrInvalidArgument, req)
	}
	if err := boxedInput.Unmarshal(&input); err != nil {
		return 0, coro, fmt.Errorf("%w: invalid input %v: %v", ErrInvalidArgument, boxedInput, err)
	}
	id, coro := c.create(input)
	return id, coro, nil
}

func (c *Coroutine[I, O]) create(input I) (coroutineID, dispatchCoroutine) {
	var id coroutineID
	coro := coroutine.NewWithReturn[Response, Request](c.entrypoint(input))

	// In volatile mode, we need to create an "instance" of the coroutine that
	// resides in memory.
	if !coroutine.Durable {
		c.mu.Lock()
		defer c.mu.Unlock()

		// Give the instance a unique ID so that we can later find it
		// when resuming execution.
		c.nextID++
		id = c.nextID
		if c.instances == nil {
			c.instances = map[coroutineID]dispatchCoroutine{}
		}
		c.instances[id] = coro
	}

	return id, coro
}

func (c *Coroutine[I, O]) tearDown(id coroutineID, coro dispatchCoroutine) {
	// Always tear down durable coroutines. They'll be rebuilt
	// on the next call (if applicable) from their serialized state,
	// possibly in a new location.
	if coroutine.Durable && !coro.Done() {
		coro.Stop()
		coro.Next()
	}

	// Remove volatile coroutine instances only once they're done.
	if !coroutine.Durable && coro.Done() {
		c.mu.Lock()
		defer c.mu.Unlock()

		delete(c.instances, id)
	}
}

func (c *Coroutine[I, O]) serialize(id coroutineID, coro dispatchCoroutine) (Any, error) {
	// In volatile mode, serialize a reference to the coroutine instance.
	if !coroutine.Durable {
		return Int(id), nil
	}

	// In durable mode, we serialize the entire state of the coroutine.
	rawState, err := coro.Context().Marshal()
	if err != nil {
		return Any{}, fmt.Errorf("%w: marshal state: %v", ErrPermanent, err)
	}
	state := newAnyTypeValue(durableCoroutineStateTypeUrl, rawState)
	return state, nil
}

func (c *Coroutine[I, O]) deserialize(state Any) (coroutineID, dispatchCoroutine, error) {
	var id coroutineID
	var coro dispatchCoroutine

	// Deserialize durable coroutine state.
	if coroutine.Durable {
		var zero I
		coro = coroutine.NewWithReturn[Response, Request](c.entrypoint(zero))
		if state.TypeURL() != durableCoroutineStateTypeUrl {
			return 0, coro, fmt.Errorf("%w: unexpected type URL: %q", ErrIncompatibleState, state.TypeURL())
		} else if err := coro.Context().Unmarshal(state.Value()); err != nil {
			return 0, coro, fmt.Errorf("%w: unmarshal state: %v", ErrIncompatibleState, err)
		}
		return id, coro, nil
	}

	// In volatile mode, find the suspended coroutine instance.
	if err := state.Unmarshal(&id); err != nil {
		return 0, coro, fmt.Errorf("%w: invalid volatile coroutine reference: %s", ErrIncompatibleState, state)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var ok bool
	coro, ok = c.instances[id]
	if !ok {
		return 0, coro, fmt.Errorf("%w: volatile coroutine %d", ErrNotFound, id)
	}
	return id, coro, nil
}

func (c *Coroutine[I, O]) Coroutine() bool {
	return true
}

// Close closes the coroutine.
//
// In volatile mode, Close destroys all running instances of the coroutine.
// In durable mode, Close is a noop.
func (c *Coroutine[I, O]) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, fn := range c.instances {
		fn.Stop()
		fn.Next()
	}
	clear(c.instances)
	return nil
}

func (c *Coroutine[I, O]) entrypoint(input I) func() Response {
	return func() Response {
		// The context that gets passed as argument here should be recreated
		// each time the coroutine is resumed, ideally inheriting from the
		// parent context passed to the Run method. This is difficult to
		// do right in durable mode because we shouldn't capture the parent
		// context in the coroutine state.
		output, err := c.fn(context.TODO(), input)
		if err != nil {
			// TODO: include output if not nil
			return NewResponseError(err)
		}
		boxedOutput, err := NewAny(output)
		if err != nil {
			return NewResponseErrorf("%w: invalid output %v: %v", ErrInvalidResponse, output, err)
		}
		return NewResponse(StatusOf(output), boxedOutput)
	}
}

// Yield yields control to Dispatch.
//
// The coroutine is suspended while the Response is sent to Dispatch.
// If the Response carries a directive to perform work, Dispatch will
// send the results back in a Request and resume execution from this
// point.
func Yield(res Response) Request {
	return coroutine.Yield[Response, Request](res)
}

// Await awaits the results of calls.
func Await(strategy AwaitStrategy, calls ...Call) ([]CallResult, error) {
	if len(calls) == 0 {
		return nil, nil
	}

	// Assign a correlation ID to each call, and map to the index
	// in the provided set of []Call.
	//
	// The reason we use a random starting correlation ID, rather than
	// the index of each Call, is that Dispatch has at-least once execution
	// guarantees and may rarely deliver a call result from a previous Await
	// operation. Using random correlation ID helps guard against this.
	nextCorrelationID := rand.Uint64()
	pending := map[uint64]int{}
	for i, call := range calls {
		correlationID := nextCorrelationID
		nextCorrelationID++
		pending[correlationID] = i
		calls[i] = call.With(CorrelationID(correlationID))
	}

	// Set polling configuration. There's no value in waking up the
	// coroutine sooner than when all results are available (by reducing
	// minResults and/or maxWait), since there's no internal concurrency
	// in the Go SDK.
	minResults := len(calls)
	maxResults := len(calls)
	maxWait := 5 * time.Minute

	callResults := make([]CallResult, len(calls))

	// Poll until results available.
	for len(pending) > 0 {
		poll := NewResponse(NewPoll(minResults, maxResults, maxWait, Calls(calls...)))
		res := Yield(poll)

		calls = nil // only submit calls once

		// Unpack poll results.
		pollResult, ok := res.PollResult()
		if !ok {
			return nil, fmt.Errorf("unexpected response when polling: %s", res)
		} else if err, ok := pollResult.Error(); ok {
			return nil, fmt.Errorf("poll error: %w", err)
		}

		// Map call results back to calls.
		var hasSuccess bool
		var hasFailure bool
		for _, result := range pollResult.Results() {
			correlationID := result.CorrelationID()
			i, ok := pending[correlationID]
			if !ok {
				// This can occur due to the at-least once execution
				// guarantees of Dispatch.
				slog.Debug("skipping call result with unknown correlation ID", "call_result", result, "correlation_id", correlationID)
				continue
			}
			callResults[i] = result
			delete(pending, correlationID)

			if _, failed := result.Error(); failed {
				hasFailure = true
			} else {
				hasSuccess = true
			}
		}

		switch {
		case hasFailure && strategy == AwaitAll:
			return callResults, joinErrors(callResults)
		case hasSuccess && strategy == AwaitAny:
			return callResults, nil
		}
	}

	if strategy == AwaitAny && allFailed(callResults) {
		return callResults, joinErrors(callResults)
	}
	return callResults, nil
}

func allFailed(results []CallResult) bool {
	for _, result := range results {
		if _, ok := result.Error(); !ok {
			return false
		}
	}
	return true
}

func joinErrors(results []CallResult) error {
	var errs []error
	for _, result := range results {
		if err, ok := result.Error(); ok {
			errs = append(errs, err)
		}
	}
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return errors.Join(errs...)
	}
}

// AwaitStrategy controls an Await operation.
type AwaitStrategy int

const (
	// AwaitAll instructs Await to wait until all results are available,
	// or any call fails.
	AwaitAll AwaitStrategy = iota

	// AwaitAny instructs Await to wait until any result is available,
	// or all calls fail.
	AwaitAny
)

// Gather awaits the results of calls. It waits until all results
// are available, or any call fails. It unpacks the output value
// from the call result when all calls succeed.
func Gather[O any](calls ...Call) ([]O, error) {
	if len(calls) == 0 {
		return nil, nil
	}

	results, err := Await(AwaitAll, calls...)
	if err != nil {
		return nil, err
	}

	outputs := make([]O, len(calls))
	for i, result := range results {
		if boxedOutput, ok := result.Output(); ok {
			if err := boxedOutput.Unmarshal(&outputs[i]); err != nil {
				return nil, fmt.Errorf("failed to unmarshal call %d output: %w", i, err)
			}
		}
	}
	return outputs, nil
}

// Await calls the function and awaits a result.
//
// Await should only be called within a Dispatch coroutine (created via NewFunction).
func (f *PrimitiveFunction) Await(input Any, opts ...CallOption) (Any, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return Any{}, err
	}

	callResults, err := Await(AwaitAll, call)
	if err != nil {
		return Any{}, err
	}
	callResult := callResults[0]

	output, _ := callResult.Output()
	if err, ok := callResult.Error(); ok {
		return output, err
	}
	return output, nil
}

// Gather makes many concurrent calls to the function and awaits the results.
//
// Gather should only be called within a Dispatch coroutine (created via NewFunction).
func (f *PrimitiveFunction) Gather(inputs []Any, opts ...CallOption) ([]Any, error) {
	calls := make([]Call, len(inputs))
	for i, input := range inputs {
		call, err := f.NewCall(input, opts...)
		if err != nil {
			return nil, err
		}
		calls[i] = call
	}

	callResults, err := Await(AwaitAll, calls...)
	if err != nil {
		return nil, err
	}

	outputs := make([]Any, len(inputs))
	for i, result := range callResults {
		output, _ := result.Output()
		outputs[i] = output
	}
	return outputs, nil
}

// Await calls the function and awaits a result.
//
// Await should only be called within a Dispatch coroutine.
func (c *Coroutine[I, O]) Await(input I, opts ...CallOption) (O, error) {
	var output O

	call, err := c.NewCall(input, opts...)
	if err != nil {
		return output, err
	}
	results, err := Gather[O](call)
	if err != nil {
		return output, err
	}
	return results[0], nil
}

// Gather makes many concurrent calls to the function and awaits the results.
//
// Gather should only be called within a Dispatch coroutine.
func (c *Coroutine[I, O]) Gather(inputs []I, opts ...CallOption) ([]O, error) {
	calls := make([]Call, len(inputs))
	for i, input := range inputs {
		call, err := c.NewCall(input, opts...)
		if err != nil {
			return nil, err
		}
		calls[i] = call
	}
	return Gather[O](calls...)
}
