package dispatch

import (
	"context"
	"fmt"
	"sync"

	"github.com/dispatchrun/coroutine"
)

const durableCoroutineStateTypeUrl = "buf.build/stealthrocket/coroutine/coroutine.v1.State"

// NewCoroutine creates a Dispatch coroutine Function.
func NewCoroutine[I, O any](name string, fn func(context.Context, I) (O, error)) *GenericCoroutine[I, O] {
	return &GenericCoroutine[I, O]{
		GenericFunction: GenericFunction[I, O]{
			PrimitiveFunction{name: name},
			fn,
		},
	}
}

// GenericCoroutine is a Function that accepts any input and returns any output.
type GenericCoroutine[I, O any] struct {
	GenericFunction[I, O]

	instances map[coroutineID]dispatchCoroutine
	nextID    coroutineID
	mu        sync.Mutex
}

type coroutineID = int
type dispatchCoroutine = coroutine.Coroutine[Response, Request]

// Run runs the coroutine function.
func (c *GenericCoroutine[I, O]) Run(ctx context.Context, req Request) Response {
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
	return NewResponse(yield.Status(), yield, CoroutineState(state))
}

func (c *GenericCoroutine[I, O]) setUp(req Request) (id coroutineID, coro dispatchCoroutine, err error) {
	// Start a new coroutine if the request carries function input.
	// Otherwise, resume a coroutine that is suspended.
	if _, ok := req.Input(); ok {
		var input I
		input, err = c.unpackInput(req)
		if err == nil {
			id, coro = c.create(input)
		}
	} else if pollResult, ok := req.PollResult(); ok {
		id, coro, err = c.deserialize(pollResult.CoroutineState())
	}
	return
}

func (c *GenericCoroutine[I, O]) create(input I) (coroutineID, dispatchCoroutine) {
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

func (c *GenericCoroutine[I, O]) tearDown(id coroutineID, coro dispatchCoroutine) {
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

func (c *GenericCoroutine[I, O]) serialize(id coroutineID, coro dispatchCoroutine) (Any, error) {
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

func (c *GenericCoroutine[I, O]) deserialize(state Any) (coroutineID, dispatchCoroutine, error) {
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

func (c *GenericCoroutine[I, O]) Coroutine() bool {
	return true
}

// Close closes the coroutine.
//
// In volatile mode, Close destroys all running instances of the coroutine.
// In durable mode, Close is a noop.
func (c *GenericCoroutine[I, O]) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, fn := range c.instances {
		fn.Stop()
		fn.Next()
	}
	clear(c.instances)
	return nil
}

func (c *GenericCoroutine[I, O]) entrypoint(input I) func() Response {
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
		return c.packOutput(output)
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
