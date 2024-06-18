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

	instances map[instanceID]coroutine.Coroutine[CoroR[O], CoroS]
	nextID    instanceID
	mu        sync.Mutex
}

type instanceID = int

// Run runs the coroutine function.
func (c *GenericCoroutine[I, O]) Run(ctx context.Context, req Request) Response {
	if name := req.Function(); name != c.name {
		return NewResponseErrorf("%w: function %q received call for function %q", ErrInvalidArgument, c.name, name)
	}

	var id instanceID
	var coro coroutine.Coroutine[CoroR[O], CoroS]

	if _, ok := req.Input(); ok {
		// Start a new coroutine if the request carries function input.
		input, err := c.unpackInput(req)
		if err != nil {
			return NewResponseError(err)
		}
		id, coro = c.setup(input)

	} else if pollResult, ok := req.PollResult(); ok {
		// Otherwise, resume a coroutine that is suspended.
		var err error
		id, coro, err = c.deserialize(pollResult.CoroutineState())
		if err != nil {
			return NewResponseError(err)
		}

		// Send poll results to the coroutine.
		coro.Send(CoroS{directive: pollResult})
	}

	// Tidy up the coroutine when returning.
	defer c.tearDown(id, coro)

	// Run the coroutine until it yields or returns.
	if coro.Next() {
		// The coroutine yielded and is now paused.
		// Serialize the coroutine.
		state, err := c.serialize(id, coro)
		if err != nil {
			return NewResponseError(err)
		}
		// Yield to Dispatch with the directive from the coroutine.
		result := coro.Recv()
		return NewResponse(result.status, result.directive, CoroutineState(state))
	}

	// The coroutine returned. Serialize the output / error.
	result := coro.Result()
	if result.err != nil {
		// TODO: serialize the output too if present
		return NewResponseError(result.err)
	}
	return c.packOutput(result.output)
}

func (c *GenericCoroutine[I, O]) setup(input I) (instanceID, coroutine.Coroutine[CoroR[O], CoroS]) {
	var id instanceID
	coro := coroutine.NewWithReturn[CoroR[O], CoroS](c.entrypoint(input))

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
			c.instances = map[instanceID]coroutine.Coroutine[CoroR[O], CoroS]{}
		}
		c.instances[id] = coro
	}

	return id, coro
}

func (c *GenericCoroutine[I, O]) tearDown(id instanceID, coro coroutine.Coroutine[CoroR[O], CoroS]) {
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

func (c *GenericCoroutine[I, O]) serialize(id instanceID, coro coroutine.Coroutine[CoroR[O], CoroS]) (Any, error) {
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

func (c *GenericCoroutine[I, O]) deserialize(state Any) (instanceID, coroutine.Coroutine[CoroR[O], CoroS], error) {
	var id instanceID
	var coro coroutine.Coroutine[CoroR[O], CoroS]

	// Deserialize durable coroutine state.
	if coroutine.Durable {
		var zero I
		coro = coroutine.NewWithReturn[CoroR[O], CoroS](c.entrypoint(zero))
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

func (c *GenericCoroutine[I, O]) entrypoint(input I) func() CoroR[O] {
	return func() CoroR[O] {
		// The context that gets passed as argument here should be recreated
		// each time the coroutine is resumed, ideally inheriting from the
		// parent context passed to the Run method. This is difficult to
		// do right in durable mode because we shouldn't capture the parent
		// context in the coroutine state.
		var r CoroR[O]
		r.output, r.err = c.fn(context.TODO(), input)
		return r
	}
}

type CoroS struct {
	directive RequestDirective
}

type CoroR[O any] struct {
	status    Status
	directive ResponseDirective

	output O
	err    error
}

// Yield yields control to Dispatch.
//
// The coroutine is paused, serialized and sent to Dispatch. The
// directive instructs Dispatch to perform an operation while
// the coroutine is suspended. Once the operation is complete,
// Dispatch yields control back to the coroutine, which is resumed
// from the point execution was suspended.
func Yield[O any](status Status, directive ResponseDirective) RequestDirective {
	result := coroutine.Yield[CoroR[O], CoroS](CoroR[O]{
		status:    status,
		directive: directive,
	})
	return result.directive
}
