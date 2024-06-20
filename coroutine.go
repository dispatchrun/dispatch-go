//go:build !durable

package dispatch

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/dispatchrun/coroutine"
	"github.com/dispatchrun/dispatch-go/dispatchcoro"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Func creates a Dispatch Function.
func Func[I, O any](name string, fn func(context.Context, I) (O, error)) *Coroutine[I, O] {
	return &Coroutine[I, O]{
		PrimitiveFunction: PrimitiveFunction{name: name},
		fn:                fn,
		nextID:            rand.Uint64(),
	}
}

// Coroutine is a Dispatch Function that accepts any input and returns any output,
// and that can be suspended during execution.
type Coroutine[I, O any] struct {
	PrimitiveFunction

	fn func(ctx context.Context, input I) (O, error)

	instances map[coroutineID]dispatchcoro.Coroutine
	nextID    coroutineID
	mu        sync.Mutex
}

// coroutineID is an identifier for a coroutine instance.
//
// "Instances" are only applicable when coroutines are running
// in volatile mode, since we must be keep suspended coroutines in
// memory while they're polling. In durable mode, there's no need
// to keep "instances" around, since we can serialize the state of
// each coroutine and send it back and forth to Dispatch. In durable
// mode the GenericCoroutine is stateless.
type coroutineID = uint64

// Run runs the function.
func (c *Coroutine[I, O]) Run(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
	if name := req.Function(); name != c.name {
		return dispatchproto.NewResponseErrorf("%w: function %q received call for function %q", ErrInvalidArgument, c.name, name)
	}

	// Create or deserialize the coroutine (depending on the type of request).
	id, coro, err := c.setUp(req)
	if err != nil {
		return dispatchproto.NewResponseError(err)
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
		return dispatchproto.NewResponseError(err)
	}
	return yield.With(dispatchproto.CoroutineState(state))
}

// NewCall creates a Call for the function.
func (f *Coroutine[I, O]) NewCall(input I, opts ...dispatchproto.CallOption) (dispatchproto.Call, error) {
	boxedInput, err := dispatchproto.NewAny(input)
	if err != nil {
		return dispatchproto.Call{}, fmt.Errorf("cannot serialize input: %v", err)
	}
	return f.PrimitiveFunction.NewCall(boxedInput, opts...)
}

// Dispatch dispatches a Call to the function.
func (f *Coroutine[I, O]) Dispatch(ctx context.Context, input I, opts ...dispatchproto.CallOption) (ID, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return "", err
	}
	return f.dispatchCall(ctx, call)
}

func (c *Coroutine[I, O]) setUp(req dispatchproto.Request) (coroutineID, dispatchcoro.Coroutine, error) {
	// If the request carries a poll result, find or deserialize the
	// suspended coroutine.
	if pollResult, ok := req.PollResult(); ok {
		return c.deserialize(pollResult.CoroutineState())
	}

	// Otherwise, this is a new function call. Prepare input from the request.
	var input I
	boxedInput, ok := req.Input()
	if !ok {
		return 0, dispatchcoro.Coroutine{}, fmt.Errorf("%w: unsupported request: %v", ErrInvalidArgument, req)
	}
	if err := boxedInput.Unmarshal(&input); err != nil {
		return 0, dispatchcoro.Coroutine{}, fmt.Errorf("%w: invalid input %v: %v", ErrInvalidArgument, boxedInput, err)
	}

	// Create a new coroutine.
	coro := coroutine.NewWithReturn[dispatchproto.Response, dispatchproto.Request](c.entrypoint(input))

	// In volatile mode, register the coroutine instance after
	// assigning a unique identifier.
	var id coroutineID
	if !coroutine.Durable {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.nextID++
		id = c.nextID
		if c.instances == nil {
			c.instances = map[coroutineID]dispatchcoro.Coroutine{}
		}
		c.instances[id] = coro
	}

	return id, coro, nil
}

func (c *Coroutine[I, O]) tearDown(id coroutineID, coro dispatchcoro.Coroutine) {
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

func (c *Coroutine[I, O]) serialize(id coroutineID, coro dispatchcoro.Coroutine) (dispatchproto.Any, error) {
	// In volatile mode, serialize a reference to the coroutine instance.
	if !coroutine.Durable {
		return dispatchproto.NewAny(id)
	}

	// In durable mode, serialize the state of the coroutine.
	state, err := dispatchcoro.Serialize(coro)
	if err != nil {
		return dispatchproto.Any{}, fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	return state, nil
}

func (c *Coroutine[I, O]) deserialize(state dispatchproto.Any) (coroutineID, dispatchcoro.Coroutine, error) {
	var id coroutineID
	var coro dispatchcoro.Coroutine

	// In durable mode, create the coroutine and then deserialize its prior state.
	if coroutine.Durable {
		var zero I
		coro = coroutine.NewWithReturn[dispatchproto.Response, dispatchproto.Request](c.entrypoint(zero))
		if err := dispatchcoro.Deserialize(coro, state); err != nil {
			return 0, coro, fmt.Errorf("%w: %v", ErrIncompatibleState, err)
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
		return 0, coro, fmt.Errorf("%w: volatile coroutine %d not found", ErrIncompatibleState, id)
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

func (c *Coroutine[I, O]) entrypoint(input I) func() dispatchproto.Response {
	return func() dispatchproto.Response {
		// The context that gets passed as argument here should be recreated
		// each time the coroutine is resumed, ideally inheriting from the
		// parent context passed to the Run method. This is difficult to
		// do right in durable mode because we shouldn't capture the parent
		// context in the coroutine state.
		output, err := c.fn(context.TODO(), input)
		if err != nil {
			// TODO: include output if not nil
			return dispatchproto.NewResponseError(err)
		}
		boxedOutput, err := dispatchproto.NewAny(output)
		if err != nil {
			return dispatchproto.NewResponseErrorf("%w: invalid output %v: %v", ErrInvalidResponse, output, err)
		}
		return dispatchproto.NewResponse(dispatchproto.StatusOf(output), boxedOutput)
	}
}

// Await calls the function and awaits a result.
//
// Await should only be called within a Dispatch coroutine.
func (c *Coroutine[I, O]) Await(input I, opts ...dispatchproto.CallOption) (O, error) {
	var output O

	call, err := c.NewCall(input, opts...)
	if err != nil {
		return output, err
	}
	results, err := dispatchcoro.Gather[O](call)
	if err != nil {
		return output, err
	}
	return results[0], nil
}

// Gather makes many concurrent calls to the function and awaits the results.
//
// Gather should only be called within a Dispatch coroutine.
func (c *Coroutine[I, O]) Gather(inputs []I, opts ...dispatchproto.CallOption) ([]O, error) {
	calls := make([]dispatchproto.Call, len(inputs))
	for i, input := range inputs {
		call, err := c.NewCall(input, opts...)
		if err != nil {
			return nil, err
		}
		calls[i] = call
	}
	return dispatchcoro.Gather[O](calls...)
}

// Await calls the function and awaits a result.
//
// Await should only be called within a Dispatch coroutine (created via NewFunction).
func (f *PrimitiveFunction) Await(input dispatchproto.Any, opts ...dispatchproto.CallOption) (dispatchproto.Any, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return dispatchproto.Any{}, err
	}

	callResults, err := dispatchcoro.Await(dispatchcoro.AwaitAll, call)
	if err != nil {
		return dispatchproto.Any{}, err
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
func (f *PrimitiveFunction) Gather(inputs []dispatchproto.Any, opts ...dispatchproto.CallOption) ([]dispatchproto.Any, error) {
	calls := make([]dispatchproto.Call, len(inputs))
	for i, input := range inputs {
		call, err := f.NewCall(input, opts...)
		if err != nil {
			return nil, err
		}
		calls[i] = call
	}

	callResults, err := dispatchcoro.Await(dispatchcoro.AwaitAll, calls...)
	if err != nil {
		return nil, err
	}

	outputs := make([]dispatchproto.Any, len(inputs))
	for i, result := range callResults {
		output, _ := result.Output()
		outputs[i] = output
	}
	return outputs, nil
}
