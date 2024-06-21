//go:build !durable

package dispatch

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"

	"github.com/dispatchrun/coroutine"
	"github.com/dispatchrun/dispatch-go/dispatchcoro"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Func creates a Function.
func Func[I, O any](name string, fn func(context.Context, I) (O, error)) *Function[I, O] {
	return &Function[I, O]{name: name, fn: fn}
}

// Function is a Dispatch Function.
type Function[I, O any] struct {
	name string

	fn func(ctx context.Context, input I) (O, error)

	endpoint *Dispatch

	volatileCoroutines
}

// Name is the name of the function.
func (f *Function[I, O]) Name() string {
	return f.name
}

// BuildCall creates (but does not dispatch) a Call for the function.
func (f *Function[I, O]) BuildCall(input I, opts ...dispatchproto.CallOption) (dispatchproto.Call, error) {
	boxedInput, err := dispatchproto.NewAny(input)
	if err != nil {
		return dispatchproto.Call{}, fmt.Errorf("cannot serialize input: %v", err)
	}
	var url string
	if f.endpoint != nil {
		url = f.endpoint.URL()
	}
	opts = append(slices.Clip(opts), boxedInput)
	return dispatchproto.NewCall(url, f.name, opts...), nil
}

// Dispatch dispatches a Call to the function.
func (f *Function[I, O]) Dispatch(ctx context.Context, input I, opts ...dispatchproto.CallOption) (dispatchproto.ID, error) {
	call, err := f.BuildCall(input, opts...)
	if err != nil {
		return "", err
	}
	if f.endpoint == nil {
		return "", fmt.Errorf("cannot dispatch function call: function has not been registered with a Dispatch endpoint")
	}
	client, err := f.endpoint.Client()
	if err != nil {
		return "", fmt.Errorf("cannot dispatch function call: %w", err)
	}
	return client.Dispatch(ctx, call)
}

func (f *Function[I, O]) run(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
	if name := req.Function(); name != f.name {
		return dispatchproto.NewResponseErrorf("%w: function %q received call for function %q", ErrInvalidArgument, f.name, name)
	}

	id, coro, err := f.setUp(req)
	if err != nil {
		return dispatchproto.NewResponseError(err)
	}
	defer f.tearDown(id, coro)

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
	state, err := f.serialize(id, coro)
	if err != nil {
		return dispatchproto.NewResponseError(err)
	}
	return yield.With(dispatchproto.CoroutineState(state))
}

func (f *Function[I, O]) setUp(req dispatchproto.Request) (coroutineID, dispatchcoro.Coroutine, error) {
	// If the request carries a poll result, find/deserialize the
	// suspended coroutine.
	if pollResult, ok := req.PollResult(); ok {
		return f.deserialize(pollResult.CoroutineState())
	}

	// Otherwise, create a new coroutine using input from the request.
	var input I
	boxedInput, ok := req.Input()
	if !ok {
		return 0, dispatchcoro.Coroutine{}, fmt.Errorf("%w: unsupported request: %v", ErrInvalidArgument, req)
	}
	if err := boxedInput.Unmarshal(&input); err != nil {
		return 0, dispatchcoro.Coroutine{}, fmt.Errorf("%w: invalid input %v: %v", ErrInvalidArgument, boxedInput, err)
	}
	coro := coroutine.NewWithReturn[dispatchproto.Response, dispatchproto.Request](f.entrypoint(input))

	// In volatile mode, register the coroutine instance and assign a unique ID.
	var id coroutineID
	if !coroutine.Durable {
		id = f.registerCoroutineInstance(coro)
	}
	return id, coro, nil
}

func (f *Function[I, O]) tearDown(id coroutineID, coro dispatchcoro.Coroutine) {
	// Remove volatile coroutine instances only once they're done.
	if !coroutine.Durable && coro.Done() {
		f.volatileCoroutines.deleteCoroutineInstance(id)
	}

	// Always tear down durable coroutines. They'll be rebuilt
	// on the next call (if applicable) from their serialized state,
	// possibly in a new location.
	if coroutine.Durable && !coro.Done() {
		coro.Stop()
		coro.Next()
	}
}

func (f *Function[I, O]) serialize(id coroutineID, coro dispatchcoro.Coroutine) (dispatchproto.Any, error) {
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

func (f *Function[I, O]) deserialize(state dispatchproto.Any) (coroutineID, dispatchcoro.Coroutine, error) {
	// In durable mode, create the coroutine and then deserialize its prior state.
	if coroutine.Durable {
		var zero I
		coro := coroutine.NewWithReturn[dispatchproto.Response, dispatchproto.Request](f.entrypoint(zero))
		if err := dispatchcoro.Deserialize(coro, state); err != nil {
			return 0, dispatchcoro.Coroutine{}, fmt.Errorf("%w: %v", ErrIncompatibleState, err)
		}
		return 0, coro, nil
	}

	// In volatile mode, find the suspended coroutine instance.
	var id coroutineID
	if err := state.Unmarshal(&id); err != nil {
		return 0, dispatchcoro.Coroutine{}, fmt.Errorf("%w: invalid volatile coroutine reference: %s", ErrIncompatibleState, state)
	}
	coro, err := f.findCoroutineInstance(id)
	return id, coro, err
}

// Register is called when the function is registered
// on a Dispatch endpoint.
func (f *Function[I, O]) Register(endpoint *Dispatch) (string, dispatchproto.Function) {
	f.endpoint = endpoint
	return f.name, f.run
}

func (c *Function[I, O]) entrypoint(input I) func() dispatchproto.Response {
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
// Await should only be called within a Dispatch Function (created via Func).
func (f *Function[I, O]) Await(input I, opts ...dispatchproto.CallOption) (O, error) {
	var output O
	call, err := f.BuildCall(input, opts...)
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
// Gather should only be called within a Dispatch Function (created via Func).
func (f *Function[I, O]) Gather(inputs []I, opts ...dispatchproto.CallOption) ([]O, error) {
	calls := make([]dispatchproto.Call, len(inputs))
	for i, input := range inputs {
		call, err := f.BuildCall(input, opts...)
		if err != nil {
			return nil, err
		}
		calls[i] = call
	}
	return dispatchcoro.Gather[O](calls...)
}

// AnyFunction is a Function[I, O] instance.
type AnyFunction interface {
	Register(*Dispatch) (string, dispatchproto.Function)
}

// "Instances" are only applicable when coroutines are running
// in volatile mode, since we must be keep suspended coroutines in
// memory while they're polling. In durable mode, there's no need
// to keep "instances" around, since we can serialize the state of
// each coroutine and send it back and forth to Dispatch. In durable
// mode Function[I,O] is stateless.
type volatileCoroutines struct {
	instances map[coroutineID]dispatchcoro.Coroutine
	nextID    coroutineID
	mu        sync.Mutex
}

type coroutineID = uint64

func (f *volatileCoroutines) registerCoroutineInstance(coro dispatchcoro.Coroutine) coroutineID {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.nextID == 0 {
		f.nextID = rand.Uint64()
	}
	f.nextID++

	id := f.nextID
	if f.instances == nil {
		f.instances = map[coroutineID]dispatchcoro.Coroutine{}
	}
	f.instances[id] = coro

	return id
}

func (f *volatileCoroutines) findCoroutineInstance(id coroutineID) (dispatchcoro.Coroutine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	coro, ok := f.instances[id]
	if !ok {
		return coro, fmt.Errorf("%w: volatile coroutine %d not found", ErrIncompatibleState, id)
	}
	return coro, nil
}

func (f *volatileCoroutines) deleteCoroutineInstance(id coroutineID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.instances, id)
}

func (f *volatileCoroutines) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, fn := range f.instances {
		fn.Stop()
		fn.Next()
	}
	clear(f.instances)
	return nil
}
