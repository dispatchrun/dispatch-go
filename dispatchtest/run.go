package dispatchtest

import (
	"context"
	"fmt"
	"sync"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Run runs a function and returns its result.
func Run[O any](call dispatchproto.Call, functions ...dispatch.AnyFunction) (O, error) {
	runner := NewRunner(functions...)

	res := runner.Run(call.Request())

	var output O
	result, ok := res.Result()
	if !ok {
		if !res.OK() {
			return output, dispatchproto.StatusError(res.Status())
		}
		return output, fmt.Errorf("unexpected response: %s", res)
	}
	var err error
	if resultErr, ok := result.Error(); ok {
		err = resultErr
	} else if !res.OK() {
		err = dispatchproto.StatusError(res.Status())
	}
	boxedOutput, ok := res.Output()
	if ok {
		if unmarshalErr := boxedOutput.Unmarshal(&output); err == nil && unmarshalErr != nil {
			err = fmt.Errorf("failed to unmarshal output: %w", unmarshalErr)
		}
	}
	return output, err
}

// Runner runs functions.
type Runner struct {
	functions dispatchproto.FunctionMap
}

// NewRunner creates a Runner.
func NewRunner(functions ...dispatch.AnyFunction) *Runner {
	runner := &Runner{functions: dispatchproto.FunctionMap{}}
	for _, fn := range functions {
		runner.Register(fn)
	}
	return runner
}

// Register registers a function.
func (r *Runner) Register(fn dispatch.AnyFunction) {
	name, primitive := fn.Register(nil)
	r.RegisterPrimitive(name, primitive)
}

// RegisterPrimitive registers a primitive function.
func (r *Runner) RegisterPrimitive(name string, fn dispatchproto.Function) {
	r.functions[name] = fn
}

// Run runs a function to completion and returns its response.
func (r *Runner) Run(req dispatchproto.Request) dispatchproto.Response {
	for {
		res := r.RoundTrip(req)
		if _, ok := res.Exit(); ok {
			return res
		}
		req = r.poll(req, res)
	}
}

// RoundTrip sends a request to a function and returns its response.
func (r Runner) RoundTrip(req dispatchproto.Request) dispatchproto.Response {
	return r.functions.Run(context.Background(), req)
}

func (r *Runner) poll(req dispatchproto.Request, res dispatchproto.Response) dispatchproto.Request {
	poll, ok := res.Poll()
	if !ok {
		panic(fmt.Errorf("not implemented: %s", res))
	}

	result := poll.Result()

	// Make nested calls.
	if calls := poll.Calls(); len(calls) > 0 {
		callResults := gomap(calls, func(call dispatchproto.Call) dispatchproto.CallResult {
			res := r.Run(call.Request())
			callResult, _ := res.Result()
			return callResult.With(dispatchproto.CorrelationID(call.CorrelationID()))
		})
		result = result.With(dispatchproto.CallResults(callResults...))
	}

	return req.With(result)
}

// Concurrently convert []I to []O using the func(I) O mapper.
func gomap[I, O any](input []I, mapper func(I) O) []O {
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(len(input))

	output := make([]O, len(input))

	for i := range input {
		go func(i int) {
			defer wg.Done()

			out := mapper(input[i])

			mu.Lock()
			defer mu.Unlock()

			output[i] = out
		}(i)
	}
	wg.Wait()

	return output
}
