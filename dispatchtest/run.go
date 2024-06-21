package dispatchtest

import (
	"context"
	"fmt"
	"sync"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Call invokes a function or coroutine, runs it to completion,
// and returns its result.
func Call[O any](call dispatchproto.Call, functions ...dispatch.AnyFunction) (O, error) {
	res := Run(call.Request(), functions...)

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

// Run runs a function or coroutine to completion.
func Run(req dispatchproto.Request, functions ...dispatch.AnyFunction) dispatchproto.Response {
	var runner runner
	runner.registry.Register(functions...)
	return runner.run(req)
}

// RoundTrip makes a request to a function and returns the response.
func RoundTrip(req dispatchproto.Request, function dispatch.AnyFunction) dispatchproto.Response {
	var runner runner
	runner.registry.Register(function)
	return runner.roundTrip(req)
}

type runner struct {
	registry dispatch.FunctionRegistry
}

func (r *runner) run(req dispatchproto.Request) dispatchproto.Response {
	for {
		res := r.roundTrip(req)
		if _, ok := res.Exit(); ok {
			return res
		}
		req = r.poll(req, res)
	}
}

func (r *runner) roundTrip(req dispatchproto.Request) dispatchproto.Response {
	return r.registry.Run(context.Background(), req)
}

func (r *runner) poll(req dispatchproto.Request, res dispatchproto.Response) dispatchproto.Request {
	poll, ok := res.Poll()
	if !ok {
		panic(fmt.Errorf("not implemented: %s", res))
	}

	result := poll.Result()

	// Make nested calls.
	if calls := poll.Calls(); len(calls) > 0 {
		callResults := gomap(calls, func(call dispatchproto.Call) dispatchproto.CallResult {
			res := r.run(call.Request())
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
