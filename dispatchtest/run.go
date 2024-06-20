package dispatchtest

import (
	"context"
	"fmt"
	"sync"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Runnable is something that can be Run.
type Runnable interface {
	Run(context.Context, dispatchproto.Request) dispatchproto.Response
}

var _ Runnable = (dispatch.Function)(nil)
var _ Runnable = (*dispatch.Registry)(nil)

// Call invokes a function or coroutine, runs it to completion,
// and returns its result.
func Call[O any](functions Runnable, call dispatch.Call) (O, error) {
	res := Run(functions, call.Request())

	var output O
	boxedOutput, ok := res.Output()
	if !ok || !res.OK() {
		return output, fmt.Errorf("unexpected response: %s", res)
	}
	err := boxedOutput.Unmarshal(&output)
	return output, err
}

// Run runs a function or coroutine to completion.
func Run(functions Runnable, req dispatchproto.Request) dispatchproto.Response {
	for {
		res := functions.Run(context.Background(), req)
		if _, ok := res.Exit(); ok {
			return res
		}
		req = poll(functions, req, res)
	}
}

func poll(functions Runnable, req dispatchproto.Request, res dispatchproto.Response) dispatchproto.Request {
	poll, ok := res.Poll()
	if !ok {
		panic(fmt.Errorf("not implemented: %s", res))
	}

	result := poll.Result()

	// Make nested calls.
	if calls := poll.Calls(); len(calls) > 0 {
		callResults := gomap(calls, func(call dispatchproto.Call) dispatchproto.CallResult {
			res := Run(functions, call.Request())
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
