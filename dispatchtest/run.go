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
	m := dispatchproto.FunctionMap{}
	for _, fn := range functions {
		m[fn.Name()] = fn.Primitive()
	}
	res := Run(call.Request(), m)

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
func Run(req dispatchproto.Request, functions dispatchproto.FunctionMap) dispatchproto.Response {
	r := runner{functions}
	return r.run(req)
}

type runner struct{ functions dispatchproto.FunctionMap }

func (r *runner) run(req dispatchproto.Request) dispatchproto.Response {
	for {
		res := r.functions.Run(context.Background(), req)
		if _, ok := res.Exit(); ok {
			return res
		}
		req = r.poll(req, res)
	}
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
