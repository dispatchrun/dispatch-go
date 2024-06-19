package dispatchtest

import (
	"context"
	"fmt"
	"sync"

	"github.com/dispatchrun/dispatch-go"
)

// Run runs a function or coroutine to completion.
func Run(functions dispatch.Runnable, req dispatch.Request) dispatch.Response {
	for {
		res := functions.Run(context.Background(), req)
		if _, ok := res.Exit(); ok {
			return res
		}
		req = poll(functions, req, res)
	}
}

func poll(functions dispatch.Runnable, req dispatch.Request, res dispatch.Response) dispatch.Request {
	poll, ok := res.Poll()
	if !ok {
		panic(fmt.Errorf("not implemented: %s", res))
	}

	pollResult := []dispatch.PollResultOption{dispatch.CoroutineState(poll.CoroutineState())}

	// Make any nested calls.
	if calls := poll.Calls(); len(calls) > 0 {
		callResults := gomap(calls, func(call dispatch.Call) dispatch.CallResult {
			res := Run(functions, call.Request())
			callResult, ok := res.Result()
			if !ok {
				callResult = dispatch.NewCallResult()
			}
			return callResult.With(dispatch.CorrelationID(call.CorrelationID()))
		})
		pollResult = append(pollResult, dispatch.CallResults(callResults...))
	}

	return dispatch.NewRequest(req.Function(), dispatch.NewPollResult(pollResult...))

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
