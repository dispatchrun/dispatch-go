//go:build !durable

package dispatchcoro

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Await awaits the results of calls.
func Await(strategy AwaitStrategy, calls ...dispatchproto.Call) ([]dispatchproto.CallResult, error) {
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
		calls[i] = call.With(dispatchproto.CorrelationID(correlationID))
	}

	// Set polling configuration. There's no value in waking up the
	// coroutine sooner than when all results are available (by reducing
	// minResults and/or maxWait), since there's no internal concurrency
	// in the Go SDK.
	minResults := len(calls)
	maxResults := len(calls)
	maxWait := 5 * time.Minute

	callResults := make([]dispatchproto.CallResult, len(calls))

	// Poll until results available.
	for len(pending) > 0 {
		poll := dispatchproto.NewResponse(dispatchproto.NewPoll(minResults, maxResults, maxWait, dispatchproto.Calls(calls...)))
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

func allFailed(results []dispatchproto.CallResult) bool {
	for _, result := range results {
		if _, ok := result.Error(); !ok {
			return false
		}
	}
	return true
}

func joinErrors(results []dispatchproto.CallResult) error {
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
func Gather[O any](calls ...dispatchproto.Call) ([]O, error) {
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
