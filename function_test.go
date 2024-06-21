package dispatch_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dispatchrun/coroutine"
	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchcoro"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
)

func logMode(t *testing.T) {
	t.Helper()

	if coroutine.Durable {
		t.Log("running in durable mode")
	} else {
		t.Log("running in volatile mode")
	}
}

func TestCoroutineReturn(t *testing.T) {
	logMode(t)

	stringify := dispatch.Func("stringify", func(ctx context.Context, in int) (string, error) {
		if in < 0 {
			return "", fmt.Errorf("%w: %d", dispatch.ErrInvalidArgument, in)
		}
		return strconv.Itoa(in), nil
	})

	call, err := stringify.BuildCall(11)
	if err != nil {
		t.Fatal(err)
	}
	output, err := dispatchtest.Run[string](call, stringify)
	if err != nil {
		t.Fatal(err)
	}
	if output != "11" {
		t.Errorf("unexpected output: %s", output)
	}

	call2, err := stringify.BuildCall(-23)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dispatchtest.Run[string](call2, stringify)
	if err == nil || !strings.Contains(err.Error(), "InvalidArgument: -23") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCoroutineExit(t *testing.T) {
	logMode(t)

	stringify := dispatch.Func("stringify", func(ctx context.Context, in int) (string, error) {
		var res dispatchproto.Response
		if in < 0 {
			res = dispatchproto.NewResponseErrorf("%w: %d", dispatch.ErrInvalidArgument, in)
		} else {
			res = dispatchproto.NewResponse(dispatchproto.String(strconv.Itoa(in)))
		}
		dispatchcoro.Yield(res)
		panic("unreachable")
	})

	call, err := stringify.BuildCall(11)
	if err != nil {
		t.Fatal(err)
	}
	output, err := dispatchtest.Run[string](call, stringify)
	if err != nil {
		t.Fatal(err)
	}
	if output != "11" {
		t.Errorf("unexpected output: %s", output)
	}

	call2, err := stringify.BuildCall(-23)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dispatchtest.Run[string](call2, stringify)
	if err == nil || !strings.Contains(err.Error(), "InvalidArgument: -23") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCoroutinePoll(t *testing.T) {
	logMode(t)

	repeat := dispatch.Func("repeat", func(ctx context.Context, n int) (string, error) {
		var repeated string
		for i := 0; i < n; i++ {
			// Call a mock identity function that returns its input.
			call := dispatchproto.NewCall("http://example.com", "identity", dispatchproto.String("x"), dispatchproto.CorrelationID(uint64(i)))
			poll := dispatchproto.NewResponse(dispatchproto.NewPoll(1, 2, time.Minute, dispatchproto.Calls(call)))

			res := dispatchcoro.Yield(poll)

			pollResult, ok := res.PollResult()
			if !ok {
				return "", fmt.Errorf("expected poll result, got %s", res)
			}
			callResults := pollResult.Results()
			if len(callResults) != 1 {
				return "", fmt.Errorf("expected one poll call result, got %s", pollResult)
			}
			callResult := callResults[0]
			if got := callResult.CorrelationID(); got != uint64(i) {
				return "", fmt.Errorf("unexpected correlation ID: got %v, want %v", got, uint64(i))
			}
			output, ok := callResult.Output()
			if !ok {
				return "", fmt.Errorf("expected call result output, got %s", callResults[0])
			}

			var s string
			if err := output.Unmarshal(&s); err != nil {
				return "", fmt.Errorf("unmarshal string: %w", err)
			}
			repeated += s
		}
		return repeated, nil
	})

	// Continously run the coroutine until it returns/exits.
	var req dispatchproto.Request = dispatchproto.NewRequest("repeat", dispatchproto.Int(3))
	var res dispatchproto.Response
	for {
		res = repeat.Primitive()(context.Background(), req)
		if res.Status() != dispatchproto.OKStatus {
			t.Errorf("unexpected status: %s", res.Status())
		}
		if _, done := res.Exit(); done {
			break
		}

		// Check the poll directive.
		poll, ok := res.Poll()
		if !ok {
			t.Fatalf("expected poll response, got %s", res)
		}
		if got := poll.MinResults(); got != 1 {
			t.Errorf("unexpected poll min results: %v", got)
		}
		if got := poll.MaxResults(); got != 2 {
			t.Errorf("unexpected poll max results: %v", got)
		}
		if got := poll.MaxWait(); got != time.Minute {
			t.Errorf("unexpected poll max wait: %v", got)
		}

		// Check the call.
		calls := poll.Calls()
		if len(calls) != 1 {
			t.Fatalf("expected one poll call, got %s", poll)
		}
		call := calls[0]
		if got := call.Endpoint(); got != "http://example.com" {
			t.Errorf("unexpected call endpoint: %v", got)
		}
		if got := call.Function(); got != "identity" {
			t.Errorf("unexpected call endpoint: %v", got)
		}

		// Prepare the next request that carries the call result.
		callResult := dispatchproto.NewCallResult(
			call.Input(), // send call input back as the output
			dispatchproto.CorrelationID(call.CorrelationID())) // correlation ID needs to match

		pollResult := dispatchproto.NewPollResult(
			dispatchproto.CoroutineState(poll.CoroutineState()), // send coroutine state back
			dispatchproto.CallResults(callResult))

		req = dispatchproto.NewRequest("repeat", pollResult)
	}

	exit, _ := res.Exit()
	if err, ok := exit.Error(); ok {
		t.Fatalf("unexpected error: %s", err)
	}

	var repeated string
	output, ok := exit.Output()
	if !ok {
		t.Errorf("unexpected result, got %s", exit)
	} else if err := output.Unmarshal(&repeated); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}

	if repeated != "xxx" {
		t.Errorf("unexpected function result: %q", repeated)
	}
}

func TestCoroutineAwait(t *testing.T) {
	logMode(t)

	// This test is essentially the same as the test above, just
	// using the higher level helpers for awaiting a call.

	identity := dispatch.Func("identity", func(ctx context.Context, x string) (string, error) {
		panic("not implemented") // this is a mock only
	})

	repeat := dispatch.Func("repeat", func(ctx context.Context, n int) (string, error) {
		var repeated string
		for i := 0; i < n; i++ {
			res, err := identity.Await("x")
			if err != nil {
				return "", err
			}
			repeated += res
		}
		return repeated, nil
	})

	const repeatCount = 3

	req := dispatchproto.NewRequest("repeat", dispatchproto.Int(repeatCount))
	var res dispatchproto.Response

	requestCount := 0
	for {
		res = repeat.Primitive()(context.Background(), req)
		if res.Status() != dispatchproto.OKStatus {
			t.Errorf("unexpected status: %s", res.Status())
		}
		if _, done := res.Exit(); done {
			requestCount++
			break
		}
		poll, ok := res.Poll()
		if !ok {
			t.Fatalf("expected poll response, got %s", res)
		}
		calls := poll.Calls()
		if len(calls) != 1 {
			t.Fatalf("expected one poll call, got %s", poll)
		}
		call := calls[0]

		callResult := dispatchproto.NewCallResult(
			call.Input(),
			dispatchproto.CorrelationID(call.CorrelationID()))

		pollResult := dispatchproto.NewPollResult(
			dispatchproto.CoroutineState(poll.CoroutineState()),
			dispatchproto.CallResults(callResult))

		req = dispatchproto.NewRequest("repeat", pollResult)

		requestCount++
	}

	if requestCount != repeatCount+1 { // one input request + `repeatCount` polls
		t.Errorf("unexpected number of requests: got %d, want %d", requestCount, repeatCount+1)
	}

	exit, _ := res.Exit()
	if err, ok := exit.Error(); ok {
		t.Fatalf("unexpected error: %s", err)
	}

	var repeated string
	output, ok := exit.Output()
	if !ok {
		t.Errorf("unexpected result, got %s", exit)
	} else if err := output.Unmarshal(&repeated); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}

	if want := strings.Repeat("x", repeatCount); repeated != want {
		t.Errorf("unexpected function result: got %q, want %q", repeated, want)
	}
}

func TestCoroutineGather(t *testing.T) {
	logMode(t)

	// This test is essentially the same as the test above, just
	// using the higher level helpers for gathering the results
	// of many calls.

	identity := dispatch.Func("identity", func(ctx context.Context, x string) (string, error) {
		panic("not implemented") // this is a mock only
	})

	repeat := dispatch.Func("repeat", func(ctx context.Context, n int) (string, error) {
		inputs := make([]string, n)
		for i := range inputs {
			inputs[i] = "x"
		}
		results, err := identity.Gather(inputs)
		if err != nil {
			return "", err
		}
		return strings.Join(results, ""), nil
	})

	const repeatCount = 3

	req := dispatchproto.NewRequest("repeat", dispatchproto.Int(repeatCount))
	res := repeat.Primitive()(context.Background(), req)
	if res.Status() != dispatchproto.OKStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}

	poll, ok := res.Poll()
	if !ok {
		t.Fatalf("expected poll response, got %s", res)
	}
	calls := poll.Calls()
	if len(calls) != repeatCount {
		t.Fatalf("expected %d poll calls, got %s", repeatCount, poll)
	}

	callResults := make([]dispatchproto.CallResult, len(calls))
	for i, call := range calls {
		callResults[i] = dispatchproto.NewCallResult(
			call.Input(),
			dispatchproto.CorrelationID(call.CorrelationID()))
	}

	// Send all results back at once.
	pollResult := dispatchproto.NewPollResult(
		dispatchproto.CoroutineState(poll.CoroutineState()),
		dispatchproto.CallResults(callResults...))

	req = dispatchproto.NewRequest("repeat", pollResult)
	res = repeat.Primitive()(context.Background(), req)
	if res.Status() != dispatchproto.OKStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}

	exit, ok := res.Exit()
	if !ok {
		t.Fatalf("unexpected response, got %s", res)
	}
	if err, ok := exit.Error(); ok {
		t.Fatalf("unexpected error: %s", err)
	}

	var repeated string
	output, ok := exit.Output()
	if !ok {
		t.Errorf("unexpected result, got %s", exit)
	} else if err := output.Unmarshal(&repeated); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}

	if want := strings.Repeat("x", repeatCount); repeated != want {
		t.Errorf("unexpected function result: got %q, want %q", repeated, want)
	}
}

func TestCoroutineGatherSlow(t *testing.T) {
	logMode(t)

	// This test is essentially the same as the test above, just
	// sending back call results one at a time, and in random order.

	identity := dispatch.Func("identity", func(ctx context.Context, x string) (string, error) {
		panic("not implemented") // this is a mock only
	})

	repeat := dispatch.Func("repeat", func(ctx context.Context, n int) (string, error) {
		inputs := make([]string, n)
		for i := range inputs {
			inputs[i] = "x"
		}
		results, err := identity.Gather(inputs)
		if err != nil {
			return "", err
		}
		return strings.Join(results, ""), nil
	})

	const repeatCount = 3

	req := dispatchproto.NewRequest("repeat", dispatchproto.Int(repeatCount))
	res := repeat.Primitive()(context.Background(), req)
	if res.Status() != dispatchproto.OKStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}

	poll, ok := res.Poll()
	if !ok {
		t.Fatalf("expected poll response, got %s", res)
	}
	calls := poll.Calls()
	if len(calls) != repeatCount {
		t.Fatalf("expected %d poll calls, got %s", repeatCount, poll)
	}

	callResults := make([]dispatchproto.CallResult, len(calls))
	for i, call := range calls {
		callResults[i] = dispatchproto.NewCallResult(
			call.Input(),
			dispatchproto.CorrelationID(call.CorrelationID()))
	}

	// Randomize call result order.
	rand.Shuffle(len(callResults), func(i, j int) {
		callResults[i], callResults[j] = callResults[j], callResults[i]
	})

	// Deliver an empty poll result, to assert it's a noop.
	req = dispatchproto.NewRequest("repeat", poll.Result())
	res = repeat.Primitive()(context.Background(), req)
	if res.Status() != dispatchproto.OKStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}

	// Deliver one call result at a time.
	for i := range callResults {
		if _, ok := res.Poll(); !ok {
			t.Fatalf("expected previous response to be a poll before delivering call result %d, but got %s", i, res)
		}

		pollResult := poll.Result().With(dispatchproto.CallResults(callResults[i]))

		req = dispatchproto.NewRequest("repeat", pollResult)
		res = repeat.Primitive()(context.Background(), req)
		if res.Status() != dispatchproto.OKStatus {
			t.Errorf("unexpected status: %s", res.Status())
		}

		// Only the final response should be an exit.
		if _, ok := res.Exit(); ok {
			if i != len(callResults)-1 {
				t.Errorf("unexpected exit after delivering call result %d: %s", i, res)
			}
		}
	}

	exit, ok := res.Exit()
	if !ok {
		t.Fatalf("unexpected response, got %s", res)
	}
	if err, ok := exit.Error(); ok {
		t.Fatalf("unexpected error: %s", err)
	}

	var repeated string
	output, ok := exit.Output()
	if !ok {
		t.Errorf("unexpected result, got %s", exit)
	} else if err := output.Unmarshal(&repeated); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}

	if want := strings.Repeat("x", repeatCount); repeated != want {
		t.Errorf("unexpected function result: got %q, want %q", repeated, want)
	}
}

func TestFunctionNewCallAndDispatchWithoutEndpoint(t *testing.T) {
	fn := dispatch.Func("foo", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})

	_, err := fn.BuildCall("bar") // allowed
	if err != nil {
		t.Fatal(err)
	}
	_, err = fn.Dispatch(context.Background(), "bar")
	if err == nil || err.Error() != "cannot dispatch function call: function has not been registered with a Dispatch endpoint" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFunctionDispatchWithoutClient(t *testing.T) {
	// It's not necessary to have valid Client configuration when
	// creating a Dispatch endpoint. In this case, there's no
	// Dispatch API key available.
	endpoint, err := dispatch.New(dispatch.EndpointUrl("http://example.com"), dispatch.Env( /* i.e. no env vars */ ))
	if err != nil {
		t.Fatal(err)
	}

	fn := dispatch.Func("foo", func(ctx context.Context, input string) (string, error) {
		panic("not implemented")
	})
	endpoint.Register(fn)

	if _, err := fn.BuildCall("bar"); err != nil { // allowed
		t.Fatal(err)
	}

	// However, a client is not available.
	_, err = fn.Dispatch(context.Background(), "bar")
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "cannot dispatch function call: Dispatch API key has not been set. Use APIKey(..), or set the DISPATCH_API_KEY environment variable" {
		t.Errorf("unexpected error: %v", err)
	}
}
