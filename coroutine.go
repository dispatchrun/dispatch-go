package dispatch

import (
	"context"

	"github.com/dispatchrun/coroutine"
)

// NewCoroutine creates a Dispatch coroutine Function.
func NewCoroutine[I, O any](name string, fn func(context.Context, I) (O, error)) *GenericCoroutine[I, O] {
	return &GenericCoroutine[I, O]{*NewFunction(name, fn)}
}

// GenericCoroutine is a Function that accepts any input and returns any output.
type GenericCoroutine[I, O any] struct{ GenericFunction[I, O] }

// Run runs the coroutine function.
func (c *GenericCoroutine[I, O]) Run(ctx context.Context, req Request) Response {
	var coro coroutine.Coroutine[CoroR[O], any]

	if _, ok := req.Input(); ok {
		input, err := c.unpackInput(req)
		if err != nil {
			return NewResponseError(err)
		}
		coro = coroutine.NewWithReturn[CoroR[O], any](c.entrypoint(input))

	} else if pollResult, ok := req.PollResult(); ok {
		var zero I
		coro = coroutine.NewWithReturn[CoroR[O], any](c.entrypoint(zero))

		state := pollResult.CoroutineState()

		// TODO: deserialize state
		_ = state
		panic("not implemented")
	}

	if coro.Next() {
		if !coroutine.Durable {
			return NewResponseErrorf("%w: cannot serialize volatile coroutine", ErrPermanent)
		}
		// TODO: serialize state
		// TODO: inspect coro.Recv() yield
		// TODO: build Poll Response
		panic("not implemented")
	}

	r := coro.Result()
	if r.err != nil {
		return NewResponseError(r.err)
	}
	return c.packOutput(r.output)
}

//go:noinline
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

type CoroR[O any] struct {
	output O
	err    error
}
