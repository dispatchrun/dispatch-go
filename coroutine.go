package dispatch

import (
	"context"
	"fmt"

	"github.com/dispatchrun/coroutine"
	"github.com/dispatchrun/coroutine/types"
	"google.golang.org/protobuf/proto"
)

const stateTypeUrl = "buf.build/stealthrocket/coroutine/coroutine.v1.State"

// NewCoroutine creates a Dispatch coroutine Function.
func NewCoroutine[I, O any](name string, fn func(context.Context, I) (O, error)) *GenericCoroutine[I, O] {
	return &GenericCoroutine[I, O]{
		GenericFunction[I, O]{
			PrimitiveFunction{name: name},
			fn,
		},
	}
}

// GenericCoroutine is a Function that accepts any input and returns any output.
type GenericCoroutine[I, O any] struct{ GenericFunction[I, O] }

// Run runs the coroutine function.
func (c *GenericCoroutine[I, O]) Run(ctx context.Context, req Request) Response {
	var coro coroutine.Coroutine[CoroR[O], CoroS]

	// Start a fresh coroutine if the request carries function input.
	if _, ok := req.Input(); ok {
		input, err := c.unpackInput(req)
		if err != nil {
			return NewResponseError(err)
		}
		coro = coroutine.NewWithReturn[CoroR[O], CoroS](c.entrypoint(input))

	} else if pollResult, ok := req.PollResult(); ok {
		// Otherwise, handle poll results bound for a suspended coroutine.
		var zero I
		coro = coroutine.NewWithReturn[CoroR[O], CoroS](c.entrypoint(zero))

		// Deserialize the coroutine.
		state := pollResult.CoroutineState()
		if state.TypeURL() != stateTypeUrl {
			return NewResponseErrorf("%w: unexpected type URL: %q", ErrIncompatibleState, state.TypeURL())
		} else if err := coro.Context().Unmarshal(state.Value()); err != nil {
			return NewResponseErrorf("%w: unmarshal state: %v", ErrIncompatibleState, err)
		}

		// Send poll results back to the yield point.
		coro.Send(CoroS{directive: pollResult})
	}

	// Run the coroutine until it yields or returns.
	if coro.Next() {
		// The coroutine yielded and is now paused.

		// Serialize the coroutine.
		if !coroutine.Durable {
			return NewResponseErrorf("%w: cannot serialize volatile coroutine", ErrPermanent)
		}
		rawState, err := coro.Context().Marshal()
		if err != nil {
			return NewResponseErrorf("%w: marshal state: %v", ErrPermanent, err)
		}
		state := newAnyTypeValue(stateTypeUrl, rawState)

		// Yield to Dispatch with the directive from the coroutine.
		result := coro.Recv()
		return NewResponse(result.status, result.directive, CoroutineState(state))
	}

	// The coroutine returned. Serialize the output / error.
	result := coro.Result()
	if result.err != nil {
		// TODO: serialize the output too if present
		return NewResponseError(result.err)
	}
	return c.packOutput(result.output)
}

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

type CoroS struct {
	directive RequestDirective
}

type CoroR[O any] struct {
	status    Status
	directive ResponseDirective

	output O
	err    error
}

// Yield yields control to Dispatch.
//
// The coroutine is paused, serialized and sent to Dispatch. The
// directive instructs Dispatch to perform an operation while
// the coroutine is suspended. Once the operation is complete,
// Dispatch yields control back to the coroutine, which is resumed
// from the point execution was suspended.
func Yield[O any](status Status, directive ResponseDirective) RequestDirective {
	result := coroutine.Yield[CoroR[O], CoroS](CoroR[O]{
		status:    status,
		directive: directive,
	})
	return result.directive
}

func init() {
	types.Register(protoSerializer, protoDeserializer)
}

func protoSerializer(s *types.Serializer, mp *proto.Message) error {
	m := *mp
	if m == nil {
		types.SerializeT(s, false)
		return nil
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("proto.Marshal: %w", err)
	}
	types.SerializeT(s, true)
	types.SerializeT(s, b)
	return nil
}

func protoDeserializer(d *types.Deserializer, mp *proto.Message) error {
	var ok bool
	types.DeserializeTo(d, &ok)
	if !ok {
		*mp = nil
		return nil
	}

	var b []byte
	types.DeserializeTo(d, &b)

	var m proto.Message
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("proto.Unmarshal: %w", err)
	}
	*mp = m

	return nil
}
