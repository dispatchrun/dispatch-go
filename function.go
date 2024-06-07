//go:build !durable

package dispatch

import (
	"context"
	"fmt"
	"slices"

	"github.com/stealthrocket/coroutine"
	"google.golang.org/protobuf/proto"
)

// Function is a Dispatch function.
type Function interface {
	// Name is the name of the function.
	Name() string

	// Run runs the function.
	Run(context.Context, Request) Response

	// bind is an internal hook for binding a function to
	// a Dispatch endpoint, allowing the NewCall and Dispatch
	// methods to be called on the function.
	bind(endpoint *Dispatch)
}

// NewFunction creates a Dispatch function.
func NewFunction[I, O proto.Message](name string, fn func(context.Context, I) (O, error)) *GenericFunction[I, O] {
	return &GenericFunction[I, O]{name: name, fn: fn}
}

// GenericFunction is a Dispatch function that accepts arbitrary input
// and returns arbitrary output.
type GenericFunction[I, O proto.Message] struct {
	name string
	fn   func(ctx context.Context, input I) (O, error)

	endpoint *Dispatch
}

// Name is the name of the function.
func (f *GenericFunction[I, O]) Name() string {
	return f.name
}

// Run runs the function.
func (f *GenericFunction[I, O]) Run(ctx context.Context, req Request) Response {
	var coro coroutine.Coroutine[any, any]
	var zero I

	if boxedInput, ok := req.Input(); ok {
		message, err := boxedInput.Proto()
		if err != nil {
			return NewResponseErrorf("%w: invalid input: %v", ErrInvalidArgument, err)
		}
		input, ok := message.(I)
		if !ok {
			return NewResponseErrorf("%w: invalid input type: %T", ErrInvalidArgument, message)
		}
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(input))

	} else if pollResult, ok := req.PollResult(); ok {
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(zero))
		if err := coro.Context().Unmarshal(pollResult.CoroutineState()); err != nil {
			return NewResponseErrorf("%w: invalid coroutine state: %v", ErrIncompatibleState, err)
		}

	} else {
		return NewResponseErrorf("%w: unsupported request directive: %v", ErrInvalidArgument, req)
	}

	// When running in volatile mode, we cannot snapshot the coroutine state
	// and return it to the caller. Instead, we run the coroutine to completion
	// in a blocking fashion until it returns a result or an error.
	if !coroutine.Durable {
		var canceled bool
		coroutine.Run(coro, func(v any) any {
			// TODO
			return nil
		})
		if canceled {
			return NewResponseError(context.Cause(ctx))
		}
	}

	var res Response
	if coro.Next() {
		coroutineState, err := coro.Context().Marshal()
		if err != nil {
			return NewResponseErrorf("%w: cannot serialize coroutine: %v", ErrPermanent, err)
		}
		switch yield := coro.Recv().(type) {
		// TODO
		default:
			res = NewResponseErrorf("%w: unsupported coroutine yield: %T", ErrInvalidResponse, yield)
		}
		// TODO
		_ = coroutineState
	} else {
		switch ret := coro.Result().(type) {
		case proto.Message:
			output, err := NewAny(ret)
			if err != nil {
				res = NewResponseErrorf("%w: cannot serialize return value: %v", ErrInvalidResponse, err)
			} else {
				// TODO: automatically derive a status from the ret value
				res = NewResponse(StatusOf(ret), Output(output))
			}
		case error:
			res = NewResponseError(ret)
		default:
			res = NewResponseErrorf("%w: unsupported coroutine return: %T", ErrInvalidResponse, ret)
		}
	}

	return res
}

func (f *GenericFunction[I, O]) bind(endpoint *Dispatch) {
	f.endpoint = endpoint
}

// NewCall creates a Call for the function.
func (f *GenericFunction[I, O]) NewCall(input I, opts ...CallOption) (Call, error) {
	if f.endpoint == nil {
		return Call{}, fmt.Errorf("cannot build function call: function has not been registered with a Dispatch endpoint")
	}
	anyInput, err := NewAny(input)
	if err != nil {
		return Call{}, fmt.Errorf("cannot serialize input: %v", err)
	}
	opts = append(slices.Clip(opts), Input(anyInput))
	return NewCall(f.endpoint.URL(), f.name, opts...), nil
}

// Dispatch dispatches a call to the function.
func (f *GenericFunction[I, O]) Dispatch(ctx context.Context, input I, opts ...CallOption) (ID, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return "", err
	}
	client, err := f.endpoint.Client()
	if err != nil {
		return "", fmt.Errorf("cannot dispatch function call: %w", err)
	}
	return client.Dispatch(ctx, call)
}

//go:noinline
func (f *GenericFunction[I, O]) entrypoint(input I) func() any {
	return func() any {
		// The context that gets passed as argument here should be recreated
		// each time the coroutine is resumed, ideally inheriting from the
		// parent context passed to the Run method. This is difficult to
		// do right in durable mode because we shouldn't capture the parent
		// context in the coroutine state.
		if res, err := f.fn(context.TODO(), input); err != nil {
			return err
		} else {
			return res
		}
	}
}

// NewPrimitiveFunction creates a PrimitiveFunction.
func NewPrimitiveFunction(name string, fn func(context.Context, Request) Response) *PrimitiveFunction {
	return &PrimitiveFunction{name: name, fn: fn}
}

// PrimitiveFunction is a function that's close to the underlying
// Dispatch protocol, accepting a Request and returning a Response.
type PrimitiveFunction struct {
	name string
	fn   func(context.Context, Request) Response

	endpoint *Dispatch
}

// Name is the name of the function.
func (f *PrimitiveFunction) Name() string {
	return f.name
}

// Run runs the function.
func (f *PrimitiveFunction) Run(ctx context.Context, req Request) Response {
	return f.fn(ctx, req)
}

func (f *PrimitiveFunction) bind(endpoint *Dispatch) {
	f.endpoint = endpoint
}

// NewCall creates a Call for the function.
func (f *PrimitiveFunction) NewCall(input Any, opts ...CallOption) (Call, error) {
	if f.endpoint == nil {
		return Call{}, fmt.Errorf("cannot build function call: function has not been registered with a Dispatch endpoint")
	}
	opts = append(slices.Clip(opts), Input(input))
	return NewCall(f.endpoint.URL(), f.name, opts...), nil
}

// Dispatch dispatches a call to the function.
func (f *PrimitiveFunction) Dispatch(ctx context.Context, input Any, opts ...CallOption) (ID, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return "", err
	}
	client, err := f.endpoint.Client()
	if err != nil {
		return "", fmt.Errorf("cannot dispatch function call: %w", err)
	}
	return client.Dispatch(ctx, call)
}
