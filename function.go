//go:build !durable

package dispatch

import (
	"context"
	"fmt"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"github.com/stealthrocket/coroutine"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Function is a Dispatch function.
type Function interface {
	// Name is the name of the function.
	Name() string

	// Run runs the function.
	Run(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse

	// bind is an internal hook for binding a function to
	// a Dispatch endpoint, allowing the NewCall and Dispatch
	// methods to be called on the function.
	bind(endpoint *Dispatch)
}

// NewFunction creates a Dispatch function.
func NewFunction[Input, Output proto.Message](name string, fn func(context.Context, Input) (Output, error)) *GenericFunction[Input, Output] {
	return &GenericFunction[Input, Output]{name: name, fn: fn}
}

// GenericFunction is a Dispatch function that accepts arbitrary input
// and returns arbitrary output.
type GenericFunction[Input, Output proto.Message] struct {
	name string
	fn   func(ctx context.Context, input Input) (Output, error)

	endpoint *Dispatch
}

// Name is the name of the function.
func (f *GenericFunction[Input, Output]) Name() string {
	return f.name
}

// Run runs the function.
func (f *GenericFunction[Input, Output]) Run(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
	var coro coroutine.Coroutine[any, any]
	var zero Input

	switch c := req.Directive.(type) {
	case *sdkv1.RunRequest_PollResult:
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(zero))
		if err := coro.Context().Unmarshal(c.PollResult.GetCoroutineState()); err != nil {
			return ErrorResponse(fmt.Errorf("%w: invalid coroutine state: %v", ErrIncompatibleState, err))
		}
	case *sdkv1.RunRequest_Input:
		var input Input
		if c.Input != nil {
			message := zero.ProtoReflect().New()
			options := proto.UnmarshalOptions{
				DiscardUnknown: true,
				RecursionLimit: protowire.DefaultRecursionLimit,
			}
			if err := options.Unmarshal(c.Input.Value, message.Interface()); err != nil {
				return ErrorResponse(fmt.Errorf("%w: invalid function input: %v", ErrInvalidArgument, err))
			}
			input = message.Interface().(Input)
		}
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(input))

	default:
		return ErrorResponse(fmt.Errorf("%w: unsupported coroutine directive: %T", ErrInvalidArgument, c))
	}

	res := &sdkv1.RunResponse{
		Status: sdkv1.Status_STATUS_OK,
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
			return ErrorResponse(context.Cause(ctx))
		}
	}

	if coro.Next() {
		coroutineState, err := coro.Context().Marshal()
		if err != nil {
			return ErrorResponse(fmt.Errorf("%w: cannot serialize coroutine: %v", ErrPermanent, err))
		}
		switch yield := coro.Recv().(type) {
		// TODO
		default:
			res = ErrorResponse(fmt.Errorf("%w: unsupported coroutine yield: %T", ErrInvalidResponse, yield))
		}
		// TODO
		_ = coroutineState
	} else {
		switch ret := coro.Result().(type) {
		case proto.Message:
			output, _ := anypb.New(ret)
			if status := statusOf(ret); status != UnspecifiedStatus {
				res.Status = status.proto()
			}
			res.Directive = &sdkv1.RunResponse_Exit{
				Exit: &sdkv1.Exit{
					Result: &sdkv1.CallResult{
						Output: output,
					},
				},
			}
		case error:
			res = ErrorResponse(ret)
		default:
			res = ErrorResponse(fmt.Errorf("%w: unsupported coroutine return: %T", ErrInvalidResponse, ret))
		}
	}

	return res
}

func (f *GenericFunction[Input, Output]) bind(endpoint *Dispatch) {
	f.endpoint = endpoint
}

// NewCall creates a Call for the function.
func (f *GenericFunction[Input, Output]) NewCall(input Input, opts ...CallOption) (Call, error) {
	if f.endpoint == nil {
		return Call{}, fmt.Errorf("cannot build function call: function has not been registered with a Dispatch endpoint")
	}
	return NewCall(f.endpoint.URL(), f.name, input, opts...)
}

// Dispatch dispatches a call to the function.
func (f *GenericFunction[Input, Output]) Dispatch(ctx context.Context, input Input, opts ...CallOption) (ID, error) {
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
func (f *GenericFunction[Input, Output]) entrypoint(input Input) func() any {
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
func NewPrimitiveFunction(name string, fn func(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse) *PrimitiveFunction {
	return &PrimitiveFunction{name: name, fn: fn}
}

// PrimitiveFunction is a function that's close to the underlying
// Dispatch protocol, accepting a RunRequest and returning a RunResponse.
type PrimitiveFunction struct {
	name string
	fn   func(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse

	endpoint *Dispatch
}

// Name is the name of the function.
func (f *PrimitiveFunction) Name() string {
	return f.name
}

// Run runs the function.
func (f *PrimitiveFunction) Run(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
	return f.fn(ctx, req)
}

func (f *PrimitiveFunction) bind(endpoint *Dispatch) {
	f.endpoint = endpoint
}

// NewCall creates a Call for the function.
func (f *PrimitiveFunction) NewCall(input proto.Message, opts ...CallOption) (Call, error) {
	if f.endpoint == nil {
		return Call{}, fmt.Errorf("cannot build function call: function has not been registered with a Dispatch endpoint")
	}
	return NewCall(f.endpoint.URL(), f.name, input, opts...)
}

// Dispatch dispatches a call to the function.
func (f *PrimitiveFunction) Dispatch(ctx context.Context, input proto.Message, opts ...CallOption) (ID, error) {
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
