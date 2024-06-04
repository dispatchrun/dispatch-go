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
}

// NewPrimitiveFunction creates a PrimitiveFunction.
func NewPrimitiveFunction(name string, fn func(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse) *PrimitiveFunction {
	return &PrimitiveFunction{name, fn}
}

// PrimitiveFunction is a function that's close to the underlying
// Dispatch protocol, accepting a RunRequest and returning a RunResponse.
type PrimitiveFunction struct {
	name string
	fn   func(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse
}

// Name is the name of the function.
func (p *PrimitiveFunction) Name() string {
	return p.name
}

// Run runs the function.
func (p *PrimitiveFunction) Run(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
	return p.fn(ctx, req)
}

// NewGenericFunction creates a GenericFunction.
func NewGenericFunction[Input, Output proto.Message](name string, fn func(context.Context, Input) (Output, error)) *GenericFunction[Input, Output] {
	return &GenericFunction[Input, Output]{name, fn}
}

// GenericFunction is a higher level Dispatch function that accepts
// arbitrary input and returns arbitrary output.
type GenericFunction[Input, Output proto.Message] struct {
	name string
	fn   func(ctx context.Context, input Input) (Output, error)
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
			return ErrorResponse(IncompatibleStateStatus, fmt.Errorf("invalid coroutine state: %w", err))
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
				return ErrorResponse(InvalidArgumentStatus, fmt.Errorf("invalid function input: %w", err))
			}
			input = message.Interface().(Input)
		}
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(input))

	default:
		return ErrorResponse(InvalidArgumentStatus, fmt.Errorf("unsupported coroutine directive: %T", c))
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
			return ErrorResponse(UnspecifiedStatus, context.Cause(ctx))
		}
	}

	if coro.Next() {
		coroutineState, err := coro.Context().Marshal()
		if err != nil {
			return ErrorResponse(PermanentErrorStatus, fmt.Errorf("cannot serialize coroutine: %w", err))
		}
		switch yield := coro.Recv().(type) {
		// TODO
		default:
			res = ErrorResponse(InvalidResponseStatus, fmt.Errorf("unsupported coroutine yield: %T", yield))
		}
		// TODO
		_ = coroutineState
	} else {
		switch ret := coro.Result().(type) {
		case proto.Message:
			output, _ := anypb.New(ret)
			res.Status = statusOf(ret)
			res.Directive = &sdkv1.RunResponse_Exit{
				Exit: &sdkv1.Exit{
					Result: &sdkv1.CallResult{
						Output: output,
					},
				},
			}
		case error:
			res = ErrorResponse(UnspecifiedStatus, ret)
		default:
			res = ErrorResponse(InvalidResponseStatus, fmt.Errorf("unsupported coroutine return: %T", ret))
		}
	}

	return res
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

func statusOf(msg proto.Message) sdkv1.Status {
	if m, ok := msg.(interface{ Status() sdkv1.Status }); ok {
		return m.Status()
	}
	return sdkv1.Status_STATUS_OK
}
