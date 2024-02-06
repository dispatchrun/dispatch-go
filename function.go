//go:build !durable

package dispatch

import (
	"context"
	"fmt"
	"time"

	"github.com/stealthrocket/coroutine"
	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-sdk/protocolbuffers/go/dispatch/sdk/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Func is a constructor for Function values. In most cases, it is useful to
// infer the type parameters from the signature of the function passed as
// argument.
func Func[Input, Output proto.Message](f func(context.Context, Input) (Output, error)) Function[Input, Output] {
	return Function[Input, Output](f)
}

// Function is a generic function type that can be used as entrypoint to a
// durable coroutine.
type Function[Input, Output proto.Message] func(ctx context.Context, input Input) (Output, error)

// Run implements a dispatch function.
//
// The request passed as argument is interpreted to either start a new coroutine
// or resume from a previous state. If the coroutine yields, the returned
// response embeds a sdkv1.Poll message capturing its state; otherwise, the
// response contains either the result of the execution or an error.
//
// Note that the ability to execute durable coroutines relies on the program
// being compiled with -tags=durable. Without this build tag, coroutines are
// volatiles and the method acts as a simple invocation that runs the whole
// function to completion.
func (f Function[Input, Output]) Run(ctx context.Context, req *sdkv1.RunRequest) (*sdkv1.RunResponse, error) {
	// TODO: since the coroutine yield and return values are the same the only
	// common denominator is any. We could improve type safety if we were able
	// to separate the two.
	var coro coroutine.Coroutine[any, any]
	var zero Input

	switch c := req.Directive.(type) {
	case *sdkv1.RunRequest_PollResult:
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(zero))
		if err := coro.Context().Unmarshal(c.PollResult.GetCoroutineState()); err != nil {
			return nil, err
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
				return nil, err
			}
			input = message.Interface().(Input)
		}
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(input))

	default:
		return nil, fmt.Errorf("unsupported coroutine type: %T", c)
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
			switch yield := v.(type) {
			case sleep:
				select {
				case <-time.After(time.Duration(yield)):
				case <-ctx.Done():
					coro.Stop()
					canceled = true
				}
			}
			return nil
		})
		if canceled {
			return nil, context.Cause(ctx)
		}
	}

	if coro.Next() {
		coroutineState, err := coro.Context().Marshal()
		if err != nil {
			return nil, err
		}
		switch yield := coro.Recv().(type) {
		case sleep:
			res.Status = sdkv1.Status_STATUS_OK // TODO: is it the expected status for suspended coroutines?
			res.Directive = &sdkv1.RunResponse_Poll{
				Poll: &sdkv1.Poll{
					CoroutineState: coroutineState,
					MaxWait:        durationpb.New(time.Duration(yield)),
				},
			}
		default:
			return nil, fmt.Errorf("coroutine yielded an unsupported value type: %T", yield)
		}
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
			res.Status = errorStatusOf(ret)
			res.Directive = &sdkv1.RunResponse_Exit{
				Exit: &sdkv1.Exit{
					Result: &sdkv1.CallResult{
						Error: &sdkv1.Error{
							Type:    errorTypeOf(ret),
							Message: ret.Error(),
						},
					},
				},
			}
		default:
			return nil, fmt.Errorf("coroutine returned an unsupported value type: %T", ret)
		}
	}

	return res, nil
}

// TODO: remove explicit noinline directive once stealthrocket/coroutine#84 is fixed.
//
//go:noinline
func (f Function[Input, Output]) entrypoint(input Input) func() any {
	return func() any {
		// The context that gets passed as argument here should be recreated
		// each time the coroutine is resumed, ideally inheriting from the
		// parent context passed to the Run method. This is difficult to
		// do right in durable mode because we shouldn't capture the parent
		// context in the coroutine state.
		if res, err := f(context.TODO(), input); err != nil {
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
