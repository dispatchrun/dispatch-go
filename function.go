//go:build !durable

package dispatch

import (
	"context"
	"fmt"
	"sync"
	"time"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"github.com/stealthrocket/coroutine"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Registry is a registry of functions.
type Registry struct {
	functions map[string]NamedFunction
	mu        sync.Mutex
}

// Register registers a function.
func (r *Dispatch) Register(fn NamedFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.functions == nil {
		r.functions = map[string]NamedFunction{}
	}
	r.functions[fn.Name()] = fn
}

// Lookup looks up a function by name.
//
// It returns nil if a function with that name has not been registered.
func (r *Registry) Lookup(name string) NamedFunction {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.functions[name]
}

// Run forwards a request to a function in the registry.
func (r *Registry) Run(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
	fn := r.Lookup(req.Function)
	if fn == nil {
		return ErrorResponse(sdkv1.Status_STATUS_NOT_FOUND, fmt.Errorf("function %q not found", req.Function))
	}
	return fn.Run(ctx, req)
}

// NamedFunction is a Dispatch function with a name.
type NamedFunction interface {
	// Name is the name of the function.
	Name() string

	// Run runs the function.
	Run(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse
}

// NewPrimitiveFunction creates a primitive function that
// accepts a RunRequest and returns a RunResponse.
func NewPrimitiveFunction(name string, fn func(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse) NamedFunction {
	return &primitiveFunction{name, fn}
}

type primitiveFunction struct {
	name string
	fn   func(context.Context, *sdkv1.RunRequest) *sdkv1.RunResponse
}

func (p *primitiveFunction) Name() string {
	return p.name
}

func (p *primitiveFunction) Run(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
	return p.fn(ctx, req)
}

// NewFunction creates a Dispatch function.
func NewFunction[Input, Output proto.Message](name string, fn func(context.Context, Input) (Output, error)) *Function[Input, Output] {
	return &Function[Input, Output]{name, fn}
}

// Function is a generic function type that can be used as entrypoint to a
// durable coroutine.
type Function[Input, Output proto.Message] struct {
	name string
	fn   func(ctx context.Context, input Input) (Output, error)
}

// Name is the name of the function.
func (f *Function[Input, Output]) Name() string {
	return f.name
}

// Run runs the Dispatch function.
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
func (f *Function[Input, Output]) Run(ctx context.Context, req *sdkv1.RunRequest) *sdkv1.RunResponse {
	// TODO: since the coroutine yield and return values are the same the only
	// common denominator is any. We could improve type safety if we were able
	// to separate the two.
	var coro coroutine.Coroutine[any, any]
	var zero Input

	switch c := req.Directive.(type) {
	case *sdkv1.RunRequest_PollResult:
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(zero))
		if err := coro.Context().Unmarshal(c.PollResult.GetCoroutineState()); err != nil {
			return ErrorResponse(sdkv1.Status_STATUS_INCOMPATIBLE_STATE, fmt.Errorf("invalid coroutine state: %w", err))
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
				return ErrorResponse(sdkv1.Status_STATUS_INVALID_ARGUMENT, fmt.Errorf("invalid function input: %w", err))
			}
			input = message.Interface().(Input)
		}
		coro = coroutine.NewWithReturn[any, any](f.entrypoint(input))

	default:
		return ErrorResponse(sdkv1.Status_STATUS_INVALID_ARGUMENT, fmt.Errorf("unsupported coroutine directive: %T", c))
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
			return ErrorResponse(sdkv1.Status_STATUS_UNSPECIFIED, context.Cause(ctx))
		}
	}

	if coro.Next() {
		coroutineState, err := coro.Context().Marshal()
		if err != nil {
			return ErrorResponse(sdkv1.Status_STATUS_PERMANENT_ERROR, fmt.Errorf("cannot serialize coroutine: %w", err))
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
			res = ErrorResponse(sdkv1.Status_STATUS_INVALID_RESPONSE, fmt.Errorf("unsupported coroutine yield: %T", yield))
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
			res = ErrorResponse(sdkv1.Status_STATUS_UNSPECIFIED, ret)
		default:
			res = ErrorResponse(sdkv1.Status_STATUS_INVALID_RESPONSE, fmt.Errorf("unsupported coroutine return: %T", ret))
		}
	}

	return res
}

//go:noinline
func (f *Function[Input, Output]) entrypoint(input Input) func() any {
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
