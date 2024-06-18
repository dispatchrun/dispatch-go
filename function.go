package dispatch

import (
	"context"
	"fmt"
	"slices"
	"sync"
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

// Registry is a collection of Dispatch functions.
type Registry struct {
	functions map[string]Function

	mu sync.Mutex
}

// Register registers a function.
func (r *Registry) Register(fn Function) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.functions == nil {
		r.functions = map[string]Function{}
	}

	r.functions[fn.Name()] = fn
}

// Lookup retrieves a function by name.
func (r *Registry) Lookup(name string) Function {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.functions[name]
}

// Run runs a function.
func (r *Registry) Run(ctx context.Context, req Request) Response {
	fn := r.Lookup(req.Function())
	if fn == nil {
		return NewResponseErrorf("%w: function %q not found", ErrNotFound, req.Function())
	}
	return fn.Run(ctx, req)
}

// NewFunction creates a Dispatch Function.
func NewFunction[I, O any](name string, fn func(context.Context, I) (O, error)) *GenericFunction[I, O] {
	return &GenericFunction[I, O]{PrimitiveFunction{name: name}, fn}
}

// GenericFunction is a Function that accepts any input and returns any output.
type GenericFunction[I, O any] struct {
	PrimitiveFunction

	fn func(ctx context.Context, input I) (O, error)
}

var _ Function = (*GenericFunction[int, int])(nil)

// Run runs the function.
func (f *GenericFunction[I, O]) Run(ctx context.Context, req Request) Response {
	input, err := f.unpackInput(req)
	if err != nil {
		return NewResponseError(err)
	}
	output, err := f.fn(ctx, input)
	if err != nil {
		return NewResponseError(err)
	}
	return f.packOutput(output)
}

func (f *GenericFunction[I, O]) unpackInput(req Request) (I, error) {
	var input I
	boxedInput, ok := req.Input()
	if !ok {
		return input, fmt.Errorf("%w: unsupported request: %v", ErrInvalidArgument, req)
	}
	if err := boxedInput.Unmarshal(&input); err != nil {
		return input, fmt.Errorf("%w: invalid input %v: %v", ErrInvalidArgument, boxedInput, err)
	}
	return input, nil
}

func (f *GenericFunction[I, O]) packOutput(output O) Response {
	boxedOutput, err := NewAny(output)
	if err != nil {
		return NewResponseErrorf("%w: invalid output %v: %v", ErrInvalidResponse, output, err)
	}
	return NewResponse(StatusOf(output), Output(boxedOutput))
}

// NewCall creates a Call for the function.
func (f *GenericFunction[I, O]) NewCall(input I, opts ...CallOption) (Call, error) {
	boxedInput, err := NewAny(input)
	if err != nil {
		return Call{}, fmt.Errorf("cannot serialize input: %v", err)
	}
	return f.PrimitiveFunction.NewCall(boxedInput, opts...)
}

// Dispatch dispatches a Call to the function.
func (f *GenericFunction[I, O]) Dispatch(ctx context.Context, input I, opts ...CallOption) (ID, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return "", err
	}
	return f.dispatchCall(ctx, call)
}

// NewPrimitiveFunction creates a PrimitiveFunction.
func NewPrimitiveFunction(name string, fn func(context.Context, Request) Response) *PrimitiveFunction {
	return &PrimitiveFunction{name: name, fn: fn}
}

// PrimitiveFunction is a Function that's close to the underlying
// Dispatch protocol, accepting a Request and returning a Response.
type PrimitiveFunction struct {
	name string

	fn func(context.Context, Request) Response

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
	return f.dispatchCall(ctx, call)
}

func (f *PrimitiveFunction) dispatchCall(ctx context.Context, call Call) (ID, error) {
	client, err := f.endpoint.Client()
	if err != nil {
		return "", fmt.Errorf("cannot dispatch function call: %w", err)
	}
	return client.Dispatch(ctx, call)
}
