package dispatch

import (
	"context"
	"fmt"
	"slices"
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
func NewFunction[I, O any](name string, fn func(context.Context, I) (O, error)) *GenericFunction[I, O] {
	return &GenericFunction[I, O]{name: name, fn: fn}
}

// GenericFunction is a Dispatch function that accepts arbitrary input
// and returns arbitrary output.
type GenericFunction[I, O any] struct {
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
	boxedInput, ok := req.Input()
	if !ok {
		return NewResponseErrorf("%w: unsupported request directive: %v", ErrInvalidArgument, req)
	}
	var input I
	if err := boxedInput.Unmarshal(&input); err != nil {
		return NewResponseErrorf("%w: invalid input %v: %v", ErrInvalidArgument, boxedInput, err)
	}
	output, err := f.fn(ctx, input)
	if err != nil {
		return NewResponseError(err)
	}
	boxedOutput, err := NewAny(output)
	if err != nil {
		return NewResponseErrorf("%w: cannot serialize return value %v: %v", ErrInvalidResponse, output, err)
	}
	return NewResponse(StatusOf(output), Output(boxedOutput))
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
