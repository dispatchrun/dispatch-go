//go:build !durable

package dispatch

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// AnyFunction is the interface implemented by all Dispatch functions.
//
// See Func and PrimitiveFunc.
type AnyFunction interface {
	name() string

	run(context.Context, dispatchproto.Request) dispatchproto.Response

	bind(*Dispatch)
}

// FunctionRegistry is a collection of Dispatch functions.
type FunctionRegistry struct {
	functions map[string]AnyFunction

	mu sync.Mutex
}

// Register registers functions.
func (r *FunctionRegistry) Register(fns ...AnyFunction) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.functions == nil {
		r.functions = map[string]AnyFunction{}
	}
	for _, fn := range fns {
		r.functions[fn.name()] = fn
	}
}

func (r *FunctionRegistry) lookup(name string) AnyFunction {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.functions[name]
}

// RoundTrip makes a request to a function in the registry
// and returns its response.
func (r *FunctionRegistry) RoundTrip(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
	fn := r.lookup(req.Function())
	if fn == nil {
		return dispatchproto.NewResponseErrorf("%w: function %q not found", ErrNotFound, req.Function())
	}
	return fn.run(ctx, req)
}

// Close closes the function registry.
func (r *FunctionRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, fn := range r.functions {
		if c, ok := fn.(interface{ close() }); ok {
			c.close()
		}
	}
	clear(r.functions)
	return nil
}

// PrimitiveFunc creates a PrimitiveFunction.
//
// Most users should instead use Func to create a Dispatch Function.
func PrimitiveFunc(name string, function func(context.Context, dispatchproto.Request) dispatchproto.Response) *PrimitiveFunction {
	return &PrimitiveFunction{functionName: name, function: function}
}

// PrimitiveFunction is a Function that's close to the underlying
// Dispatch protocol, accepting a dispatchproto.Request and returning
// a dispatchproto.Response.
type PrimitiveFunction struct {
	functionName string

	function func(context.Context, dispatchproto.Request) dispatchproto.Response

	endpoint *Dispatch
}

// BuildCall creates (but does not dispatch) a Call for the function.
func (f *PrimitiveFunction) BuildCall(input dispatchproto.Any, opts ...dispatchproto.CallOption) (dispatchproto.Call, error) {
	var url string
	if f.endpoint != nil {
		url = f.endpoint.URL()
	}
	opts = append(slices.Clip(opts), input)
	return dispatchproto.NewCall(url, f.functionName, opts...), nil
}

// Dispatch dispatches a call to the function.
func (f *PrimitiveFunction) Dispatch(ctx context.Context, input dispatchproto.Any, opts ...dispatchproto.CallOption) (dispatchproto.ID, error) {
	call, err := f.BuildCall(input, opts...)
	if err != nil {
		return "", err
	}
	if f.endpoint == nil {
		return "", fmt.Errorf("cannot dispatch function call: function has not been registered with a Dispatch endpoint")
	}
	client, err := f.endpoint.Client()
	if err != nil {
		return "", fmt.Errorf("cannot dispatch function call: %w", err)
	}
	return client.Dispatch(ctx, call)
}

func (f *PrimitiveFunction) name() string {
	return f.functionName
}

func (f *PrimitiveFunction) run(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
	if name := req.Function(); name != f.functionName {
		return dispatchproto.NewResponseErrorf("%w: function %q received call for function %q", ErrInvalidArgument, f.functionName, name)
	}
	return f.function(ctx, req)
}

func (f *PrimitiveFunction) bind(endpoint *Dispatch) {
	f.endpoint = endpoint
}
