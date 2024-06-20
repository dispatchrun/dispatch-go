//go:build !durable

package dispatch

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

// Function is a Dispatch function.
type Function interface {
	// Name is the name of the function.
	Name() string

	// Run runs the function.
	Run(context.Context, dispatchproto.Request) dispatchproto.Response

	// Coroutine is true if the function is a coroutine that can be
	// suspended and resumed.
	Coroutine() bool

	// Close closes the function.
	Close() error

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

// Register registers functions.
func (r *Registry) Register(fns ...Function) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.functions == nil {
		r.functions = map[string]Function{}
	}

	for _, fn := range fns {
		r.functions[fn.Name()] = fn
	}
}

// Lookup retrieves a function by name.
func (r *Registry) Lookup(name string) Function {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.functions[name]
}

// Run runs a function.
func (r *Registry) Run(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
	fn := r.Lookup(req.Function())
	if fn == nil {
		return dispatchproto.NewResponseErrorf("%w: function %q not found", ErrNotFound, req.Function())
	}
	return fn.Run(ctx, req)
}

// Close closes the registry and all functions within it.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for _, fn := range r.functions {
		if closeErr := fn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	clear(r.functions)
	return err
}

// PrimitiveFunc creates a PrimitiveFunction.
//
// Most users should instead use Func to create a Dispatch Function.
func PrimitiveFunc(name string, fn func(context.Context, dispatchproto.Request) dispatchproto.Response) *PrimitiveFunction {
	return &PrimitiveFunction{name: name, fn: fn}
}

// PrimitiveFunction is a Function that's close to the underlying
// Dispatch protocol, accepting a dispatchproto.Request and returning
// a dispatchproto.Response.
type PrimitiveFunction struct {
	name string

	fn func(context.Context, dispatchproto.Request) dispatchproto.Response

	endpoint *Dispatch
}

// Name is the name of the function.
func (f *PrimitiveFunction) Name() string {
	return f.name
}

// Run runs the function.
func (f *PrimitiveFunction) Run(ctx context.Context, req dispatchproto.Request) dispatchproto.Response {
	if name := req.Function(); name != f.name {
		return dispatchproto.NewResponseErrorf("%w: function %q received call for function %q", ErrInvalidArgument, f.name, name)
	}
	return f.fn(ctx, req)
}

func (f *PrimitiveFunction) Coroutine() bool {
	return false
}

func (f *PrimitiveFunction) Close() error {
	return nil
}

func (f *PrimitiveFunction) bind(endpoint *Dispatch) {
	f.endpoint = endpoint
}

// NewCall creates a Call for the function.
func (f *PrimitiveFunction) NewCall(input dispatchproto.Any, opts ...dispatchproto.CallOption) (dispatchproto.Call, error) {
	var url string
	if f.endpoint != nil {
		url = f.endpoint.URL()
	}
	opts = append(slices.Clip(opts), input)
	return dispatchproto.NewCall(url, f.name, opts...), nil
}

// Dispatch dispatches a call to the function.
func (f *PrimitiveFunction) Dispatch(ctx context.Context, input dispatchproto.Any, opts ...dispatchproto.CallOption) (ID, error) {
	call, err := f.NewCall(input, opts...)
	if err != nil {
		return "", err
	}
	return f.dispatchCall(ctx, call)
}

func (f *PrimitiveFunction) dispatchCall(ctx context.Context, call dispatchproto.Call) (ID, error) {
	if f.endpoint == nil {
		return "", fmt.Errorf("cannot dispatch function call: function has not been registered with a Dispatch endpoint")
	}
	client, err := f.endpoint.Client()
	if err != nil {
		return "", fmt.Errorf("cannot dispatch function call: %w", err)
	}
	return client.Dispatch(ctx, call)
}
