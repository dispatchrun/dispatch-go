package dispatch

import (
	"context"
	"sync"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

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
