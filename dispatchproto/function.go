//go:build !durable

package dispatchproto

import "context"

// Function is a Dispatch function.
type Function func(context.Context, Request) Response

// FunctionMap is a map of Dispatch functions.
type FunctionMap map[string]Function

// Run runs a function.
func (m FunctionMap) Run(ctx context.Context, req Request) Response {
	fn, ok := m[req.Function()]
	if !ok {
		return NewResponse(NotFoundStatus, Errorf("function %q not found", req.Function()))
	}
	return fn(ctx, req)
}
