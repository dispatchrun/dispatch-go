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
		err := NewErrorf("function %q not found", req.Function())
		return NewResponse(NotFoundStatus, err)
	}
	return fn(ctx, req)
}
