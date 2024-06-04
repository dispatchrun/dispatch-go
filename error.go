package dispatch

import (
	"errors"
	"reflect"
	"strings"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
)

func errorTypeOf(err error) string {
	if err == nil {
		return ""
	}
	typ := reflect.TypeOf(err)
	if name := typ.Name(); name != "" {
		return name
	}
	str := typ.String()
	if i := strings.LastIndexByte(str, '.'); i >= 0 {
		return str[i+1:]
	}
	return str
}

func errorStatusOf(err error) Status {
	if err == nil {
		return OKStatus
	}
	if isTimeout(err) {
		return TimeoutStatus
	}
	if isTemporary(err) {
		return TemporaryErrorStatus
	}
	return PermanentErrorStatus
}

func isTemporary(err error) bool {
	if ok, found := errorIsTemporary(err); found {
		return ok
	} else {
		return isTimeout(err)
	}
}

func isTimeout(err error) bool {
	ok, _ := errorIsTimeout(err)
	return ok
}

func errorIsTemporary(err error) (ok, found bool) {
	var t temporary
	if errors.As(err, &t) {
		return t.Temporary(), true
	}
	return
}

func errorIsTimeout(err error) (ok, found bool) {
	var t timeout
	if errors.As(err, &t) {
		return t.Timeout(), true
	}
	return
}

type temporary interface {
	Temporary() bool
}

type timeout interface {
	Timeout() bool
}

// ErrorResponse creates a RunResponse for the specified error.
func ErrorResponse(status Status, err error) *sdkv1.RunResponse {
	if status == UnspecifiedStatus {
		status = errorStatusOf(err)
	}
	return &sdkv1.RunResponse{
		Status: status.proto(),
		Directive: &sdkv1.RunResponse_Exit{
			Exit: &sdkv1.Exit{
				Result: &sdkv1.CallResult{
					Error: &sdkv1.Error{
						Type:    errorTypeOf(err),
						Message: err.Error(),
					},
				},
			},
		},
	}
}
