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

func errorStatusOf(err error) sdkv1.Status {
	if err == nil {
		return sdkv1.Status_STATUS_OK
	}
	if isTimeout(err) {
		return sdkv1.Status_STATUS_TIMEOUT
	}
	if isTemporary(err) {
		return sdkv1.Status_STATUS_TEMPORARY_ERROR
	}
	return sdkv1.Status_STATUS_PERMANENT_ERROR
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

func errResponse(status sdkv1.Status, err error) *sdkv1.RunResponse {
	if status == sdkv1.Status_STATUS_UNSPECIFIED {
		status = errorStatusOf(err)
	}
	return &sdkv1.RunResponse{
		Status: status,
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
