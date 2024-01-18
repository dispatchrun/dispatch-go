package dispatch

import (
	"errors"
	"reflect"
	"strings"

	statusv1 "github.com/stealthrocket/ring/proto/go/ring/status/v1"
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

func errorStatusOf(err error) statusv1.Status {
	if err == nil {
		return statusv1.Status_STATUS_OK
	}
	if isTimeout(err) {
		return statusv1.Status_STATUS_TIMEOUT
	}
	if isTemporary(err) {
		return statusv1.Status_STATUS_TEMPORARY_ERROR
	}
	return statusv1.Status_STATUS_PERMANENT_ERROR
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
