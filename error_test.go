package dispatch

import (
	"errors"
	"fmt"
	"testing"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
)

func TestErrorStatusOf(t *testing.T) {
	tests := []struct {
		error  error
		status sdkv1.Status
	}{
		{nil, sdkv1.Status_STATUS_OK},
		{temporaryError{}, sdkv1.Status_STATUS_TEMPORARY_ERROR},
		{timeoutError{}, sdkv1.Status_STATUS_TIMEOUT},
		{errors.New("permanent"), sdkv1.Status_STATUS_PERMANENT_ERROR},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.error), func(t *testing.T) {
			if status := errorStatusOf(test.error); status != test.status {
				t.Errorf("errorStatusOf(%q) = %v, want %v", test.error, status, test.status)
			}
		})
	}
}

type temporaryError struct{}

func (temporaryError) Error() string   { return "temporary" }
func (temporaryError) Temporary() bool { return true }

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Temporary() bool { return true }
func (timeoutError) Timeout() bool   { return true }
