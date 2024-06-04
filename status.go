package dispatch

import (
	"fmt"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
)

// Status categorizes the success or failure conditions resulting from
// an execution request.
type Status sdkv1.Status

const (
	UnspecifiedStatus       = Status(sdkv1.Status_STATUS_UNSPECIFIED)
	OKStatus                = Status(sdkv1.Status_STATUS_OK)
	TimeoutStatus           = Status(sdkv1.Status_STATUS_TIMEOUT)
	ThrottledStatus         = Status(sdkv1.Status_STATUS_THROTTLED)
	InvalidArgumentStatus   = Status(sdkv1.Status_STATUS_INVALID_ARGUMENT)
	InvalidResponseStatus   = Status(sdkv1.Status_STATUS_INVALID_RESPONSE)
	TemporaryErrorStatus    = Status(sdkv1.Status_STATUS_TEMPORARY_ERROR)
	PermanentErrorStatus    = Status(sdkv1.Status_STATUS_PERMANENT_ERROR)
	IncompatibleStateStatus = Status(sdkv1.Status_STATUS_INCOMPATIBLE_STATE)
	DNSErrorStatus          = Status(sdkv1.Status_STATUS_DNS_ERROR)
	TCPErrorStatus          = Status(sdkv1.Status_STATUS_TCP_ERROR)
	TLSErrorStatus          = Status(sdkv1.Status_STATUS_TLS_ERROR)
	HTTPErrorStatus         = Status(sdkv1.Status_STATUS_HTTP_ERROR)
	UnauthenticatedStatus   = Status(sdkv1.Status_STATUS_UNAUTHENTICATED)
	PermissionDeniedStatus  = Status(sdkv1.Status_STATUS_PERMISSION_DENIED)
	NotFoundStatus          = Status(sdkv1.Status_STATUS_NOT_FOUND)
)

var statusNames = [...]string{
	UnspecifiedStatus:       "UnspecifiedStatus",
	OKStatus:                "OKStatus",
	TimeoutStatus:           "TimeoutStatus",
	ThrottledStatus:         "ThrottledStatus",
	InvalidArgumentStatus:   "InvalidArgumentStatus",
	InvalidResponseStatus:   "InvalidResponseStatus",
	TemporaryErrorStatus:    "TemporaryErrorStatus",
	PermanentErrorStatus:    "PermanentErrorStatus",
	IncompatibleStateStatus: "IncompatibleStateStatus",
	DNSErrorStatus:          "DNSErrorStatus",
	TCPErrorStatus:          "TCPErrorStatus",
	TLSErrorStatus:          "TLSErrorStatus",
	HTTPErrorStatus:         "HTTPErrorStatus",
	UnauthenticatedStatus:   "UnauthenticatedStatus",
	PermissionDeniedStatus:  "PermissionDeniedStatus",
	NotFoundStatus:          "NotFoundStatus",
}

func (s Status) proto() sdkv1.Status {
	return sdkv1.Status(s)
}

func (s Status) String() string {
	if s < 0 || int(s) >= len(statusNames) {
		return fmt.Sprintf("Status(%d)", int(s))
	}
	return statusNames[s]
}