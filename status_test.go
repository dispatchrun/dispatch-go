package dispatch

import (
	"math"
	"testing"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
)

func TestStatus(t *testing.T) {
	checked := map[Status]struct{}{}
	minStatus := Status(math.MaxInt32)
	maxStatus := Status(math.MinInt32)

	for _, test := range []struct {
		status Status
		proto  sdkv1.Status
		string string
	}{
		{
			status: UnspecifiedStatus,
			proto:  sdkv1.Status_STATUS_UNSPECIFIED,
			string: "Unspecified",
		},
		{
			status: OKStatus,
			proto:  sdkv1.Status_STATUS_OK,
			string: "OK",
		},
		{
			status: TimeoutStatus,
			proto:  sdkv1.Status_STATUS_TIMEOUT,
			string: "Timeout",
		},
		{
			status: ThrottledStatus,
			proto:  sdkv1.Status_STATUS_THROTTLED,
			string: "Throttled",
		},
		{
			status: InvalidArgumentStatus,
			proto:  sdkv1.Status_STATUS_INVALID_ARGUMENT,
			string: "InvalidArgument",
		},
		{
			status: InvalidResponseStatus,
			proto:  sdkv1.Status_STATUS_INVALID_RESPONSE,
			string: "InvalidResponse",
		},
		{
			status: TemporaryErrorStatus,
			proto:  sdkv1.Status_STATUS_TEMPORARY_ERROR,
			string: "TemporaryError",
		},
		{
			status: PermanentErrorStatus,
			proto:  sdkv1.Status_STATUS_PERMANENT_ERROR,
			string: "PermanentError",
		},
		{
			status: IncompatibleStateStatus,
			proto:  sdkv1.Status_STATUS_INCOMPATIBLE_STATE,
			string: "IncompatibleState",
		},
		{
			status: DNSErrorStatus,
			proto:  sdkv1.Status_STATUS_DNS_ERROR,
			string: "DNSError",
		},
		{
			status: TCPErrorStatus,
			proto:  sdkv1.Status_STATUS_TCP_ERROR,
			string: "TCPError",
		},
		{
			status: TLSErrorStatus,
			proto:  sdkv1.Status_STATUS_TLS_ERROR,
			string: "TLSError",
		},
		{
			status: HTTPErrorStatus,
			proto:  sdkv1.Status_STATUS_HTTP_ERROR,
			string: "HTTPError",
		},
		{
			status: UnauthenticatedStatus,
			proto:  sdkv1.Status_STATUS_UNAUTHENTICATED,
			string: "Unauthenticated",
		},
		{
			status: PermissionDeniedStatus,
			proto:  sdkv1.Status_STATUS_PERMISSION_DENIED,
			string: "PermissionDenied",
		},
		{
			status: NotFoundStatus,
			proto:  sdkv1.Status_STATUS_NOT_FOUND,
			string: "NotFound",
		},
	} {
		t.Run(test.string, func(t *testing.T) {
			if got := sdkv1.Status(test.status); got != test.proto {
				t.Errorf("unexpected proto status: got %v, want %v", got, test.proto)
			}
			if got := test.status.String(); got != test.string {
				t.Errorf("unexpected string: got %v, want %v", got, test.string)
			}
			if got, want := test.status.GoString(), test.string+"Status"; got != want {
				t.Errorf("unexpected go string: got %v, want %v", got, want)
			}
		})

		minStatus = min(minStatus, test.status)
		maxStatus = max(maxStatus, test.status)
		checked[test.status] = struct{}{}
	}

	if minStatus != 0 || maxStatus <= minStatus {
		t.Fatalf("not all statuses were checked")
	}
	for i := minStatus; i <= maxStatus; i++ {
		if _, ok := sdkv1.Status_name[int32(i)]; !ok {
			t.Fatalf("status %v is invalid", i)
		}
	}
	if _, ok := sdkv1.Status_name[int32(maxStatus)+1]; ok {
		// This indicates that a new status (or statuses) have been
		// added to github.com/dispatchrun/dispatch-proto.
		t.Fatalf("status %v was not the last status", maxStatus)
	}
}
