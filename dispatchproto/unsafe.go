//go:build !durable

package dispatchproto

import (
	_ "unsafe"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

// These are hooks used by other dispatch-go packages that let us
// avoid exposing proto messages. Exposing the underlying proto
// messages complicates the API and opens up new failure modes.

//go:linkname newProtoCall
func newProtoCall(proto *sdkv1.Call) Call { //nolint
	return Call{proto}
}

//go:linkname newProtoAny
func newProtoAny(proto *anypb.Any) Any { //nolint
	return Any{proto}
}

//go:linkname newProtoResponse
func newProtoResponse(proto *sdkv1.RunResponse) Response { //nolint
	return Response{proto}
}

//go:linkname newProtoRequest
func newProtoRequest(proto *sdkv1.RunRequest) Request { //nolint
	return Request{proto}
}

//go:linkname callProto
func callProto(c Call) *sdkv1.Call { //nolint
	return c.proto
}

//go:linkname anyProto
func anyProto(a Any) *anypb.Any { //nolint
	return a.proto
}

//go:linkname requestProto
func requestProto(r Request) *sdkv1.RunRequest { //nolint
	return r.proto
}

//go:linkname responseProto
func responseProto(r Response) *sdkv1.RunResponse { //nolint
	return r.proto
}
