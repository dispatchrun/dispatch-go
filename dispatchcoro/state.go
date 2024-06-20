//go:build !durable

package dispatchcoro

import (
	"fmt"
	_ "unsafe"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"google.golang.org/protobuf/types/known/anypb"
)

const durableCoroutineStateTypeUrl = "buf.build/stealthrocket/coroutine/coroutine.v1.State"

// Serialize serializes a coroutine.
func Serialize(coro Coroutine) (dispatchproto.Any, error) {
	rawState, err := coro.Context().Marshal()
	if err != nil {
		return dispatchproto.Any{}, fmt.Errorf("cannot serialize coroutine: %w", err)
	}
	return newProtoAny(&anypb.Any{
		TypeUrl: durableCoroutineStateTypeUrl,
		Value:   rawState,
	}), nil
}

// Deserialize deserializes a coroutine.
func Deserialize(coro Coroutine, state dispatchproto.Any) error {
	if state.TypeURL() != durableCoroutineStateTypeUrl {
		return fmt.Errorf("cannot deserialize coroutine state: unexpected type URL %q", state.TypeURL())
	}
	if err := coro.Context().Unmarshal(anyProto(state).GetValue()); err != nil {
		return fmt.Errorf("cannot deserialize coroutine state: %w", err)
	}
	return nil
}

//go:linkname newProtoAny github.com/dispatchrun/dispatch-go/dispatchproto.newProtoAny
func newProtoAny(*anypb.Any) dispatchproto.Any

//go:linkname anyProto github.com/dispatchrun/dispatch-go/dispatchproto.anyProto
func anyProto(r dispatchproto.Any) *anypb.Any
