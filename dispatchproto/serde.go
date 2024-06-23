package dispatchproto

import (
	"fmt"

	"github.com/dispatchrun/coroutine/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func init() {
	types.Register(protoSerializer, protoDeserializer)
}

func protoSerializer(s *types.Serializer, mp *proto.Message) error {
	m := *mp
	if m == nil {
		types.SerializeT(s, false)
		return nil
	}

	any, err := anypb.New(m)
	if err != nil {
		return fmt.Errorf("anypb.New: %w", err)
	}
	b, err := proto.Marshal(any)
	if err != nil {
		return fmt.Errorf("proto.Marshal: %w", err)
	}

	types.SerializeT(s, true)
	types.SerializeT(s, b)

	return nil
}

func protoDeserializer(d *types.Deserializer, mp *proto.Message) error {
	var ok bool
	types.DeserializeTo(d, &ok)
	if !ok {
		*mp = nil
		return nil
	}

	var b []byte
	types.DeserializeTo(d, &b)

	var any anypb.Any
	if err := proto.Unmarshal(b, &any); err != nil {
		return fmt.Errorf("proto.Unmarshal: %w", err)
	}
	m, err := any.UnmarshalNew()
	if err != nil {
		return fmt.Errorf("anypb.UnmarshalNew: %w", err)
	}
	*mp = m

	return nil
}
