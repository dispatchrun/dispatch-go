//go:build !durable

package dispatch

import (
	"fmt"

	"github.com/dispatchrun/coroutine/types"
	"google.golang.org/protobuf/proto"
)

func init() {
	types.Register(protoSerializer, protoDeserializer)
	types.Register(dispatchSerializer, dispatchDeserializer)
	types.Register(clientSerializer, clientDeserializer)
}

func protoSerializer(s *types.Serializer, mp *proto.Message) error {
	m := *mp
	if m == nil {
		types.SerializeT(s, false)
		return nil
	}
	b, err := proto.Marshal(m)
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

	var m proto.Message
	if err := proto.Unmarshal(b, m); err != nil {
		return fmt.Errorf("proto.Unmarshal: %w", err)
	}
	*mp = m

	return nil
}

type serializedDispatch struct {
	opts      []DispatchOption
	functions map[string]Function
}

func dispatchSerializer(s *types.Serializer, d *Dispatch) error {
	types.SerializeT(s, serializedDispatch{d.opts, d.functions})
	return nil
}

func dispatchDeserializer(d *types.Deserializer, c *Dispatch) error {
	var sd serializedDispatch
	types.DeserializeTo(d, &sd)

	dispatch, err := New(sd.opts...)
	if err != nil {
		return err
	}
	for _, fn := range sd.functions {
		dispatch.Register(fn)
	}
	*c = *dispatch //nolint
	return nil
}

func clientSerializer(s *types.Serializer, c *Client) error {
	types.SerializeT(s, c.opts)
	return nil
}

func clientDeserializer(d *types.Deserializer, c *Client) error {
	var opts []ClientOption
	types.DeserializeTo(d, &opts)

	client, err := NewClient(opts...)
	if err != nil {
		return err
	}
	*c = *client
	return nil
}
