//go:build !durable

package dispatch

import (
	"github.com/dispatchrun/coroutine/types"
	"github.com/dispatchrun/dispatch-go/dispatchproto"
)

func init() {
	types.Register(dispatchSerializer, dispatchDeserializer)
}

type serializedDispatch struct {
	opts      []Option
	functions dispatchproto.FunctionMap
}

func dispatchSerializer(s *types.Serializer, d *Dispatch) error {
	opts := make([]Option, 0, len(d.opts))
	for _, opt := range d.opts {
		if _, ok := opt.(AnyFunction); ok {
			// No need to serialize these options, since we serialize the
			// map of registered functions directly.
			continue
		}
		opts = append(opts, opt)
	}
	types.SerializeT(s, serializedDispatch{opts, d.functions})
	return nil
}

func dispatchDeserializer(d *types.Deserializer, c *Dispatch) error {
	var sd serializedDispatch
	types.DeserializeTo(d, &sd)

	dispatch, err := New(sd.opts...)
	if err != nil {
		return err
	}
	dispatch.functions = sd.functions
	*c = *dispatch //nolint
	return nil
}
