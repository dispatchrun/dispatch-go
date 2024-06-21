//go:build !durable

package dispatch

import "github.com/dispatchrun/coroutine/types"

func init() {
	types.Register(dispatchSerializer, dispatchDeserializer)
}

type serializedDispatch struct {
	opts      []Option
	functions *FunctionRegistry
}

func dispatchSerializer(s *types.Serializer, d *Dispatch) error {
	types.SerializeT(s, serializedDispatch{d.opts, &d.functions})
	return nil
}

func dispatchDeserializer(d *types.Deserializer, c *Dispatch) error {
	var sd serializedDispatch
	types.DeserializeTo(d, &sd)

	dispatch, err := New(sd.opts...)
	if err != nil {
		return err
	}
	dispatch.functions = *sd.functions // nolint
	*c = *dispatch                     //nolint
	return nil
}
