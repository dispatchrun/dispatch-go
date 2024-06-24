//go:build !durable

package dispatchclient

import "github.com/dispatchrun/coroutine/types"

func init() {
	types.Register(clientSerializer, clientDeserializer)
}

func clientSerializer(s *types.Serializer, c *Client) error {
	types.SerializeT(s, c.opts)
	return nil
}

func clientDeserializer(d *types.Deserializer, c *Client) error {
	var opts []Option
	types.DeserializeTo(d, &opts)

	client, err := New(opts...)
	if err != nil {
		return err
	}
	*c = *client
	return nil
}
