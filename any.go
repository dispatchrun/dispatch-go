package dispatch

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Any represents any value.
type Any struct {
	proto *anypb.Any
}

// NewAny creates an Any from a proto.Message.
func NewAny(message proto.Message) (Any, error) {
	proto, err := anypb.New(message)
	if err != nil {
		return Any{}, err
	}
	return Any{proto}, nil
}

// Int creates an Any that contains an integer value.
func Int(v int64) Any {
	any, err := NewAny(wrapperspb.Int64(v))
	if err != nil {
		panic(err)
	}
	return any
}

// String creates an Any that contains a string value.
func String(v string) Any {
	any, err := NewAny(wrapperspb.String(v))
	if err != nil {
		panic(err)
	}
	return any
}

// TypeURL is a URL that uniquely identifies the type of the
// serialized value.
func (a Any) TypeURL() string {
	return a.proto.GetTypeUrl()
}

// Proto returns the underlying proto.Message contained
// within the Any container.
func (a Any) Proto() (proto.Message, error) {
	if a.proto == nil {
		return nil, fmt.Errorf("empty Any")
	}
	return a.proto.UnmarshalNew()
}

// Int extracts the integer value, if applicable.
func (a Any) Int() (int64, error) {
	m, err := a.Proto()
	if err != nil {
		return 0, err
	}
	v, ok := m.(*wrapperspb.Int64Value)
	if !ok {
		return 0, fmt.Errorf("Any contains %T, not an integer value", m)
	}
	return v.Value, nil
}

// String extracts the string value, if applicable.
func (a Any) String() (string, error) {
	m, err := a.Proto()
	if err != nil {
		return "", err
	}
	v, ok := m.(*wrapperspb.StringValue)
	if !ok {
		return "", fmt.Errorf("Any contains %T, not a string value", m)
	}
	return v.Value, nil
}

func (a Any) Format(f fmt.State, verb rune) {
	// Implement fmt.Formatter rather than fmt.Stringer
	// so that we can use String() to extract the string value.
	_, _ = f.Write([]byte(fmt.Sprintf("Any(%s)", a.proto)))
}

// Equal is true if this Any is equal to another.
func (a Any) Equal(other Any) bool {
	return proto.Equal(a.proto, other.proto)
}
