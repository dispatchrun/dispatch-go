package dispatch

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Any represents any value.
type Any struct {
	proto *anypb.Any
}

// NewAny creates an Any from a proto.Message.
func NewAny(v any) (Any, error) {
	var m proto.Message
	switch vv := v.(type) {
	case proto.Message:
		m = vv
	case int:
		m = wrapperspb.Int64(int64(vv))
	case string:
		m = wrapperspb.String(vv)
	default:
		// TODO: support more types
		return Any{}, fmt.Errorf("unsupported type: %T", v)
	}
	proto, err := anypb.New(m)
	if err != nil {
		return Any{}, err
	}
	return Any{proto}, nil
}

// Int creates an Any that contains an integer value.
func Int(v int) Any {
	any, err := NewAny(wrapperspb.Int64(int64(v)))
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

// Unmarshal unmarshals the value.
func (a Any) Unmarshal(v any) error {
	if a.proto == nil {
		return fmt.Errorf("empty Any")
	}

	r := reflect.ValueOf(v)
	if r.Kind() != reflect.Pointer || r.IsNil() {
		panic("Any.Unmarshal expects a pointer")
	}
	elem := r.Elem()

	m, err := a.proto.UnmarshalNew()
	if err != nil {
		return err
	}

	rm := reflect.ValueOf(m)
	if rm.Type() == elem.Type() {
		elem.Set(rm)
		return nil
	}

	switch elem.Kind() {
	case reflect.Int:
		v, ok := m.(*wrapperspb.Int64Value)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into int", m)
		}
		elem.SetInt(v.Value)
	case reflect.String:
		v, ok := m.(*wrapperspb.StringValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into string", m)
		}
		elem.SetString(v.Value)
	default:
		// TODO: support more types
		return fmt.Errorf("unsupported type: %T", elem.Interface())
	}
	return nil
}

// TypeURL is a URL that uniquely identifies the type of the
// serialized value.
func (a Any) TypeURL() string {
	return a.proto.GetTypeUrl()
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
