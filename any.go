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

	case bool:
		m = wrapperspb.Bool(vv)

	case int:
		m = wrapperspb.Int64(int64(vv))
	case int8:
		m = wrapperspb.Int64(int64(vv))
	case int16:
		m = wrapperspb.Int64(int64(vv))
	case int32:
		m = wrapperspb.Int64(int64(vv))
	case int64:
		m = wrapperspb.Int64(vv)

	case uint:
		m = wrapperspb.UInt64(uint64(vv))
	case uint8:
		m = wrapperspb.UInt64(uint64(vv))
	case uint16:
		m = wrapperspb.UInt64(uint64(vv))
	case uint32:
		m = wrapperspb.UInt64(uint64(vv))
	case uint64:
		m = wrapperspb.UInt64(uint64(vv))

	case float32:
		m = wrapperspb.Double(float64(vv))
	case float64:
		m = wrapperspb.Double(vv)

	case string:
		m = wrapperspb.String(vv)

	case []byte:
		m = wrapperspb.Bytes(vv)

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

// Bool creates an Any that contains a boolean value.
func Bool(v bool) Any {
	return mustNewAny(wrapperspb.Bool(v))
}

// Int creates an Any that contains an integer value.
func Int(v int) Any {
	return mustNewAny(wrapperspb.Int64(int64(v)))
}

// Uint creates an Any that contains an unsigned integer value.
func Uint(v uint) Any {
	return mustNewAny(wrapperspb.UInt64(uint64(v)))
}

// Float creates an Any that contains a floating point value.
func Float(v float64) Any {
	return mustNewAny(wrapperspb.Double(v))
}

// String creates an Any that contains a string value.
func String(v string) Any {
	return mustNewAny(wrapperspb.String(v))
}

// Bytes creates an Any that contains a bytes value.
func Bytes(v []byte) Any {
	return mustNewAny(wrapperspb.Bytes(v))
}

func mustNewAny(v any) Any {
	any, err := NewAny(v)
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
	case reflect.Bool:
		v, ok := m.(*wrapperspb.BoolValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into bool", m)
		}
		elem.SetBool(v.Value)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, ok := m.(*wrapperspb.Int64Value)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into int", m)
		}
		elem.SetInt(v.Value)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, ok := m.(*wrapperspb.UInt64Value)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into uint", m)
		}
		elem.SetUint(v.Value)

	case reflect.Float32, reflect.Float64:
		v, ok := m.(*wrapperspb.DoubleValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into float", m)
		}
		elem.SetFloat(v.Value)

	case reflect.String:
		v, ok := m.(*wrapperspb.StringValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into string", m)
		}
		elem.SetString(v.Value)

	default:
		// Special case for []byte. Other reflect.Slice values aren't supported at this time.
		if elem.Kind() == reflect.Slice && elem.Type().Elem().Kind() == reflect.Uint8 {
			v, ok := m.(*wrapperspb.BytesValue)
			if !ok {
				return fmt.Errorf("cannot unmarshal %T into []byte", m)
			}
			elem.SetBytes(v.Value)
			return nil
		}

		// TODO: support more types
		return fmt.Errorf("unsupported type: %v (%v kind)", elem.Type(), elem.Kind())
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
