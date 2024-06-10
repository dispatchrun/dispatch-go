package dispatch

import (
	"fmt"
	"reflect"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	case time.Time:
		m = timestamppb.New(vv)
	case time.Duration:
		m = durationpb.New(vv)

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
func Int(v int64) Any {
	// Note: we serialize all integers using wrapperspb.Int64, even
	// though wrapperspb.Int32 is available. A variable-length
	// format is used for the wire representation of the integer, so
	// there's no penalty for using a wider variable-length type.
	// It simplifies the implementation here and elsewhere if there's
	// only one wrapper used.
	return mustNewAny(wrapperspb.Int64(v))
}

// Uint creates an Any that contains an unsigned integer value.
func Uint(v uint64) Any {
	// See note above about 64-bit wrapper.
	return mustNewAny(wrapperspb.UInt64(v))
}

// Float creates an Any that contains a floating point value.
func Float(v float64) Any {
	// See notes above. We also exclusively use the Double (float64)
	// wrapper to carry 32-bit and 64-bit floats. Although there
	// is a size penalty in some cases, we're not shipping around
	// so many floats that this is an issue. Prefer simplifying the
	// implementation here and elsewhere by limiting the number of
	// wrappers that are used.
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

// Time creates an Any that contains a time value.
func Time(v time.Time) Any {
	return mustNewAny(timestamppb.New(v))
}

// Duration creates an Any that contains a duration value.
func Duration(v time.Duration) Any {
	return mustNewAny(durationpb.New(v))
}

func mustNewAny(v any) Any {
	any, err := NewAny(v)
	if err != nil {
		panic(err)
	}
	return any
}

var (
	timeType     = reflect.TypeFor[time.Time]()
	durationType = reflect.TypeFor[time.Duration]()
)

// Unmarshal unmarshals the value.
func (a Any) Unmarshal(v any) error {
	if a.proto == nil {
		return fmt.Errorf("empty Any")
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		panic("Any.Unmarshal expects a pointer")
	}
	elem := rv.Elem()

	m, err := a.proto.UnmarshalNew()
	if err != nil {
		return err
	}
	rm := reflect.ValueOf(m)

	switch elem.Type() {
	case rm.Type(): // e.g. a proto.Message impl
		elem.Set(rm)
		return nil

	case timeType:
		v, ok := m.(*timestamppb.Timestamp)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into time.Time", m)
		} else if err := v.CheckValid(); err != nil {
			return fmt.Errorf("cannot unmarshal %T into time.Time: %w", m, err)
		}
		elem.Set(reflect.ValueOf(v.AsTime()))
		return nil

	case durationType:
		v, ok := m.(*durationpb.Duration)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into time.Duration", m)
		} else if err := v.CheckValid(); err != nil {
			return fmt.Errorf("cannot unmarshal %T into time.Duration: %w", m, err)
		}
		elem.SetInt(int64(v.AsDuration()))
		return nil
	}

	switch elem.Kind() {
	case reflect.Bool:
		v, ok := m.(*wrapperspb.BoolValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into bool", m)
		}
		elem.SetBool(v.Value)
		return nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, ok := m.(*wrapperspb.Int64Value)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into %T", m, elem.Interface())
		} else if elem.OverflowInt(v.Value) {
			return fmt.Errorf("cannot unmarshal %T of %v into %T", m, v.Value, elem.Interface())
		}
		elem.SetInt(v.Value)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, ok := m.(*wrapperspb.UInt64Value)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into %T", m, elem.Interface())
		} else if elem.OverflowUint(v.Value) {
			return fmt.Errorf("cannot unmarshal %T of %v into %T", m, v.Value, elem.Interface())
		}
		elem.SetUint(v.Value)
		return nil

	case reflect.Float32, reflect.Float64:
		v, ok := m.(*wrapperspb.DoubleValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into %T", m, elem.Interface())
		} else if elem.OverflowFloat(v.Value) {
			return fmt.Errorf("cannot unmarshal %T of %v into %T", m, v.Value, elem.Interface())
		}
		elem.SetFloat(v.Value)
		return nil

	case reflect.String:
		v, ok := m.(*wrapperspb.StringValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into string", m)
		}
		elem.SetString(v.Value)
		return nil

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
