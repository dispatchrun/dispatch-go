//go:build !durable

package dispatchproto

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Any represents any value.
type Any struct{ proto *anypb.Any }

// Nil creates an Any that contains nil/null.
func Nil() Any {
	return knownAny(&emptypb.Empty{})
}

// Bool creates an Any that contains a boolean value.
func Bool(v bool) Any {
	return knownAny(wrapperspb.Bool(v))
}

// Int creates an Any that contains an integer value.
func Int(v int64) Any {
	// Note: we serialize all integers using wrapperspb.Int64, even
	// though wrapperspb.Int32 is available. A variable-length
	// format is used for the wire representation of the integer, so
	// there's no penalty for using a wider variable-length type.
	// It simplifies the implementation here and elsewhere if there's
	// only one wrapper used.
	return knownAny(wrapperspb.Int64(v))
}

// Uint creates an Any that contains an unsigned integer value.
func Uint(v uint64) Any {
	// See note above about 64-bit wrapper.
	return knownAny(wrapperspb.UInt64(v))
}

// Float creates an Any that contains a floating point value.
func Float(v float64) Any {
	// See notes above. We also exclusively use the Double (float64)
	// wrapper to carry 32-bit and 64-bit floats. Although there
	// is a size penalty in some cases, we're not shipping around
	// so many floats that this is an issue. Prefer simplifying the
	// implementation here and elsewhere by limiting the number of
	// wrappers that are used.
	return knownAny(wrapperspb.Double(v))
}

// String creates an Any that contains a string value.
func String(v string) Any {
	return knownAny(wrapperspb.String(v))
}

// Bytes creates an Any that contains a bytes value.
func Bytes(v []byte) Any {
	return knownAny(wrapperspb.Bytes(v))
}

// Time creates an Any that contains a time value.
func Time(v time.Time) Any {
	return knownAny(timestamppb.New(v))
}

// Duration creates an Any that contains a duration value.
func Duration(v time.Duration) Any {
	return knownAny(durationpb.New(v))
}

// Marshal packages a Go value into an Any, for use as input
// to or output from a Dispatch function.
//
// Primitive values (booleans, integers, floats, strings, bytes, timestamps,
// durations) are supported, along with values that implement either
// proto.Message, json.Marshaler, encoding.TextMarshaler or
// encoding.BinaryMarshaler. Slices and maps are also supported, as long
// as they are JSON-like in shape.
func Marshal(v any) (Any, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer && rv.IsNil() {
		return Nil(), nil
	}
	var m proto.Message
	switch vv := v.(type) {
	case nil:
		m = &emptypb.Empty{}
	case proto.Message:
		m = vv
	case time.Time:
		m = timestamppb.New(vv)
	case time.Duration:
		m = durationpb.New(vv)
	case json.Marshaler:
		// Obviously not ideal going to bytes, then to any, then
		// to structpb.Value! It would be more efficient to use
		// a json.Decoder, and/or to use a third-party JSON library.
		b, err := vv.MarshalJSON()
		if err != nil {
			return Any{}, err
		}
		var v any
		if err := json.Unmarshal(b, &v); err != nil {
			return Any{}, err
		}
		m, err = structpb.NewValue(v)
		if err != nil {
			return Any{}, err
		}

	case encoding.TextMarshaler:
		b, err := vv.MarshalText()
		if err != nil {
			return Any{}, err
		}
		m = wrapperspb.String(string(b))
	case encoding.BinaryMarshaler:
		b, err := vv.MarshalBinary()
		if err != nil {
			return Any{}, err
		}
		m = wrapperspb.Bytes(b)
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
		var err error
		if m, err = newStructpbValue(rv); err != nil {
			return Any{}, fmt.Errorf("cannot serialize %v: %w", v, err)
		}
	}

	proto, err := anypb.New(m)
	if err != nil {
		return Any{}, err
	}
	return Any{proto}, nil
}

func knownAny(v any) Any {
	any, err := Marshal(v)
	if err != nil {
		panic(err)
	}
	return any
}

var (
	timeType     = reflect.TypeFor[time.Time]()
	durationType = reflect.TypeFor[time.Duration]()

	jsonUnmarshalerType   = reflect.TypeFor[json.Unmarshaler]()
	textUnmarshalerType   = reflect.TypeFor[encoding.TextUnmarshaler]()
	binaryUnmarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()
)

// Unmarshal unmarshals the value.
func (a Any) Unmarshal(v any) error {
	if a.proto == nil {
		return fmt.Errorf("empty Any")
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		panic("Any.Unmarshal expects a pointer to a non-nil object")
	}
	elem := rv.Elem()

	m, err := a.proto.UnmarshalNew()
	if err != nil {
		return err
	}

	// Check for an exact match on type (v is a proto.Message).
	rm := reflect.ValueOf(m)
	if elem.Type() == rm.Type() {
		elem.Set(rm)
		return nil
	}

	// Check for:
	// - structpb.Value => json.Unmarshaler
	// - wrapperspb.StringValue => encoding.TextUnmarshaler
	// - wrapperspb.BytesValue => encoding.BinaryUnmarshaler
	switch mm := m.(type) {
	case *structpb.Value:
		var target reflect.Value
		if elem.Type().Implements(jsonUnmarshalerType) {
			if elem.Kind() == reflect.Pointer && elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			target = elem
		} else if rv.Type().Implements(jsonUnmarshalerType) {
			target = rv
		}
		if target != (reflect.Value{}) {
			unmarshalJSON := target.MethodByName("UnmarshalJSON")
			b, err := mm.MarshalJSON()
			if err != nil {
				return err
			}
			res := unmarshalJSON.Call([]reflect.Value{reflect.ValueOf(b)})
			if err := res[0].Interface(); err != nil {
				return err.(error)
			}
			return nil
		}

	case *wrapperspb.StringValue:
		var target reflect.Value
		if elem.Type().Implements(textUnmarshalerType) {
			if elem.Kind() == reflect.Pointer && elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			target = elem
		} else if rv.Type().Implements(textUnmarshalerType) {
			target = rv
		}
		if target != (reflect.Value{}) {
			unmarshalText := target.MethodByName("UnmarshalText")
			b := []byte(mm.Value)
			res := unmarshalText.Call([]reflect.Value{reflect.ValueOf(b)})
			if err := res[0].Interface(); err != nil {
				return err.(error)
			}
			return nil
		}

	case *wrapperspb.BytesValue:
		var target reflect.Value
		if elem.Type().Implements(binaryUnmarshalerType) {
			if elem.Kind() == reflect.Pointer && elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			target = elem
		} else if rv.Type().Implements(binaryUnmarshalerType) {
			target = rv
		}
		if target != (reflect.Value{}) {
			unmarshalBinary := target.MethodByName("UnmarshalBinary")
			res := unmarshalBinary.Call([]reflect.Value{reflect.ValueOf(mm.Value)})
			if err := res[0].Interface(); err != nil {
				return err.(error)
			}
			return nil
		}
	}

	switch elem.Type() {
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
		var i int64
		if v, ok := m.(*wrapperspb.Int64Value); ok {
			i = v.Value
		} else if v, ok := m.(*wrapperspb.Int32Value); ok {
			i = int64(v.Value)
		} else {
			return fmt.Errorf("cannot unmarshal %T into %T", m, elem.Interface())
		}
		if elem.OverflowInt(i) {
			return fmt.Errorf("cannot unmarshal %T of %v into %T", m, i, elem.Interface())
		}
		elem.SetInt(i)
		return nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var u uint64
		if v, ok := m.(*wrapperspb.UInt64Value); ok {
			u = v.Value
		} else if v, ok := m.(*wrapperspb.UInt32Value); ok {
			u = uint64(v.Value)
		} else {
			return fmt.Errorf("cannot unmarshal %T into %T", m, elem.Interface())
		}
		if elem.OverflowUint(u) {
			return fmt.Errorf("cannot unmarshal %T of %v into %T", m, u, elem.Interface())
		}
		elem.SetUint(u)
		return nil

	case reflect.Float32, reflect.Float64:
		var f float64
		if v, ok := m.(*wrapperspb.DoubleValue); ok {
			f = v.Value
		} else if v, ok := m.(*wrapperspb.FloatValue); ok {
			f = float64(v.Value)
		} else {
			return fmt.Errorf("cannot unmarshal %T into %T", m, elem.Interface())
		}
		if elem.OverflowFloat(f) {
			return fmt.Errorf("cannot unmarshal %T of %v into %T", m, f, elem.Interface())
		}
		elem.SetFloat(f)
		return nil

	case reflect.String:
		v, ok := m.(*wrapperspb.StringValue)
		if !ok {
			return fmt.Errorf("cannot unmarshal %T into string", m)
		}
		elem.SetString(v.Value)
		return nil

	case reflect.Interface:
		if elem.NumMethod() == 0 {
			if _, ok := m.(*emptypb.Empty); ok {
				elem.SetZero()
				return nil
			}
		}

	case reflect.Pointer:
		if _, ok := m.(*emptypb.Empty); ok {
			elem.Set(reflect.New(elem.Type()).Elem())
			return nil
		}

	case reflect.Slice:
		if elem.Type().Elem().Kind() == reflect.Uint8 {
			if v, ok := m.(*wrapperspb.BytesValue); ok {
				elem.SetBytes(v.Value)
				return nil
			}
		}
	}

	if s, ok := m.(*structpb.Value); ok {
		return fromStructpbValue(elem, s)
	}

	return fmt.Errorf("cannot deserialize %T into %v (%v kind)", m, elem.Type(), elem.Kind())
}

// TypeURL is a URL that uniquely identifies the type of the
// serialized value.
func (a Any) TypeURL() string {
	return a.proto.GetTypeUrl()
}

// String is the string representation of the any value.
func (a Any) String() string {
	return fmt.Sprintf("Any(%s)", a.proto)
}

// Equal is true if this Any is equal to another.
func (a Any) Equal(other Any) bool {
	return proto.Equal(a.proto, other.proto)
}

func newStructpbValue(rv reflect.Value) (*structpb.Value, error) {
	switch rv.Kind() {
	case reflect.Bool:
		return structpb.NewBoolValue(rv.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := rv.Int()
		f := float64(n)
		if int64(f) != n {
			return nil, fmt.Errorf("cannot serialize %d as number structpb.Value (%v) without losing information", n, f)
		}
		return structpb.NewNumberValue(f), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n := rv.Uint()
		f := float64(n)
		if uint64(f) != n {
			return nil, fmt.Errorf("cannot serialize %d as number structpb.Value (%v) without losing information", n, f)
		}
		return structpb.NewNumberValue(f), nil
	case reflect.Float32, reflect.Float64:
		return structpb.NewNumberValue(rv.Float()), nil
	case reflect.String:
		return structpb.NewStringValue(rv.String()), nil
	case reflect.Interface:
		if rv.NumMethod() == 0 { // interface{} aka. any
			v := rv.Interface()
			if v == nil {
				return structpb.NewNullValue(), nil
			}
			return newStructpbValue(reflect.ValueOf(v))
		}
	case reflect.Slice:
		list := &structpb.ListValue{Values: make([]*structpb.Value, rv.Len())}
		for i := range list.Values {
			elem := rv.Index(i)
			var err error
			list.Values[i], err = newStructpbValue(elem)
			if err != nil {
				return nil, err
			}
		}
		return structpb.NewListValue(list), nil
	case reflect.Map:
		strct := &structpb.Struct{Fields: make(map[string]*structpb.Value, rv.Len())}
		iter := rv.MapRange()
		for iter.Next() {
			k := iter.Key()

			var strKey string
			var hasStrKey bool
			switch k.Kind() {
			case reflect.String:
				strKey = k.String()
				hasStrKey = true
			case reflect.Interface:
				if s, ok := k.Interface().(string); ok {
					strKey = s
					hasStrKey = true
				}
			}
			if !hasStrKey {
				return nil, fmt.Errorf("cannot serialize map with %s (%s) key", k.Type(), k.Kind())
			}

			v, err := newStructpbValue(iter.Value())
			if err != nil {
				return nil, err
			}
			strct.Fields[strKey] = v
		}
		return structpb.NewStructValue(strct), nil
	}
	return nil, fmt.Errorf("not implemented: %s", rv.Type())
}

func fromStructpbValue(rv reflect.Value, s *structpb.Value) error {
	switch rv.Kind() {
	case reflect.Bool:
		if b, ok := s.Kind.(*structpb.Value_BoolValue); ok {
			rv.SetBool(b.BoolValue)
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n, ok := s.Kind.(*structpb.Value_NumberValue); ok {
			rv.SetInt(int64(n.NumberValue))
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n, ok := s.Kind.(*structpb.Value_NumberValue); ok {
			rv.SetUint(uint64(n.NumberValue))
			return nil
		}
	case reflect.Float32, reflect.Float64:
		if n, ok := s.Kind.(*structpb.Value_NumberValue); ok {
			rv.SetFloat(n.NumberValue)
			return nil
		}
	case reflect.String:
		if str, ok := s.Kind.(*structpb.Value_StringValue); ok {
			rv.SetString(str.StringValue)
			return nil
		}
	case reflect.Slice:
		if l, ok := s.Kind.(*structpb.Value_ListValue); ok {
			values := l.ListValue.GetValues()
			rv.Grow(len(values))
			rv.SetLen(len(values))
			for i, value := range values {
				if err := fromStructpbValue(rv.Index(i), value); err != nil {
					return err
				}
			}
			return nil
		}
	case reflect.Map:
		if strct, ok := s.Kind.(*structpb.Value_StructValue); ok {
			fields := strct.StructValue.Fields
			rv.Set(reflect.MakeMapWithSize(rv.Type(), len(fields)))
			valueType := rv.Type().Elem()
			for key, value := range fields {
				mv := reflect.New(valueType).Elem()
				if err := fromStructpbValue(mv, value); err != nil {
					return err
				}
				rv.SetMapIndex(reflect.ValueOf(key), mv)
			}
			return nil
		}
	case reflect.Interface:
		if rv.NumMethod() == 0 { // interface{} aka. any
			v := s.AsInterface()
			if v == nil {
				rv.SetZero()
			} else {
				rv.Set(reflect.ValueOf(s.AsInterface()))
			}
			return nil
		}
	}
	return fmt.Errorf("cannot deserialize %T into %v (%v kind)", s, rv.Type(), rv.Kind())
}
