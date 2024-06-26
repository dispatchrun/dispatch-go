package dispatchproto_test

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dispatchrun/dispatch-go/dispatchproto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestAnyNil(t *testing.T) {
	boxed := dispatchproto.Nil()

	// Check nil any can be deserialized.
	var got any
	if err := boxed.Unmarshal(&got); err != nil {
		t.Fatal(err)
	} else if got != nil {
		t.Errorf("unexpected nil: got %v, want %v", got, nil)
	}

	// Check null pointers can be deserialized.
	now := time.Now()
	tp := &now // set to something, then check it gets cleared
	if err := boxed.Unmarshal(&tp); err != nil {
		t.Fatal(err)
	} else if tp != nil {
		t.Errorf("unexpected nil: got %v, want %v", tp, nil)
	}
}

func TestAnyBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		boxed := dispatchproto.Bool(v)
		var got bool
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected bool: got %v, want %v", got, v)
		}
	}
}

func TestAnyInt(t *testing.T) {
	for _, v := range []int64{0, 11, -1, 2, math.MinInt, math.MaxInt} {
		boxed := dispatchproto.Int(v)
		var got int64
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected int: got %v, want %v", got, v)
		}
	}
}

func TestAnyUint(t *testing.T) {
	for _, v := range []uint64{0, 11, 2, math.MaxUint} {
		boxed := dispatchproto.Uint(v)
		var got uint64
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected uint: got %v, want %v", got, v)
		}
	}
}

func TestAnyFloat(t *testing.T) {
	for _, v := range []float64{0, 3.14, 11.11, math.MaxFloat64} {
		boxed := dispatchproto.Float(v)
		var got float64
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected float: got %v, want %v", got, v)
		}
	}
}

func TestAnyString(t *testing.T) {
	for _, v := range []string{"", "x", "foobar", strings.Repeat("abc", 100)} {
		boxed := dispatchproto.String(v)
		var got string
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected string: got %v, want %v", got, v)
		}
	}
}

func TestAnyBytes(t *testing.T) {
	for _, v := range [][]byte{nil, []byte("foobar"), bytes.Repeat([]byte("abc"), 100)} {
		boxed := dispatchproto.Bytes(v)
		var got []byte
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(v, got) {
			t.Errorf("unexpected bytes: got %v, want %v", got, v)
		}
	}
}

func TestAnyTime(t *testing.T) {
	for _, v := range []time.Time{time.Now(), { /*zero*/ }, time.Date(2024, time.June, 10, 11, 30, 1, 2, time.UTC)} {
		boxed := dispatchproto.Time(v)
		var got time.Time
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if !got.Equal(v) {
			t.Errorf("unexpected time: got %v, want %v", got, v)
		}
	}
}

func TestAnyDuration(t *testing.T) {
	for _, v := range []time.Duration{0, time.Second, 10 * time.Hour} {
		boxed := dispatchproto.Duration(v)
		var got time.Duration
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected duration: got %v, want %v", got, v)
		}
	}
}

func TestAnyTextMarshaler(t *testing.T) {
	v := &textMarshaler{Value: "foobar"}
	boxed, err := dispatchproto.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	var v2 *textMarshaler // (pointer)
	if err := boxed.Unmarshal(&v2); err != nil {
		t.Fatal(err)
	} else if v2.Value != v.Value {
		t.Errorf("unexpected serialized value: %v", v2.Value)
	}

	var v3 textMarshaler // (not a pointer)
	if err := boxed.Unmarshal(&v3); err != nil {
		t.Fatal(err)
	} else if v3.Value != v.Value {
		t.Errorf("unexpected serialized value: %v", v3.Value)
	}

	// Check a string is sent on the wire.
	var v4 string
	if err := boxed.Unmarshal(&v4); err != nil {
		t.Fatal(err)
	} else if v4 != v.Value {
		t.Errorf("unexpected serialized value: %v", v4)
	}
}

func TestAnyBinaryMarshaler(t *testing.T) {
	v := &binaryMarshaler{Value: []byte("foobar")}
	boxed, err := dispatchproto.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	var v2 *binaryMarshaler // (pointer)
	if err := boxed.Unmarshal(&v2); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(v2.Value, v.Value) {
		t.Errorf("unexpected serialized value: %v", v2.Value)
	}

	var v3 binaryMarshaler // (not a pointer)
	if err := boxed.Unmarshal(&v3); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(v3.Value, v.Value) {
		t.Errorf("unexpected serialized value: %v", v3.Value)
	}

	// Check bytes are sent on the wire.
	var v4 []byte
	if err := boxed.Unmarshal(&v4); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(v4, v.Value) {
		t.Errorf("unexpected serialized value: %v", v4)
	}
}

func TestAnyJsonMarshaler(t *testing.T) {
	v := &jsonMarshaler{Value: jsonValue{
		Bool:   true,
		Int:    11,
		Float:  3.14,
		String: "foo",
		List:   []any{nil, false, []any{"foo", "bar"}, map[string]any{"abc": "xyz"}},
		Object: map[string]any{"n": 3.14, "flag": true, "tags": []any{"x", "y", "z"}},
	}}
	boxed, err := dispatchproto.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	var v2 *jsonMarshaler // (pointer)
	if err := boxed.Unmarshal(&v2); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(v2.Value, v.Value); diff != "" {
		t.Errorf("unexpected serialized value: %v", diff)
	}

	var v3 *jsonMarshaler // (not a pointer)
	if err := boxed.Unmarshal(&v3); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(v3.Value, v.Value); diff != "" {
		t.Errorf("unexpected serialized value: %v", diff)
	}
}

func TestOverflow(t *testing.T) {
	var i8 int8
	if err := dispatchproto.Int(math.MinInt8 - 1).Unmarshal(&i8); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of -129 into int8" {
		t.Errorf("unexpected error: %v", err)
	}
	if err := dispatchproto.Int(math.MaxInt8 + 1).Unmarshal(&i8); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of 128 into int8" {
		t.Errorf("unexpected error: %v", err)
	}

	var i16 int16
	if err := dispatchproto.Int(math.MinInt16 - 1).Unmarshal(&i16); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of -32769 into int16" {
		t.Errorf("unexpected error: %v", err)
	}
	if err := dispatchproto.Int(math.MaxInt16 + 1).Unmarshal(&i16); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of 32768 into int16" {
		t.Errorf("unexpected error: %v", err)
	}

	var i32 int32
	if err := dispatchproto.Int(math.MinInt32 - 1).Unmarshal(&i32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of -2147483649 into int32" {
		t.Errorf("unexpected error: %v", err)
	}
	if err := dispatchproto.Int(math.MaxInt32 + 1).Unmarshal(&i32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of 2147483648 into int32" {
		t.Errorf("unexpected error: %v", err)
	}

	var u8 uint8
	if err := dispatchproto.Uint(math.MaxUint8 + 1).Unmarshal(&u8); err == nil || err.Error() != "cannot unmarshal *wrapperspb.UInt64Value of 256 into uint8" {
		t.Errorf("unexpected error: %v", err)
	}
	var u16 uint16
	if err := dispatchproto.Uint(math.MaxUint16 + 1).Unmarshal(&u16); err == nil || err.Error() != "cannot unmarshal *wrapperspb.UInt64Value of 65536 into uint16" {
		t.Errorf("unexpected error: %v", err)
	}
	var u32 uint32
	if err := dispatchproto.Uint(math.MaxUint32 + 1).Unmarshal(&u32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.UInt64Value of 4294967296 into uint32" {
		t.Errorf("unexpected error: %v", err)
	}

	var f32 float32
	if err := dispatchproto.Float(math.MaxFloat32 + math.MaxFloat32).Unmarshal(&f32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.DoubleValue of 6.805646932770577e+38 into float32" {
		t.Errorf("unexpected error: %v", err)
	}

	badTime, err := dispatchproto.Marshal(&timestamppb.Timestamp{Seconds: math.MinInt64})
	if err != nil {
		t.Fatal(err)
	}
	var tt time.Time
	if err := badTime.Unmarshal(&tt); err == nil {
		t.Error("expected an error")
	}

	badDuration, err := dispatchproto.Marshal(&durationpb.Duration{Seconds: math.MaxInt64})
	if err != nil {
		t.Fatal(err)
	}
	var td time.Duration
	if err := badDuration.Unmarshal(&td); err == nil {
		t.Error("expected an error")
	}
}

func TestAny(t *testing.T) {
	for _, v := range []any{
		nil,
		(*time.Time)(nil),

		true,
		false,

		11,
		int8(-1),
		int16(math.MaxInt16),
		int32(23),
		int64(math.MinInt64),

		uint(1),
		uint8(128),
		uint16(math.MaxUint16),
		uint32(0xDEADBEEF),
		uint64(math.MaxUint64),

		float32(3.14),
		float64(11.11),

		"",
		"foo",

		[]byte("bar"),

		time.Now().UTC(),

		11 * time.Second,

		// Raw proto.Message
		&emptypb.Empty{},
		&wrapperspb.Int32Value{Value: 11},

		// encoding.{Text,Binary}Marshaler
		&textMarshaler{Value: "foobar"},
		&binaryMarshaler{Value: []byte("foobar")},

		// json.Marshaler
		&jsonMarshaler{Value: jsonValue{
			Bool:   true,
			Int:    11,
			Float:  3.14,
			String: "foo",
			List:   []any{nil, false, []any{"foo", "bar"}, map[string]any{"abc": "xyz"}},
			Object: map[string]any{"n": 3.14, "flag": true, "tags": []any{"x", "y", "z"}},
		}},
	} {
		t.Run(fmt.Sprintf("%v", v), func(t *testing.T) {
			boxed, err := dispatchproto.Marshal(v)
			if err != nil {
				t.Fatalf("Marshal(%v): %v", v, err)
			}

			var rt reflect.Type
			if v == nil {
				rt = reflect.ValueOf(&v).Elem().Type()
			} else {
				rt = reflect.ValueOf(v).Type()
			}
			rv := reflect.New(rt)
			if err := boxed.Unmarshal(rv.Interface()); err != nil {
				t.Fatal(err)
			}

			got := rv.Elem().Interface()

			var want any
			if v != nil {
				want = reflect.ValueOf(v).Interface()
			}

			if wantProto, ok := want.(proto.Message); ok {
				if equal := proto.Equal(got.(proto.Message), wantProto); !equal {
					t.Errorf("unexpected Marshal(%v).Unmarshal result: %#v", v, got)
				}
			} else if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("unexpected Marshal(%v).Unmarshal result: %v", v, diff)
			}
		})
	}
}

type textMarshaler struct{ Value string }

func (t *textMarshaler) MarshalText() ([]byte, error) {
	return []byte(t.Value), nil
}

func (t *textMarshaler) UnmarshalText(b []byte) error {
	t.Value = string(b)
	return nil
}

var _ encoding.TextMarshaler = (*textMarshaler)(nil)
var _ encoding.TextUnmarshaler = (*textMarshaler)(nil)

type binaryMarshaler struct{ Value []byte }

func (t *binaryMarshaler) MarshalBinary() ([]byte, error) {
	return t.Value, nil
}

func (t *binaryMarshaler) UnmarshalBinary(b []byte) error {
	t.Value = b
	return nil
}

var _ encoding.BinaryMarshaler = (*binaryMarshaler)(nil)
var _ encoding.BinaryUnmarshaler = (*binaryMarshaler)(nil)

type jsonMarshaler struct{ Value jsonValue }

type jsonValue struct {
	Null   *any           `json:"null"`
	Bool   bool           `json:"bool"`
	String string         `json:"string"`
	Int    int64          `json:"int"`
	Float  float64        `json:"float"`
	List   []any          `json:"list"`
	Object map[string]any `json:"object"`
}

func (j *jsonMarshaler) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.Value)
}

func (j *jsonMarshaler) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &j.Value)
}

var _ json.Marshaler = (*jsonMarshaler)(nil)
var _ json.Unmarshaler = (*jsonMarshaler)(nil)
