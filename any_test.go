package dispatch_test

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/dispatchrun/dispatch-go"
)

func TestAnyBool(t *testing.T) {
	for _, v := range []bool{true, false} {
		boxed := dispatch.Bool(v)
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
		boxed := dispatch.Int(v)
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
		boxed := dispatch.Uint(v)
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
		boxed := dispatch.Float(v)
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
		boxed := dispatch.String(v)
		var got string
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected string: got %v, want %v", got, v)
		}
	}
}

func TestAnyBytes(t *testing.T) {
	for _, v := range [][]byte{nil, []byte{}, []byte("foobar"), bytes.Repeat([]byte("abc"), 100)} {
		boxed := dispatch.Bytes(v)
		var got []byte
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(v, got) {
			t.Errorf("unexpected bytes: got %v, want %v", got, v)
		}
	}
}

func TestOverflow(t *testing.T) {
	var i8 int8
	if err := dispatch.Int(math.MinInt8 - 1).Unmarshal(&i8); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of -129 into int8" {
		t.Errorf("unexpected error: %v", err)
	}
	if err := dispatch.Int(math.MaxInt8 + 1).Unmarshal(&i8); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of 128 into int8" {
		t.Errorf("unexpected error: %v", err)
	}

	var i16 int16
	if err := dispatch.Int(math.MinInt16 - 1).Unmarshal(&i16); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of -32769 into int16" {
		t.Errorf("unexpected error: %v", err)
	}
	if err := dispatch.Int(math.MaxInt16 + 1).Unmarshal(&i16); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of 32768 into int16" {
		t.Errorf("unexpected error: %v", err)
	}

	var i32 int32
	if err := dispatch.Int(math.MinInt32 - 1).Unmarshal(&i32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of -2147483649 into int32" {
		t.Errorf("unexpected error: %v", err)
	}
	if err := dispatch.Int(math.MaxInt32 + 1).Unmarshal(&i32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.Int64Value of 2147483648 into int32" {
		t.Errorf("unexpected error: %v", err)
	}

	var u8 uint8
	if err := dispatch.Uint(math.MaxUint8 + 1).Unmarshal(&u8); err == nil || err.Error() != "cannot unmarshal *wrapperspb.UInt64Value of 256 into uint8" {
		t.Errorf("unexpected error: %v", err)
	}
	var u16 uint16
	if err := dispatch.Uint(math.MaxUint16 + 1).Unmarshal(&u16); err == nil || err.Error() != "cannot unmarshal *wrapperspb.UInt64Value of 65536 into uint16" {
		t.Errorf("unexpected error: %v", err)
	}
	var u32 uint32
	if err := dispatch.Uint(math.MaxUint32 + 1).Unmarshal(&u32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.UInt64Value of 4294967296 into uint32" {
		t.Errorf("unexpected error: %v", err)
	}

	var f32 float32
	if err := dispatch.Float(math.MaxFloat32 + math.MaxFloat32).Unmarshal(&f32); err == nil || err.Error() != "cannot unmarshal *wrapperspb.DoubleValue of 6.805646932770577e+38 into float32" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAny(t *testing.T) {
	for _, v := range []any{
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
	} {
		t.Run(fmt.Sprintf("%v", v), func(t *testing.T) {
			boxed, err := dispatch.NewAny(v)
			if err != nil {
				t.Fatalf("NewAny(%v): %v", v, err)
			}
			rv := reflect.New(reflect.TypeOf(v))
			if err := boxed.Unmarshal(rv.Interface()); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(rv.Elem().Interface(), reflect.ValueOf(v).Interface()) {
				t.Errorf("unexpected NewAny(%v).Unmarshal result: %#v", v, rv.Elem())
			}
		})
	}
}
