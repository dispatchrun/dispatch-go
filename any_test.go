package dispatch_test

import (
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
	for _, v := range []int{0, 11, -1, 2, math.MinInt, math.MaxInt} {
		boxed := dispatch.Int(v)
		var got int
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected int: got %v, want %v", got, v)
		}
	}
}

func TestAnyUint(t *testing.T) {
	for _, v := range []uint{0, 11, 2, math.MaxUint} {
		boxed := dispatch.Uint(v)
		var got uint
		if err := boxed.Unmarshal(&got); err != nil {
			t.Fatal(err)
		} else if got != v {
			t.Errorf("unexpected uint: got %v, want %v", got, v)
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

		"foo",
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
