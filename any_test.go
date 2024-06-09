package dispatch_test

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/dispatchrun/dispatch-go"
)

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
		11,
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
