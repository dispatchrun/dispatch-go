package dispatch_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/dispatchrun/coroutine"
	"github.com/dispatchrun/dispatch-go"
)

func logMode(t *testing.T) {
	t.Helper()

	if coroutine.Durable {
		t.Log("running in durable mode")
	} else {
		t.Log("running in volatile mode")
	}
}

func TestCoroutineReturn(t *testing.T) {
	logMode(t)

	coro := dispatch.NewCoroutine("stringify", func(ctx context.Context, in int) (string, error) {
		if in < 0 {
			return "", fmt.Errorf("%w: %d", dispatch.ErrInvalidArgument, in)
		}
		return strconv.Itoa(in), nil
	})
	defer coro.Close()

	res := coro.Run(context.Background(), dispatch.NewRequest("stringify", dispatch.Input(dispatch.Int(11))))
	if res.Status() != dispatch.OKStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}
	output, ok := res.Output()
	if !ok {
		t.Errorf("expected output, got: %s", res)
	}
	var got string
	if err := output.Unmarshal(&got); err != nil {
		t.Fatal(err)
	} else if got != "11" {
		t.Errorf("unexpected output: %s", got)
	}

	res = coro.Run(context.Background(), dispatch.NewRequest("stringify", dispatch.Input(dispatch.Int(-23))))
	if res.Status() != dispatch.InvalidArgumentStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}
	if err, ok := res.Error(); !ok {
		t.Errorf("expected error, got: %s", res)
	} else if got := err.Message(); got != "InvalidArgument: -23" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestCoroutineYieldExitResponse(t *testing.T) {
	logMode(t)

	coro := dispatch.NewCoroutine("stringify", func(ctx context.Context, in int) (string, error) {
		var res dispatch.Response
		if in < 0 {
			res = dispatch.NewResponseErrorf("%w: %d", dispatch.ErrInvalidArgument, in)
		} else {
			output := dispatch.String(strconv.Itoa(in))
			res = dispatch.NewResponse(dispatch.Output(output))
		}
		dispatch.Yield(res)
		panic("unreachable")
	})
	defer coro.Close()

	res := coro.Run(context.Background(), dispatch.NewRequest("stringify", dispatch.Input(dispatch.Int(11))))
	if res.Status() != dispatch.OKStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}
	output, ok := res.Output()
	if !ok {
		t.Errorf("expected output, got: %s", res)
	}
	var got string
	if err := output.Unmarshal(&got); err != nil {
		t.Fatal(err)
	} else if got != "11" {
		t.Errorf("unexpected output: %s", got)
	}

	res = coro.Run(context.Background(), dispatch.NewRequest("stringify", dispatch.Input(dispatch.Int(-23))))
	if res.Status() != dispatch.InvalidArgumentStatus {
		t.Errorf("unexpected status: %s", res.Status())
	}
	if err, ok := res.Error(); !ok {
		t.Errorf("expected error, got: %s", res)
	} else if got := err.Message(); got != "InvalidArgument: -23" {
		t.Errorf("unexpected error: %s", got)
	}
}
