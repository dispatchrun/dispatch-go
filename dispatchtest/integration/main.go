//go:build !durable

package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchtest"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	stringify := dispatch.NewFunction("stringify", func(ctx context.Context, n int) (string, error) {
		return strconv.Itoa(n), nil
	})

	double := dispatch.NewFunction("double", func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	doubleAndRepeat := dispatch.NewCoroutine("double-repeat", func(ctx context.Context, n int) (string, error) {
		doubled, err := double.Await(n)
		if err != nil {
			return "", err
		}
		stringified, err := stringify.Await(doubled)
		if err != nil {
			return "", err
		}
		return strings.Repeat(stringified, doubled), nil
	})

	var functions dispatch.Registry
	functions.Register(stringify, double, doubleAndRepeat)
	defer functions.Close()

	req := dispatch.NewRequest("double-repeat", dispatch.Int(4))
	res := dispatchtest.Run(&functions, req)

	output, ok := res.Output()
	if !ok || !res.OK() {
		return fmt.Errorf("unexpected response: %s", res)
	}
	var str string
	if err := output.Unmarshal(&str); err != nil {
		return err
	} else if str != "88888888" {
		return fmt.Errorf("unexpected result: %q", str)
	}
	return nil
}
