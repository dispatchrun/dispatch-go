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
	stringify := dispatch.Func("stringify", func(ctx context.Context, n int) (string, error) {
		return strconv.Itoa(n), nil
	})

	double := dispatch.Func("double", func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	})

	doubleAndRepeat := dispatch.Func("double-repeat", func(ctx context.Context, n int) (string, error) {
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

	runner := dispatchtest.NewRunner(stringify, double, doubleAndRepeat)

	output, err := dispatchtest.Call(runner, doubleAndRepeat, 4)
	if err != nil {
		return err
	}
	if output != "88888888" {
		return fmt.Errorf("unexpected output: %q", output)
	}
	fmt.Println("OK")
	return nil
}
