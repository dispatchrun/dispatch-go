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

	var functions dispatch.Registry
	functions.Register(stringify, double, doubleAndRepeat)
	defer functions.Close()

	call, err := doubleAndRepeat.NewCall(4)
	if err != nil {
		return fmt.Errorf("new call failed: %v", err)
	}

	output, err := dispatchtest.Call[string](&functions, call)
	if err != nil {
		return err
	}
	if output != "88888888" {
		return fmt.Errorf("unexpected output: %q", output)
	}
	return nil
}
