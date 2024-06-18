package dispatch

import (
	"testing"
	"time"

	sdkv1 "buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go/dispatch/sdk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCall(t *testing.T) {
	t.Run("with no options", func(t *testing.T) {
		call := NewCall("endpoint1", "function2", Int(11))

		if got := call.Endpoint(); got != "endpoint1" {
			t.Errorf("unexpected call endpoint: %v", got)
		}
		if got := call.Function(); got != "function2" {
			t.Errorf("unexpected call function: %v", got)
		}
		if got := call.Input(); !got.Equal(Int(11)) {
			t.Errorf("unexpected call input: %v", got)
		}
		if got := call.CorrelationID(); got != 0 {
			t.Errorf("unexpected call correlation ID: %v", got)
		}
		if got := call.Expiration(); got != 0 {
			t.Errorf("unexpected call expiration: %v", got)
		}
		if got := call.Version(); got != "" {
			t.Errorf("unexpected call version: %v", got)
		}

		inputAny, _ := anypb.New(wrapperspb.Int64(11))
		want := &sdkv1.Call{
			Endpoint: "endpoint1",
			Function: "function2",
			Input:    inputAny,
		}
		if !proto.Equal(call.proto, want) {
			t.Errorf("unexpected call proto message: %s", call.proto)
		}
	})

	t.Run("with options", func(t *testing.T) {
		call := NewCall("endpoint1", "function2",
			Input(Int(11)),
			CorrelationID(1234),
			Expiration(10*time.Second),
			Version("xyzzy"))

		if got := call.Endpoint(); got != "endpoint1" {
			t.Errorf("unexpected call endpoint: %v", got)
		}
		if got := call.Function(); got != "function2" {
			t.Errorf("unexpected call function: %v", got)
		}
		if got := call.Input(); !got.Equal(Int(11)) {
			t.Errorf("unexpected call input: %v", got)
		}
		if got := call.CorrelationID(); got != 1234 {
			t.Errorf("unexpected call correlation ID: %v", got)
		}
		if got := call.Expiration(); got != 10*time.Second {
			t.Errorf("unexpected call expiration: %v", got)
		}
		if got := call.Version(); got != "xyzzy" {
			t.Errorf("unexpected call version: %v", got)
		}

		inputAny, _ := anypb.New(wrapperspb.Int64(11))
		want := &sdkv1.Call{
			Endpoint:      "endpoint1",
			Function:      "function2",
			Input:         inputAny,
			CorrelationId: 1234,
			Expiration:    durationpb.New(10 * time.Second),
			Version:       "xyzzy",
		}
		if !proto.Equal(call.proto, want) {
			t.Errorf("unexpected call proto message: %s", call.proto)
		}
	})

	t.Run("zero value", func(t *testing.T) {
		var call Call

		if got := call.Endpoint(); got != "" {
			t.Errorf("unexpected call endpoint: %v", got)
		}
		if got := call.Function(); got != "" {
			t.Errorf("unexpected call function: %v", got)
		}
		if got := call.Input(); got.TypeURL() != "" {
			t.Errorf("unexpected call input: %v", got)
		}
		if got := call.CorrelationID(); got != 0 {
			t.Errorf("unexpected call correlation ID: %v", got)
		}
		if got := call.Expiration(); got != 0 {
			t.Errorf("unexpected call expiration: %v", got)
		}
		if got := call.Version(); got != "" {
			t.Errorf("unexpected call version: %v", got)
		}
	})
}
