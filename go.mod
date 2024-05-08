module github.com/dispatchrun/dispatch-go

go 1.22.3

require (
	buf.build/gen/go/stealthrocket/dispatch-proto/protocolbuffers/go v1.34.1-20240501223323-969ec62f124d.1
	github.com/aws/aws-lambda-go v1.47.0
	github.com/stealthrocket/coroutine v0.7.0
	google.golang.org/protobuf v1.34.1
)

require buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.34.1-20231115204500-e097f827e652.1 // indirect
