<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/dispatchrun/.github/blob/main/profile/dispatch_logo_dark.png?raw=true">
    <img alt="dispatch logo" src="https://github.com/dispatchrun/.github/blob/main/profile/dispatch_logo_light.png?raw=true" height="64">
  </picture>
</p>

# dispatch-go

[![Test](https://github.com/dispatchrun/dispatch-go/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/dispatchrun/dispatch-go/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/dispatchrun/dispatch-go.svg)](https://pkg.go.dev/github.com/dispatchrun/dispatch-go)
[![Apache 2 License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)
[![Discord](https://img.shields.io/discord/1126309607166464131?label=Discord)](Discord)

Go package to develop applications with Dispatch.

[signup]: https://console.dispatch.run/

- [What is Dispatch?](#what-is-dispatch)
- [Installation](#installation)
  - [Installing the Dispatch CLI](#installing-the-dispatch-cli)
  - [Installing the Dispatch SDK](#installing-the-dispatch-sdk)
- [Usage](#usage)
  - [Writing Dispatch Applications](#writing-dispatch-applications)
  - [Running Dispatch Applications](#running-dispatch-applications)
  - [Writing Transactional Applications with Dispatch](#writing-transactional-applications-with-dispatch)
  - [Integration with HTTP servers](#integration-with-http-servers)
  - [Configuration](#configuration)
  - [Serialization](#serialization)
- [Examples](#examples)
- [Contributing](#contributing)

## What is Dispatch?

Dispatch is a cloud service for developing scalable and reliable applications in
Go, including:

- **Event-Driven Architectures**
- **Background Jobs**
- **Transactional Workflows**
- **Multi-Tenant Data Pipelines**

Dispatch differs from alternative solutions by allowing developers to write
simple Go code: it has a **minimal API footprint**, which usually only
requires wrapping a function (no complex framework to learn), failure
recovery is built-in by default for transient errors like rate limits or
timeouts, with a **zero-configuration** model.

To get started, follow the instructions to [sign up for Dispatch][signup] 🚀.

## Installation

### Installing the Dispatch CLI

As a pre-requisite, we recommend installing the Dispatch CLI to simplify the
configuration and execution of applications that use Dispatch. On macOS, this
can be done easily using [Homebrew](https://docs.brew.sh/):

```console
brew tap dispatchrun/dispatch
brew install dispatch
```

Alternatively, you can download the latest `dispatch` binary from the
[Releases](https://github.com/dispatchrun/dispatch/releases) page.

_Note that this step is optional, applications that use Dispatch can run without
the CLI, passing configuration through environment variables or directly in the
code. However, the CLI automates the onboarding flow and simplifies the
configuration, so we recommend starting with it._

### Installing the Dispatch SDK

The Go SDK can be added as a dependency using:

```console
go get github.com/dispatchrun/dispatch-go@latest
```

If you're starting fresh, don't forget to run `go mod init` first (e.g. `go mod init dispatch-example`).

## Usage

### Writing Dispatch Applications

The following snippet shows how to write a simple Dispatch application
that does the following:

1. declare a Dispatch function named `greet` which can run asynchronously
1. start a Dispatch endpoint to handle function calls
1. schedule a call to `greet` with the argument `World`

```go
# main.go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/dispatchrun/dispatch-go"
)

func main() {
    greet := dispatch.Func("greet", func(ctx context.Context, msg string) (any, error) {
        fmt.Printf("Hello, %s!\n", msg)
        return nil, nil
    })

    endpoint, err := dispatch.New(greet)
    if err != nil {
        log.Fatal(err)
    }

    go func() {
        if _, err := greet.Dispatch(context.Background(), "World"); err != nil {
            log.Fatal(err)
        }
    }()

    if err := endpoint.ListenAndServe(); err != nil {
        log.Fatal(err)
    }
}
```

Obviously, this is just an example, a real application would perform much more
interesting work, but it's a good start to get a sense of how to use Dispatch.

### Running Dispatch Applications

The simplest way to run a Dispatch application is to use the Dispatch CLI, first
we need to login:

```console
dispatch login
```

Then we are ready to run the example program we wrote above:

```console
dispatch run -- go run main.go
```

### Writing Transactional Applications with Dispatch

Dispatch functions are _coroutines_ that can be suspended and resumed at _await_
points. The await points are durability checkpoints; if a function fails midway
through execution, it can be retried automatically from these checkpoints.

```go
pipeline := dispatch.Func("pipeline", func (ctx context.Context, msg string) (string, error) {
    // Each await point is a durability step, the functions can be run across the
    // fleet of service instances and retried as needed without losing track of
    // progress through the function execution.
    msg, _ = transform1.Await(ctx, msg)
    msg, _ = transform2.Await(ctx, msg)
    return publish.Await(ctx, msg)
})

publish := dispatch.Func("publish", func (ctx context.Context, msg string) (*dispatchhttp.Response, error) {
    // Each dispatch function runs concurrently to the others, even if it does
    // blocking operations like this POST request, it does not prevent other
    // concurrent operations from carrying on in the program.
    return dispatchhttp.Post("https://somewhere.com/", bytes.NewBufferString(msg))
})

transform1 := dispatch.Func("transform1", func (ctx context.Context, msg string) (string, error) {
    // ...
})

transform2 := dispatch.Func("transform2", func (ctx context.Context, msg string) (string, error) {
    // ...
})
```

This model is composable and can be used to create fan-out/fan-in control flows.
`gather` can be used to wait on multiple concurrent calls:

```go
process := dispatch.Func("process", func (ctx context.Context, msgs []string) ([]string, error) {
    // Transform messages concurrently and await the results.
    return transform.Gather(ctx, msgs)
})

transform := dispatch.Func("transform", func (ctx context.Context, msg string) (string, error) {
    // ...
})
```

Dispatch converts Go functions into _Distributed Coroutines_, which can be
suspended and resumed on any instance of a service across a fleet. For a deep
dive on these concepts, read our
[_Distributed Coroutines_](https://dispatch.run/blog/distributed-coroutines-in-python)
blog post.

### Integration with HTTP servers

Dispatch can be integrated into an existing HTTP server.

In the example below, a request to `/` triggers an asynchronous call to the `greet`
function:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "net/http"

    "github.com/dispatchrun/dispatch-go"
)

func main() {
    greet := dispatch.Func("greet", func(ctx context.Context, msg string) (any, error) {
        fmt.Printf("Hello, %s!\n", msg)
        return nil, nil
    })

    endpoint, err := dispatch.New(greet)
    if err != nil {
        log.Fatal(err)
    }

    mux := http.NewServeMux()

    mux.Handle(endpoint.Handler())

    mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        greet.Dispatch(r.Context(), "World")
        w.WriteHeader(http.StatusOK)
    }))

    if err := http.ListenAndServe("localhost:8000", mux); err != nil {
        log.Fatal(err)
    }
}
```

### Configuration

The Dispatch CLI automatically configures the SDK, so manual configuration is
usually not required when running Dispatch applications. However, in some
advanced cases, it might be useful to explicitly set configuration options.

In order for Dispatch to interact with functions remotely, the SDK needs to be
configured with the address at which the server can be reached. The Dispatch
API Key must also be set, and optionally, a public signing key should be
configured to verify that requests originated from Dispatch. These
configuration options can be passed as arguments to the
the `Dispatch` constructor, but by default they will be loaded from environment
variables:

| Environment Variable        | Value Example                      |
| :-------------------------- | :--------------------------------- |
| `DISPATCH_API_KEY`          | `d4caSl21a5wdx5AxMjdaMeWehaIyXVnN` |
| `DISPATCH_ENDPOINT_URL`     | `https://service.domain.com`       |
| `DISPATCH_VERIFICATION_KEY` | `-----BEGIN PUBLIC KEY-----...`    |

### Serialization

#### Inputs & Outputs

Dispatch uses protobuf to serialize input and output values.

The inputs and outputs must either be primitive values, list or maps
of primitive values, or have a type that implements one of the
following interfaces:
- `encoding.TextMarshaler`
- `encoding.BinaryMarshaler`
- `json.Marshaler`
- `proto.Message`

#### Coroutine State

Dispatch uses the [coroutine] library to serialize coroutines.

[coroutine]: https://github.com/dispatchrun/coroutine

The user must ensure that the contents of their stack frames are
serializable.

For help with a serialization issues, please submit a [GitHub issue][issues].

[issues]: https://github.com/dispatchrun/dispatch-go/issues

## Examples

Check out the [examples](examples/) directory for code samples to help you get
started with the SDK.

## Contributing

Contributions are always welcome! Would you spot a typo or anything that needs
to be improved, feel free to send a pull request.

Pull requests need to pass all CI checks before getting merged. Anything that
isn't a straightforward change would benefit from being discussed in an issue
before submitting a change.

Remember to be respectful and open minded!
