# Fetcher

[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield)](https://pkg.go.dev/github.com/coinbase/mesh-sdk-go/fetcher?tab=doc)

The Fetcher package provides a simplified client interface to communicate
with a Mesh server. It also provides automatic retries and concurrent block
fetches.

If you want a lower-level interface to communicate with a Mesh server,
check out the [Client](/client).

## Installation

```shell
go get github.com/coinbase/mesh-sdk-go/fetcher
```

## Create a Fetcher
Creating a basic Fetcher is as easy as providing a context and Mesh server URL:
```go
fetcher := fetcher.New(ctx, serverURL)
```

It is also possible to provide a series of optional arguments that override
default behavior. For example, it is possible to override the concurrency
for making block requests:
```go
fetcher := fetcher.New(ctx, serverURL, fetcher.WithBlockConcurrency(10))
```

## More Examples
Check out the [examples](/examples) to see how easy
it is to connect to a Mesh server.
