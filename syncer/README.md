# Syncer

[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield)](https://pkg.go.dev/github.com/coinbase/mesh-sdk-go/syncer?tab=doc)

The Syncer package provides support for syncing blocks from any Mesh Data API
implementation. If you want to see an example of how to use this package, take
a look at [mesh-cli](https://github.com/coinbase/mesh-cli).

## Features
* Automatic handling of block re-orgs
* Multi-threaded block fetching (using the `fetcher` package)
* Implementable `Handler` to define your own block processing logic (ex: store
processed blocks to a db or print our balance changes)

## Installation

```shell
go get github.com/coinbase/mesh-sdk-go/syncer
```

## Future Work
* Sync multiple shards in a sharded blockchain
