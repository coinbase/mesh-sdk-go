<p align="center">
  <a href="https://www.rosetta-api.org">
    <img width="90%" alt="Rosetta" src="https://www.rosetta-api.org/img/rosetta_header.png">
  </a>
</p>
<h3 align="center">
   Rosetta SDK
</h3>
<p align="center">
Go SDK to create and interact with Rosetta API implementations
</p>
<p align="center">
  <a href="https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master"><img src="https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master.svg?style=shield" /></a>
  <a href="https://coveralls.io/github/coinbase/rosetta-sdk-go"><img src="https://coveralls.io/repos/github/coinbase/rosetta-sdk-go/badge.svg" /></a>
  <a href="https://goreportcard.com/report/github.com/coinbase/rosetta-sdk-go"><img src="https://goreportcard.com/badge/github.com/coinbase/rosetta-sdk-go" /></a>
  <a href="https://github.com/coinbase/rosetta-sdk-go/blob/master/LICENSE.txt"><img src="https://img.shields.io/github/license/coinbase/rosetta-sdk-go.svg" /></a>
  <a href="https://pkg.go.dev/github.com/coinbase/rosetta-sdk-go?tab=overview"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield" /></a>
</p>

## Overview
The `rosetta-sdk-go` provides a collection of packages used for interaction
with the Rosetta API specification. Much of the code in this repository is
generated from the [rosetta-specifications](https://github.com/coinbase/rosetta-specifications).

## Documentation
Before diving into the SDK, we recommend taking a look at the Rosetta API Docs:

* [Overview](https://www.rosetta-api.org/docs/welcome.html)
* [Data API](https://www.rosetta-api.org/docs/data_api_introduction.html)
* [Construction API](https://www.rosetta-api.org/docs/construction_api_introduction.html)

## Packages
* [Types](types): Auto-generated Rosetta types
* [Client](client): Low-level communication with any Rosetta server
* [Server](server): Simplified Rosetta API server development
* [Asserter](asserter): Validation of Rosetta types
* [Fetcher](fetcher): Simplified and validated communication with
any Rosetta server
* [Parser](parser): Tool for parsing Rosetta blocks
* [Syncer](syncer): Sync Rosetta blocks with customizable handling
* [Reconciler](reconciler): Compare derived balances with node balances
* [Keys](keys): Cryptographic operations for Rosetta-supported curves
* [Constructor](constructor): Coordinate the construction and broadcast of transactions

## Examples
The packages listed above are demoed extensively in
[examples](examples) and are utilized throughout the
[rosetta-cli](https://github.com/coinbase/rosetta-cli).

## Development
* `make deps` to install dependencies
* `make gen` to generate types and helpers
* `make test` to run tests
* `make lint` to lint the source code (including generated code)
* `make release` to check if code passes all tests run by CircleCI

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2021 Coinbase
