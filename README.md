# rosetta-sdk-go

[![Coinbase](https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master.svg?style=shield)](https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/coinbase/rosetta-sdk-go/badge.svg)](https://coveralls.io/github/coinbase/rosetta-sdk-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/coinbase/rosetta-sdk-go)](https://goreportcard.com/report/github.com/coinbase/rosetta-sdk-go)
[![License](https://img.shields.io/github/license/coinbase/rosetta-sdk-go.svg)](https://github.com/coinbase/rosetta-sdk-go/blob/master/LICENSE.txt)
[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield)](https://pkg.go.dev/github.com/coinbase/rosetta-sdk-go?tab=overview)

## What is Rosetta?
Rosetta is a new project from Coinbase to standardize the process
of deploying and interacting with blockchains. With an explicit
specification to adhere to, all parties involved in blockchain
development can spend less time figuring out how to integrate
with each other and more time working on the novel advances that
will push the blockchain ecosystem forward. In practice, this means
that any blockchain project that implements the requirements outlined
in this specification will enable exchanges, block explorers,
and wallets to integrate with much less communication overhead
and network-specific work.

## Packages
* [Models](models): Auto-generated Rosetta models
* [Client](client): Low-level communication with any Rosetta server
* [Server](server): Simplified Rosetta server development
* [Asserter](asserter): Validation of Rosetta models
* [Fetcher](fetcher): Simplified and validated communication with
any Rosetta server

## Examples
The packages listed above are demoed extensively in
[examples](examples) and are utilized throughout the
[Rosetta Validator](https://github.com/coinbase/rosetta-validator).

## Development
* `make deps` to install dependencies
* `make gen` to generate models and helpers
* `make test` to run tests
* `make lint` to lint the source code (including generated code)
* `make release` to check if code passes all tests run by CircleCI

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2020 Coinbase
