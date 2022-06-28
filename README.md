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
<p align="center">
Build once. 
Integrate your blockchain everywhere.
</p>

## Overview

The `rosetta-sdk-go` provides a collection of packages used for interaction with the Rosetta API specification. Much of the code in this repository is generated from the [rosetta-specifications](https://github.com/coinbase/rosetta-specifications) repository.

## Documentation

You can find the Rosetta API documentation at [rosetta-api.org](https://www.rosetta-api.org/docs/welcome.html). 

Check out the [Getting Started](https://www.rosetta-api.org/docs/getting_started.html) section to start diving into Rosetta. 

Our documentation is divided into the following sections:

* [Product Overview](https://www.rosetta-api.org/docs/welcome.html)
* [Getting Started](https://www.rosetta-api.org/docs/getting_started.html)
* [Rosetta API Spec](https://www.rosetta-api.org/docs/Reference.html)
* [Testing](https://www.rosetta-api.org/docs/rosetta_cli.html)
* [Best Practices](https://www.rosetta-api.org/docs/node_deployment.html)
* [Repositories](https://www.rosetta-api.org/docs/rosetta_specifications.html)

## SDK Packages

* [Types](types): Auto-generated Rosetta types
* [Client](client): Low-level communication with any Rosetta server
* [Server](server): Simplified Rosetta API server development
* [Asserter](asserter): Validation of Rosetta types
* [Fetcher](fetcher): Simplified and validated communication with any Rosetta server
* [Parser](parser): Tool for parsing Rosetta blocks
* [Syncer](syncer): Sync Rosetta blocks with customizable handling
* [Reconciler](reconciler): Compare derived balances with node balances
* [Keys](keys): Cryptographic operations for Rosetta-supported curves
* [Constructor](constructor): Coordinate the construction and broadcast of transactions

These packages are demoed extensively in [examples](examples) and are utilized throughout the [rosetta-cli](https://github.com/coinbase/rosetta-cli) tool.

## Contributing

You may contribute to the `rosetta-sdk-go` project in various ways:

* [Asking Questions](CONTRIBUTING.md/#asking-questions)
* [Providing Feedback](CONTRIBUTING.md/#providing-feedback)
* [Reporting Issues](CONTRIBUTING.md/#reporting-issues)

Read our [Contributing](CONTRIBUTING.MD) documentation for more information.

When you've finished an implementation for a blockchain, share your work in the [ecosystem category of the community site](https://community.rosetta-api.org/c/ecosystem). Platforms looking for implementations for certain blockchains will be monitoring this section of the website for high-quality implementations they can use for integration. Make sure that your implementation meets the [expectations](https://www.rosetta-api.org/docs/node_deployment.html) of any implementation.

## Related Projects

* [rosetta-specifications](https://github.com/coinbase/rosetta-specifications) — The `rosetta-specifications` repository generates the SDK code in the `rosetta-sdk-go` repository.
* [rosetta-cli](https://github.com/coinbase/rosetta-cli) — Use the `rosetta-cli` tool to test your Rosetta API implementation. The tool also provides the ability to look up block contents and account balances.

## Reference Implementations

To help you with examples, we developed complete Rosetta API reference implementations for [Bitcoin](https://github.com/coinbase/rosetta-bitcoin) and [Ethereum](https://github.com/coinbase/rosetta-ethereum). Developers of Bitcoin-like or Ethereum-like blockchains may find it easier to fork these reference implementations than to write an implementation from scratch.

You can also find community implementations for a variety of blockchains in the [rosetta-ecosystem](https://github.com/coinbase/rosetta-ecosystem) repository, and in the [ecosystem category](https://community.rosetta-api.org/c/ecosystem) of our community site. 

### Using Golang
If you are comfortable with Golang, the easiest way to write a Rosetta Data API implementation is to use this repository. This Golang project provides a [server package](https://github.com/coinbase/rosetta-sdk-go/tree/master/server) that empowers a developer to write a full Rosetta Data API server by only implementing an interface. This package automatically validates client requests and calls the functions you implement with pre-parsed requests (instead of in raw JSON).

This is a simple [example](https://github.com/coinbase/rosetta-sdk-go/tree/master/examples/server) of how to write an implementation using this package in rosetta-sdk-go.

### Using Another Language

If you plan to use a language other than Golang, you will need to either codegen a server (using [Swagger Codegen](https://swagger.io/tools/swagger-codegen) or [OpenAPI Generator](https://openapi-generator.tech/)) or write one from scratch. If you choose to write an implementation in another language, we ask that you create a separate repository in an SDK-like format for all the code you generate so that other developers can use it. You can add your repository to the list in the [rosetta-ecosystem](https://github.com/coinbase/rosetta-ecosystem) repository, and in the [ecosystem category](https://community.rosetta-api.org/c/ecosystem) of our community site. Use this repository (rosetta-sdk-go) for an example of how to generate code from this specification.

### Syncer

The core of any integration is syncing blocks reliably. The [syncer](https://github.com/coinbase/rosetta-sdk-go/tree/master/syncer) serially processes blocks from a Data API implementation (automatically handling re-orgs) with user-defined handling logic and pluggable storage. After a block is processed, store it to a DB or send a push notification...it's up to you!

### Parser

When reading the operations in a block, it can be helpful to apply higher-level groupings to related operations, or match operations in a transaction to some set of generic descriptions (e.g., ensure there are two operations of equal but opposite amounts). The [parser](https://github.com/coinbase/rosetta-sdk-go/tree/master/parser) empowers any integrator to build abstractions on top of the [low-level building blocks](https://www.rosetta-api.org/docs/low_level_ops.html) that the Rosetta API exposes.

## SDK Development

While working on improvements to this repository, we recommend that you use these commands to check your code:

* `make deps` to install dependencies
* `make gen` to generate types and helpers
* `make test` to run tests
* `make lint` to lint the source code (including generated code)
* `make release` to check if code passes all tests run by CircleCI

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2022 Coinbase
