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

Build once. 
Integrate your blockchain everywhere.

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
* [rosetta-cli](https://github.com/coinbase/rosetta-ecosystem) — Use the `rosetta-cli` tool to test your Rosetta API implementation. The tool also provides the ability to look up block contents and account balances.

### Reference Implementations

To help you with examples, we developed complete Rosetta API reference implementations for [Bitcoin](https://github.com/coinbase/rosetta-bitcoin) and [Ethereum](https://github.com/coinbase/rosetta-ethereum). Developers of Bitcoin-like or Ethereum-like blockchains may find it easier to fork these reference implementations than to write an implementation from scratch.

You can also find community implementations for a variety of blockchains in the [rosetta-ecosystem][https://github.com/coinbase/rosetta-ecosystem] repository, and in the [ecosystem category](https://community.rosetta-api.org/c/ecosystem) of our community site. 

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