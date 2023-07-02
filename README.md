<!-- Rosetta Graphic Image -->
<p align="center">
  <a href="https://www.rosetta-api.org">
    <img width="90%" alt="Rosetta" src="https://www.rosetta-api.org/img/rosetta_header.png">
  </a>
</p>
<!-- Repository Title and Description -->
<h3 align="center">
   Rosetta SDK
</h3>
<p align="center">
Go SDK to create and interact with Rosetta API implementations
</p>
<!-- Badges -->
<p align="center">
  <a href="https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master"><img src="https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master.svg?style=shield" /></a>
  <a href="https://coveralls.io/github/coinbase/rosetta-sdk-go"><img src="https://coveralls.io/repos/github/coinbase/rosetta-sdk-go/badge.svg" /></a>
  <a href="https://goreportcard.com/report/github.com/coinbase/rosetta-sdk-go"><img src="https://goreportcard.com/badge/github.com/coinbase/rosetta-sdk-go" /></a>
  <a href="https://github.com/coinbase/rosetta-sdk-go/blob/master/LICENSE.txt"><img src="https://img.shields.io/github/license/coinbase/rosetta-sdk-go.svg" /></a>
  <a href="https://pkg.go.dev/github.com/coinbase/rosetta-sdk-go?tab=overview"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield" /></a>
</p>
<!-- Rosetta Tagline -->
<p align="center">
Build once. 
Integrate your blockchain everywhere.
</p>

<!-- h2 Overview -->
## Overview

The `rosetta-sdk-go` provides a collection of packages used for interaction with the Rosetta API specification. Much of the code in this repository is generated from the [rosetta-specifications](https://github.com/coinbase/rosetta-specifications) repository.

Jump to:

* [Getting Started](#Getting-Started)
* [Quick Examples](#Quick-Examples)
* [Testing](#Testing)
* [Documentation](#Documentation)
* [Related Projects](#Related-Projects)

If you have a blockchain based on go-ethereum, we recommend that you use our [rosetta-geth-sdk](https://github.com/coinbase/rosetta-geth-sdk) SDK.
 
<!-- h2 Getting Started Heading -->
## Getting Started

 This Golang project provides a [server package](https://github.com/coinbase/rosetta-sdk-go/tree/master/server) that empowers a developer to write a full Rosetta Data API server by only implementing an interface. This package automatically validates client requests and calls the functions you implement with pre-parsed requests (instead of in raw JSON).

 If you plan to use a language other than Golang, you will need to either codegen a server (using [Swagger Codegen](https://swagger.io/tools/swagger-codegen) or [OpenAPI Generator](https://openapi-generator.tech/)) or write one from scratch. If you choose to write an implementation in another language, we ask that you create a separate repository in an SDK-like format for all the code you generate so that other developers can use it. You can add your repository to the list in the [rosetta-ecosystem](https://github.com/coinbase/rosetta-ecosystem) repository, and in the [ecosystem category](https://community.rosetta-api.org/c/ecosystem) of our community site. Use this repository (rosetta-sdk-go) for an example of how to generate code from this specification.

<!-- h3 Installation Guide -->
### Installation Guide

This command installs the Server package.
```shell
go get github.com/coinbase/rosetta-sdk-go/server
```
<!-- h3 Additional Sections (Components) -->
### Components
#### Router
The router is a [Mux](https://github.com/gorilla/mux) router that routes traffic to the correct controller.

#### Controller
Controllers are automatically generated code that specify an interface that a service must implement.

#### Services
Services are implemented by you to populate responses. These services are invoked by controllers.

### Recommended Folder Structure
```
main.go
/services
  block_service.go
  network_service.go
  ...
```

<!-- h2 Quick Examples -->
## Quick Examples
<!-- h3 Complete SDK Example -->
### Complete SDK Example

This is an [example](https://github.com/coinbase/rosetta-sdk-go/tree/master/examples/server) of how to write an implementation using the Server package in rosetta-sdk-go.

```Go
package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/examples/server/services"
	"github.com/coinbase/rosetta-sdk-go/server"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	serverPort = 8080
)

// NewBlockchainRouter creates a Mux http.Handler from a collection
// of server controllers.
func NewBlockchainRouter(
	network *types.NetworkIdentifier,
	asserter *asserter.Asserter,
) http.Handler {
	networkAPIService := services.NewNetworkAPIService(network)
	networkAPIController := server.NewNetworkAPIController(
		networkAPIService,
		asserter,
	)

	blockAPIService := services.NewBlockAPIService(network)
	blockAPIController := server.NewBlockAPIController(
		blockAPIService,
		asserter,
	)

	return server.NewRouter(networkAPIController, blockAPIController)
}

func main() {
	network := &types.NetworkIdentifier{
		Blockchain: "Rosetta",
		Network:    "Testnet",
	}

	// The asserter automatically rejects incorrectly formatted
	// requests.
	asserter, err := asserter.NewServer(
		[]string{"Transfer", "Reward"},
		false,
		[]*types.NetworkIdentifier{network},
		nil,
		false,
		"",
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create the main router handler then apply the logger and Cors
	// middlewares in sequence.
	router := NewBlockchainRouter(network, asserter)
	loggedRouter := server.LoggerMiddleware(router)
	corsRouter := server.CorsMiddleware(loggedRouter)
	log.Printf("Listening on port %d\n", serverPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", serverPort), corsRouter))
}
```

<!-- h2 SDK Packages -->
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

### Syncer

The core of any integration is syncing blocks reliably. The [syncer](https://github.com/coinbase/rosetta-sdk-go/tree/master/syncer) serially processes blocks from a Data API implementation (automatically handling re-orgs) with user-defined handling logic and pluggable storage. After a block is processed, store it to a DB or send a push notification—the decision is up to you!

### Parser

When reading the operations in a block, it's helpful to apply higher-level groupings to related operations, or match operations in a transaction to some set of generic descriptions (i.e., ensure there are two operations of equal but opposite amounts). The [parser](https://github.com/coinbase/rosetta-sdk-go/tree/master/parser) empowers any integrator to build abstractions on top of the [building blocks](https://www.rosetta-api.org/docs/rosetta_principles.html#universal-transactions) that the Rosetta API exposes.

<!-- h3 Configuring the SDK -->
<!-- See the [Configuration](https://github.com/coinbase/rosetta-geth-sdk/blob/master/configuration/configuration.go) file for more information on how to configure the SDK. -->

<!-- h2 Development -->
## Development

Helpful commands for development:
<!-- Use the command action as the h3 header -->
### Install Dependencies
```
make deps
```

### Generate Types, Client and Server
```
make gen
```
If you want to modify client and server, please modify files under `templates/client` and `templates/server` then run `make gen`

### Run Tests
```
make test
```

### Lint the Source Code
This includes the generated code.
```
make lint
```

### Code Check
```
make release
```

<!-- h2 Testing Guide -->
## Testing

To validate `rosetta-sdk-go`, [install `rosetta-cli`](https://github.com/coinbase/rosetta-cli#install) and run one of the following commands:

* `rosetta-cli check:data --configuration-file rosetta-cli-conf/testnet/config.json` - This command validates that the Data API implementation is correct, using the bitcoin `testnet` node. It also ensures that the implementation does not miss any balance-changing operations.
* `rosetta-cli check:construction --configuration-file rosetta-cli-conf/testnet/config.json` - This command validates the Construction API implementation. It also verifies transaction construction, signing, and submissions to the `testnet` network.
* `rosetta-cli check:data --configuration-file rosetta-cli-conf/mainnet/config.json` - This command validates that the Data API implementation is correct, using the bitcoin `mainnet` node. It also ensures that the implementation does not miss any balance-changing operations.

Read the [How to Test your Rosetta Implementation](https://www.rosetta-api.org/docs/rosetta_test.html) documentation for additional details.

<!-- h2 Contributing Guide -->
## Contributing

You may contribute to the `rosetta-sdk-go` project in various ways:

* [Asking Questions](CONTRIBUTING.md/#asking-questions)
* [Providing Feedback](CONTRIBUTING.md/#providing-feedback)
* [Reporting Issues](CONTRIBUTING.md/#reporting-issues)

Read our [Contributing](CONTRIBUTING.md) documentation for more information.

When you've finished an implementation for a blockchain, share your work in the [ecosystem category of the community site](https://community.rosetta-api.org/c/ecosystem). Platforms looking for implementations for certain blockchains will be monitoring this section of the website for high-quality implementations they can use for integration. Make sure that your implementation meets the [expectations](https://www.rosetta-api.org/docs/node_deployment.html) of any implementation.

<!-- h2 Documentation links -->
## Documentation

You can find the Rosetta API documentation at [rosetta-api.org](https://www.rosetta-api.org/docs/welcome.html). 

Check out the [Getting Started](https://www.rosetta-api.org/docs/getting_started.html) section to start diving into Rosetta. 

Our documentation is divided into the following sections:
<!-- See the sidebar menu at rosetta-api.org for section info -->
* [Product Overview](https://www.rosetta-api.org/docs/welcome.html)
* [Getting Started](https://www.rosetta-api.org/docs/getting_started.html)
* [Rosetta API Spec](https://www.rosetta-api.org/docs/Reference.html)
* [Samples](https://www.rosetta-api.org/docs/reference-implementations.html)
* [Testing](https://www.rosetta-api.org/docs/rosetta_cli.html)
* [Best Practices](https://www.rosetta-api.org/docs/node_deployment.html)
* [Repositories](https://www.rosetta-api.org/docs/rosetta_specifications.html)

<!-- h2 Related Projects -->
## Related Projects

* [rosetta-specifications](https://github.com/coinbase/rosetta-specifications) — The `rosetta-specifications` repository generates the SDK code in the `rosetta-sdk-go` repository.
* [rosetta-cli](https://github.com/coinbase/rosetta-cli) — Use the `rosetta-cli` tool to test your Rosetta API implementation. The tool also provides the ability to look up block contents and account balances.
* [rosetta-geth-sdk](https://github.com/coinbase/rosetta-geth-sdk) – The `rosetta-geth-sdk` repository provides a collection of packages used for interaction with the Rosetta API specification. The goal of this SDK is to help accelerate Rosetta API implementation on go-ethereum based chains.
<!-- h3 Sample Implementations -->
### Sample Implementations

To help you with examples, we developed complete Rosetta API sample implementations for [Bitcoin](https://github.com/coinbase/rosetta-bitcoin) and [Ethereum](https://github.com/coinbase/rosetta-ethereum). Developers of Bitcoin-like or Ethereum-like blockchains may find it easier to fork these implementation samples than to write an implementation from scratch.

You can also find community implementations for a variety of blockchains in the [rosetta-ecosystem](https://github.com/coinbase/rosetta-ecosystem) repository, and in the [ecosystem category](https://community.rosetta-api.org/c/ecosystem) of our community site. 
<!-- h2 License -->
## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).
<!-- Copyright {© year Coinbase} -->
© 2022 Coinbase
