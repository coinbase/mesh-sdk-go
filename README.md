<!-- Mesh Graphic Image -->
<p align="center">
  <a href="https://www.mesh-api.org">
    <img width="90%" alt="Mesh" src="https://www.mesh-api.org/img/mesh_header.png">
  </a>
</p>
<!-- Repository Title and Description -->
<h3 align="center">
   Mesh SDK
</h3>
<p align="center">
Go SDK to create and interact with Mesh API implementations
</p>
<!-- Badges -->
<p align="center">
  <a href="https://circleci.com/gh/coinbase/mesh-sdk-go/tree/master"><img src="https://circleci.com/gh/coinbase/mesh-sdk-go/tree/master.svg?style=shield" /></a>
  <a href="https://coveralls.io/github/coinbase/mesh-sdk-go"><img src="https://coveralls.io/repos/github/coinbase/mesh-sdk-go/badge.svg" /></a>
  <a href="https://goreportcard.com/report/github.com/coinbase/mesh-sdk-go"><img src="https://goreportcard.com/badge/github.com/coinbase/mesh-sdk-go" /></a>
  <a href="https://github.com/coinbase/mesh-sdk-go/blob/master/LICENSE.txt"><img src="https://img.shields.io/github/license/coinbase/mesh-sdk-go.svg" /></a>
  <a href="https://pkg.go.dev/github.com/coinbase/mesh-sdk-go?tab=overview"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield" /></a>
</p>
<!-- Mesh Tagline -->
<p align="center">
Build once. 
Integrate your blockchain everywhere.
</p>

<!-- h2 Overview -->
## Overview

The `mesh-sdk-go` provides a collection of packages used for interaction with the Mesh API specification. Much of the code in this repository is generated from the [mesh-specifications](https://github.com/coinbase/mesh-specifications) repository.

Jump to:

* [Getting Started](#Getting-Started)
* [Quick Examples](#Quick-Examples)
* [Testing](#Testing)
* [Documentation](#Documentation)
* [Related Projects](#Related-Projects)

If you have a blockchain based on go-ethereum, we recommend that you use our [mesh-geth-sdk](https://github.com/coinbase/mesh-geth-sdk) SDK.
 
<!-- h2 Getting Started Heading -->
## Getting Started

 This Golang project provides a [server package](https://github.com/coinbase/mesh-sdk-go/tree/master/server) that empowers a developer to write a full Mesh Data API server by only implementing an interface. This package automatically validates client requests and calls the functions you implement with pre-parsed requests (instead of in raw JSON).

 If you plan to use a language other than Golang, you will need to either codegen a server (using [Swagger Codegen](https://swagger.io/tools/swagger-codegen) or [OpenAPI Generator](https://openapi-generator.tech/)) or write one from scratch. If you choose to write an implementation in another language, we ask that you create a separate repository in an SDK-like format for all the code you generate so that other developers can use it. You can add your repository to the list in the [mesh-ecosystem](https://github.com/coinbase/mesh-ecosystem) repository, and in the [ecosystem category](https://community.mesh-api.org/c/ecosystem) of our community site. Use this repository (mesh-sdk-go) for an example of how to generate code from this specification.

<!-- h3 Installation Guide -->
### Installation Guide

This command installs the Server package.
```shell
go get github.com/coinbase/mesh-sdk-go/server
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

This is an [example](https://github.com/coinbase/mesh-sdk-go/tree/master/examples/server) of how to write an implementation using the Server package in mesh-sdk-go.

```Go
package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/coinbase/mesh-sdk-go/asserter"
	"github.com/coinbase/mesh-sdk-go/examples/server/services"
	"github.com/coinbase/mesh-sdk-go/server"
	"github.com/coinbase/mesh-sdk-go/types"
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
		Blockchain: "Mesh",
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

* [Types](types): Auto-generated Mesh types
* [Client](client): Low-level communication with any Mesh server
* [Server](server): Simplified Mesh API server development
* [Asserter](asserter): Validation of Mesh types
* [Fetcher](fetcher): Simplified and validated communication with any Mesh server
* [Parser](parser): Tool for parsing Mesh blocks
* [Syncer](syncer): Sync Mesh blocks with customizable handling
* [Reconciler](reconciler): Compare derived balances with node balances
* [Keys](keys): Cryptographic operations for Mesh-supported curves
* [Constructor](constructor): Coordinate the construction and broadcast of transactions

These packages are demoed extensively in [examples](examples) and are utilized throughout the [mesh-cli](https://github.com/coinbase/mesh-cli) tool.

### Syncer

The core of any integration is syncing blocks reliably. The [syncer](https://github.com/coinbase/mesh-sdk-go/tree/master/syncer) serially processes blocks from a Data API implementation (automatically handling re-orgs) with user-defined handling logic and pluggable storage. After a block is processed, store it to a DB or send a push notification—the decision is up to you!

### Parser

When reading the operations in a block, it's helpful to apply higher-level groupings to related operations, or match operations in a transaction to some set of generic descriptions (i.e., ensure there are two operations of equal but opposite amounts). The [parser](https://github.com/coinbase/mesh-sdk-go/tree/master/parser) empowers any integrator to build abstractions on top of the [building blocks](https://www.mesh-api.org/docs/mesh_principles.html#universal-transactions) that the Mesh API exposes.

<!-- h3 Configuring the SDK -->
<!-- See the [Configuration](https://github.com/coinbase/mesh-geth-sdk/blob/master/configuration/configuration.go) file for more information on how to configure the SDK. -->

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

To validate `mesh-sdk-go`, [install `mesh-cli`](https://github.com/coinbase/mesh-cli#install) and run one of the following commands:

* `mesh-cli check:data --configuration-file mesh-cli-conf/testnet/config.json` - This command validates that the Data API implementation is correct, using the bitcoin `testnet` node. It also ensures that the implementation does not miss any balance-changing operations.
* `mesh-cli check:construction --configuration-file mesh-cli-conf/testnet/config.json` - This command validates the Construction API implementation. It also verifies transaction construction, signing, and submissions to the `testnet` network.
* `mesh-cli check:data --configuration-file mesh-cli-conf/mainnet/config.json` - This command validates that the Data API implementation is correct, using the bitcoin `mainnet` node. It also ensures that the implementation does not miss any balance-changing operations.

Read the [How to Test your Mesh Implementation](https://www.mesh-api.org/docs/mesh_test.html) documentation for additional details.

<!-- h2 Contributing Guide -->
## Contributing

You may contribute to the `mesh-sdk-go` project in various ways:

* [Asking Questions](CONTRIBUTING.md/#asking-questions)
* [Providing Feedback](CONTRIBUTING.md/#providing-feedback)
* [Reporting Issues](CONTRIBUTING.md/#reporting-issues)

Read our [Contributing](CONTRIBUTING.MD) documentation for more information.

When you've finished an implementation for a blockchain, share your work in the [ecosystem category of the community site](https://community.mesh-api.org/c/ecosystem). Platforms looking for implementations for certain blockchains will be monitoring this section of the website for high-quality implementations they can use for integration. Make sure that your implementation meets the [expectations](https://www.mesh-api.org/docs/node_deployment.html) of any implementation.

<!-- h2 Documentation links -->
## Documentation

You can find the Mesh API documentation at [mesh-api.org](https://www.mesh-api.org/docs/welcome.html). 

Check out the [Getting Started](https://www.mesh-api.org/docs/getting_started.html) section to start diving into Mesh. 

Our documentation is divided into the following sections:
<!-- See the sidebar menu at mesh-api.org for section info -->
* [Product Overview](https://www.mesh-api.org/docs/welcome.html)
* [Getting Started](https://www.mesh-api.org/docs/getting_started.html)
* [Mesh API Spec](https://www.mesh-api.org/docs/Reference.html)
* [Samples](https://www.mesh-api.org/docs/reference-implementations.html)
* [Testing](https://www.mesh-api.org/docs/mesh_cli.html)
* [Best Practices](https://www.mesh-api.org/docs/node_deployment.html)
* [Repositories](https://www.mesh-api.org/docs/mesh_specifications.html)

<!-- h2 Related Projects -->
## Related Projects

* [mesh-specifications](https://github.com/coinbase/mesh-specifications) — The `mesh-specifications` repository generates the SDK code in the `mesh-sdk-go` repository.
* [mesh-cli](https://github.com/coinbase/mesh-cli) — Use the `mesh-cli` tool to test your Mesh API implementation. The tool also provides the ability to look up block contents and account balances.
* [mesh-geth-sdk](https://github.com/coinbase/mesh-geth-sdk) – The `mesh-geth-sdk` repository provides a collection of packages used for interaction with the Mesh API specification. The goal of this SDK is to help accelerate Mesh API implementation on go-ethereum based chains.
<!-- h3 Sample Implementations -->
### Sample Implementations

To help you with examples, we developed complete Mesh API sample implementations for [Bitcoin](https://github.com/coinbase/mesh-bitcoin) and [Ethereum](https://github.com/coinbase/mesh-ethereum). Developers of Bitcoin-like or Ethereum-like blockchains may find it easier to fork these implementation samples than to write an implementation from scratch.

You can also find community implementations for a variety of blockchains in the [mesh-ecosystem](https://github.com/coinbase/mesh-ecosystem) repository, and in the [ecosystem category](https://community.mesh-api.org/c/ecosystem) of our community site. 
<!-- h2 License -->
## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).
<!-- Copyright {© year Coinbase} -->
© 2022 Coinbase