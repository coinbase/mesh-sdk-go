# Reconciler

[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield)](https://pkg.go.dev/github.com/coinbase/mesh-sdk-go/reconciler?tab=doc)

The Reconciler package is used to ensure that balance changes derived from
parsing Mesh blocks are equivalent to the balance changes computed by the
node. If you want to see an example of how to use this package, take
a look at [mesh-cli](https://github.com/coinbase/mesh-cli).

## Features
* Customizable `Helper` and `Handler` to define your own logic for retrieving
calculated balance changes and handling successful/unsuccessful comparisons
* Perform balance lookup at either historical blocks or the current block (if
historical balance query is not supported)
* Provide a list of accounts to compare at each block (for quick and easy
debugging)

## Installation

```shell
go get github.com/coinbase/mesh-sdk-go/reconciler
```

## Reconciliation Strategies
### Active Addresses
The reconciler checks that the balance of an account computed by
its operations is equal to the balance of the account according
to the node. If this balance is not identical, the reconciler will
error.

### Inactive Addresses
The reconciler randomly checks the balances of accounts that aren't
involved in any transactions. The balances of accounts could change
on the blockchain node without being included in an operation
returned by the Mesh Data API. Recall that all balance-changing
operations must be returned by the Mesh Data API.
