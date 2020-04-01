[![Coinbase](https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master.svg?style=svg)](https://circleci.com/gh/coinbase/rosetta-sdk-go/tree/master)
# rosetta-sdk-go

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

## Versioning
- Rosetta version: 1.2.4
- Package version: 0.0.1

## Installation

```shell
go get github.com/coinbase/rosetta-sdk-go
```

## Automatic Assertion
When using the helper methods to access a Rosetta Server (in `fetcher/*.go`),
responses from the server are automatically checked for adherence to
the Rosetta Interface. For example, if a `BlockIdentifer` is returned without a
`Hash`, the fetch will fail. Take a look at the tests in `asserter/*_test.go`
if you are curious about what exactly is asserted.

_It is possible, but not recommended, to bypass this assertion using the
`unsafe` helper methods available in `fetcher/*.go`._

## License
This project is available open source under the terms of the [Apache 2.0 License](https://opensource.org/licenses/Apache-2.0).

© 2020 Coinbase
