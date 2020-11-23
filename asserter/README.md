# Asserter

[![GoDoc](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=shield)](https://pkg.go.dev/github.com/coinbase/rosetta-sdk-go/asserter?tab=doc)

The Asserter package is used to validate the correctness of Rosetta types. It is
important to note that this validation only ensures that required fields are
populated, fields are in the correct format, and transaction operations only
contain types and statuses specified in the /network/status endpoint.

If you want more intensive validation, try running the
[Rosetta Validator](https://github.com/coinbase/rosetta-validator).

## Installation

```shell
go get github.com/coinbase/rosetta-sdk-go/asserter
```
