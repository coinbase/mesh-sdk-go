// Copyright 2024 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errors

import (
	"errors"

	utils "github.com/coinbase/rosetta-sdk-go/errors"
)

// Badger Storage Errors
var (
	ErrDBCloseFailed             = errors.New("unable to close database")
	ErrScanGetValueFailed        = errors.New("unable to get value for key")
	ErrScanWorkerFailed          = errors.New("worker failed")
	ErrDecompressOutputMismatch  = errors.New("decompressed dictionary output does not match")
	ErrMaxEntries                = errors.New("max entries reached")
	ErrScanFailed                = errors.New("unable to scan")
	ErrNoEntriesFoundInNamespace = errors.New("found 0 entries for namespace")

	BadgerStorageErrs = []error{
		ErrScanGetValueFailed,
		ErrScanWorkerFailed,
		ErrDecompressOutputMismatch,
		ErrMaxEntries,
		ErrScanFailed,
		ErrNoEntriesFoundInNamespace,
	}
)

// Broadcast Storage Errors
var (
	ErrBroadcastAlreadyExists      = errors.New("already broadcasting transaction")
	ErrBroadcastCommitUpdateFailed = errors.New("unable to commit broadcast update")
	ErrBroadcastIdentifierMismatch = errors.New(
		"unexpected transaction hash returned by broadcast",
	)

	BroadcastStorageErrs = []error{
		ErrBroadcastAlreadyExists,
		ErrBroadcastCommitUpdateFailed,
		ErrBroadcastIdentifierMismatch,
	}
)

// Coin Storage Errors
var (
	ErrDuplicateCoinFound = errors.New("duplicate coin found")
	ErrCoinParseFailed    = errors.New("unable to parse amount for coin")
	ErrCoinNotFound       = errors.New("coin not found")

	CoinStorageErrs = []error{
		ErrDuplicateCoinFound,
		ErrCoinParseFailed,
		ErrCoinNotFound,
	}
)

// Compressor Errors
var (
	ErrWriterCloseFailed  = errors.New("unable to close writer")
	ErrObjectDecodeFailed = errors.New("unable to decode object")
	ErrCopyBlockFailed    = errors.New("unable to copy block")
	ErrRawDecodeFailed    = errors.New("unable to decode raw bytes")

	CompressorErrs = []error{
		ErrWriterCloseFailed,
		ErrObjectDecodeFailed,
		ErrCopyBlockFailed,
		ErrRawDecodeFailed,
	}
)

// Job Storage Errors
var (
	ErrJobIdentifierNotFound = errors.New("identifier not found")
	ErrJobUpdateOldFailed    = errors.New("unable to update terminal job")
	ErrJobDoesNotExist       = errors.New("job does not exist")

	JobStorageErrs = []error{
		ErrJobIdentifierNotFound,
		ErrJobUpdateOldFailed,
		ErrJobDoesNotExist,
	}
)

// Key Storage Errors
var (
	// ErrAddrExists is returned when key storage already
	// contains an address.
	ErrAddrExists             = errors.New("key already exists")
	ErrAddrNotFound           = errors.New("address not found")
	ErrParseKeyPairFailed     = errors.New("unable to parse key pair")
	ErrDetermineSigTypeFailed = errors.New("cannot determine signature type for payload")
	ErrNoAddrAvailable        = errors.New("no addresses available")

	KeyStorageErrs = []error{
		ErrAddrExists,
		ErrAddrNotFound,
		ErrParseKeyPairFailed,
		ErrDetermineSigTypeFailed,
		ErrNoAddrAvailable,
	}
)

// Balance Storage Errors
var (
	// ErrNegativeBalance is returned when an account
	// balance goes negative as the result of an operation.
	ErrNegativeBalance = errors.New("negative balance")

	// ErrInvalidLiveBalance is returned when an account's
	// live balance varies in a way that is inconsistent
	// with any balance exemption.
	ErrInvalidLiveBalance = errors.New("invalid live balance")

	// ErrBalancePruned is returned when the caller attempts
	// to retrieve a pruned balance.
	ErrBalancePruned = errors.New("balance pruned")

	// ErrBlockNil is returned when the block to lookup
	// a balance at is nil.
	ErrBlockNil = errors.New("block nil")

	// ErrAccountMissing is returned when a fetched
	// account does not exist.
	ErrAccountMissing = errors.New("account missing")

	// ErrInvalidChangeValue is returned when the change value
	// cannot be parsed.
	ErrInvalidChangeValue = errors.New("invalid change value")

	// ErrInvalidValue is returned when the value we are trying
	// to save cannot be parsed.
	ErrInvalidValue = errors.New("invalid value")

	ErrHelperHandlerMissing = errors.New("balance storage helper or handler is missing")

	ErrInvalidCurrency = errors.New("invalid currency")

	BalanceStorageErrs = []error{
		ErrNegativeBalance,
		ErrInvalidLiveBalance,
		ErrBalancePruned,
		ErrBlockNil,
		ErrAccountMissing,
		ErrInvalidChangeValue,
		ErrInvalidValue,
		ErrHelperHandlerMissing,
	}
)

// Block Storage Errors
var (
	// ErrHeadBlockNotFound is returned when there is no
	// head block found in BlockStorage.
	ErrHeadBlockNotFound = errors.New("head block not found")

	// ErrBlockNotFound is returned when a block is not
	// found in BlockStorage.
	ErrBlockNotFound = errors.New("block not found")

	// ErrDuplicateKey is returned when a key
	// cannot be stored because it is a duplicate.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrDuplicateTransactionHash is returned when a transaction
	// hash cannot be stored because it is a duplicate.
	ErrDuplicateTransactionHash = errors.New("duplicate transaction hash")

	ErrLastProcessedBlockPrecedesStart = errors.New(
		"last processed block is less than start index",
	)
	ErrTransactionHashContentsDecodeFailed = errors.New(
		"could not decode transaction hash contents",
	)
	ErrTransactionDeleteFailed = errors.New("could not remove transaction")
	ErrTransactionHashNotFound = errors.New(
		"saved blocks at transaction does not contain transaction hash",
	)
	ErrBlockDataDecodeFailed = errors.New(
		"unable to decode block data for transaction",
	)
	ErrTransactionNotFound            = errors.New("unable to find transaction")
	ErrTransactionDoesNotExistInBlock = errors.New("transaction does not exist in block")
	ErrOldestIndexMissing             = errors.New("oldest index missing")
	ErrCannotRemoveOldest             = errors.New("cannot remove oldest index")
	ErrCannotAccessPrunedData         = errors.New("cannot access pruned data")
	ErrNothingToPrune                 = errors.New("nothing to prune")

	BlockStorageErrs = []error{
		ErrHeadBlockNotFound,
		ErrBlockNotFound,
		ErrDuplicateKey,
		ErrDuplicateTransactionHash,
		ErrLastProcessedBlockPrecedesStart,
		ErrTransactionHashContentsDecodeFailed,
		ErrTransactionDeleteFailed,
		ErrTransactionHashNotFound,
		ErrBlockDataDecodeFailed,
		ErrTransactionNotFound,
		ErrTransactionDoesNotExistInBlock,
		ErrOldestIndexMissing,
		ErrCannotRemoveOldest,
		ErrCannotAccessPrunedData,
		ErrNothingToPrune,
	}
)

// Err takes an error as an argument and returns
// whether or not the error is one thrown by the storage
// along with the specific source of the error
func Err(err error) (bool, string) {
	storageErrs := map[string][]error{
		"balance storage error":   BalanceStorageErrs,
		"block storage error":     BlockStorageErrs,
		"coin storage error":      CoinStorageErrs,
		"key storage error":       KeyStorageErrs,
		"badger storage error":    BadgerStorageErrs,
		"compressor error":        CompressorErrs,
		"job storage error":       JobStorageErrs,
		"broadcast storage error": BroadcastStorageErrs,
	}

	for key, val := range storageErrs {
		if utils.FindError(val, err) {
			return true, key
		}
	}
	return false, ""
}
