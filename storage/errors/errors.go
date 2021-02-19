// Copyright 2020 Coinbase, Inc.
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
	ErrDatabaseOpenFailed         = errors.New("unable to open database")
	ErrCompressorLoadFailed       = errors.New("unable to load compressor")
	ErrDBCloseFailed              = errors.New("unable to close database")
	ErrCommitFailed               = errors.New("unable to commit transaction")
	ErrScanGetValueFailed         = errors.New("unable to get value for key")
	ErrScanWorkerFailed           = errors.New("worker failed")
	ErrDecompressFailed           = errors.New("unable to decompress")
	ErrDecompressSaveUnsuccessful = errors.New("unable to store decompressed file")
	ErrLoadFileUnsuccessful       = errors.New("unable to load file")
	ErrCompressNormalFailed       = errors.New("unable to compress normal")
	ErrCompressWithDictFailed     = errors.New("unable to compress with dictionary")
	ErrDecompressWithDictFailed   = errors.New("unable to decompress with dictionary")
	ErrDecompressOutputMismatch   = errors.New("decompressed dictionary output does not match")
	ErrRecompressFailed           = errors.New("unable to recompress")
	ErrCreateTempDirectoryFailed  = errors.New("unable to create temporary directory")
	ErrMaxEntries                 = errors.New("max entries reached")
	ErrScanFailed                 = errors.New("unable to scan")
	ErrNoEntriesFoundInNamespace  = errors.New("found 0 entries for namespace")
	ErrInvokeZSTDFailed           = errors.New("unable to start zstd")
	ErrTrainZSTDFailed            = errors.New("unable to train zstd")
	ErrWalkFilesFailed            = errors.New("unable to walk files")

	BadgerStorageErrs = []error{
		ErrDatabaseOpenFailed,
		ErrCompressorLoadFailed,
		ErrDBCloseFailed,
		ErrCommitFailed,
		ErrScanGetValueFailed,
		ErrScanWorkerFailed,
		ErrDecompressFailed,
		ErrDecompressSaveUnsuccessful,
		ErrLoadFileUnsuccessful,
		ErrCompressNormalFailed,
		ErrCompressWithDictFailed,
		ErrDecompressWithDictFailed,
		ErrDecompressOutputMismatch,
		ErrRecompressFailed,
		ErrCreateTempDirectoryFailed,
		ErrMaxEntries,
		ErrScanFailed,
		ErrNoEntriesFoundInNamespace,
		ErrInvokeZSTDFailed,
		ErrTrainZSTDFailed,
		ErrWalkFilesFailed,
	}
)

// Broadcast Storage Errors
var (
	ErrBroadcastTxStale     = errors.New("unable to handle stale transaction")
	ErrBroadcastTxConfirmed = errors.New(
		"unable to handle confirmed transaction",
	)
	ErrBroadcastFindTxFailed = errors.New(
		"unable to determine if transaction was seen",
	)
	ErrBroadcastEncodeUpdateFailed        = errors.New("unable to encode updated broadcast")
	ErrBroadcastUpdateFailed              = errors.New("unable to update broadcast")
	ErrBroadcastDeleteConfirmedTxFailed   = errors.New("unable to delete confirmed broadcast")
	ErrBroadcastInvokeBlockHandlersFailed = errors.New("unable to handle block")
	ErrBroadcastFailed                    = errors.New(
		"unable to broadcast pending transactions",
	)
	ErrBroadcastDBGetFailed = errors.New(
		"unable to determine if already broadcasting transaction",
	)
	ErrBroadcastAlreadyExists      = errors.New("already broadcasting transaction")
	ErrBroadcastEncodeFailed       = errors.New("unable to encode broadcast")
	ErrBroadcastSetFailed          = errors.New("unable to set broadcast")
	ErrBroadcastScanFailed         = errors.New("unable to scan for all broadcasts")
	ErrBroadcastDecodeFailed       = errors.New("unable to decode broadcast")
	ErrBroadcastCommitUpdateFailed = errors.New("unable to commit broadcast update")
	ErrBroadcastIdentifierMismatch = errors.New(
		"unexpected transaction hash returned by broadcast",
	)
	ErrBroadcastGetCurrentBlockIdentifierFailed = errors.New(
		"unable to get current block identifier",
	)
	ErrBroadcastAtTipFailed               = errors.New("unable to determine if at tip")
	ErrBroadcastGetAllFailed              = errors.New("unable to get all broadcasts")
	ErrBroadcastDeleteFailed              = errors.New("unable to delete broadcast")
	ErrBroadcastHandleFailureUnsuccessful = errors.New("unable to handle broadcast failure")
	ErrBroadcastCommitDeleteFailed        = errors.New("unable to commit broadcast delete")
	ErrBroadcastPerformFailed             = errors.New("unable to perform broadcast")

	BroadcastStorageErrs = []error{
		ErrBroadcastTxStale,
		ErrBroadcastTxConfirmed,
		ErrBroadcastFindTxFailed,
		ErrBroadcastEncodeUpdateFailed,
		ErrBroadcastUpdateFailed,
		ErrBroadcastDeleteConfirmedTxFailed,
		ErrBroadcastInvokeBlockHandlersFailed,
		ErrBroadcastFailed,
		ErrBroadcastDBGetFailed,
		ErrBroadcastAlreadyExists,
		ErrBroadcastEncodeFailed,
		ErrBroadcastSetFailed,
		ErrBroadcastScanFailed,
		ErrBroadcastDecodeFailed,
		ErrBroadcastCommitUpdateFailed,
		ErrBroadcastIdentifierMismatch,
		ErrBroadcastGetCurrentBlockIdentifierFailed,
		ErrBroadcastAtTipFailed,
		ErrBroadcastGetAllFailed,
		ErrBroadcastDeleteFailed,
		ErrBroadcastHandleFailureUnsuccessful,
		ErrBroadcastCommitDeleteFailed,
		ErrBroadcastPerformFailed,
	}
)

// Coin Storage Errors
var (
	ErrCoinQueryFailed                  = errors.New("unable to query for coin")
	ErrCoinDecodeFailed                 = errors.New("unable to decode coin")
	ErrCoinGetFailed                    = errors.New("unable to get coin")
	ErrCoinAddFailed                    = errors.New("unable to add coin")
	ErrReconciliationUpdateCommitFailed = errors.New("unable to commit last reconciliation update")
	ErrCoinDataEncodeFailed             = errors.New("unable to encode coin data")
	ErrCoinStoreFailed                  = errors.New("unable to store coin")
	ErrAccountCoinStoreFailed           = errors.New("unable to store account coin")
	ErrAccountCoinQueryFailed           = errors.New("unable to query coins for account")
	ErrCoinDeleteFailed                 = errors.New("unable to delete coin")
	ErrOperationParseFailed             = errors.New("unable to parse operation success")
	ErrUnableToDetermineIfSkipOperation = errors.New(
		"unable to to determine if should skip operation",
	)
	ErrDuplicateCoinFound           = errors.New("duplicate coin found")
	ErrCoinRemoveFailed             = errors.New("unable to remove coin")
	ErrAccountIdentifierQueryFailed = errors.New("unable to query account identifier")
	ErrCurrentBlockGetFailed        = errors.New("unable to get current block identifier")
	ErrCoinLookupFailed             = errors.New("unable to lookup coin")
	ErrUTXOBalanceGetFailed         = errors.New("unable to get utxo balance")
	ErrCoinParseFailed              = errors.New("unable to parse amount for coin")
	ErrCoinImportFailed             = errors.New("unable to import coins")
	ErrCoinNotFound                 = errors.New("coin not found")

	CoinStorageErrs = []error{
		ErrCoinQueryFailed,
		ErrCoinDecodeFailed,
		ErrCoinGetFailed,
		ErrCoinAddFailed,
		ErrReconciliationUpdateCommitFailed,
		ErrCoinDataEncodeFailed,
		ErrCoinStoreFailed,
		ErrAccountCoinStoreFailed,
		ErrAccountCoinQueryFailed,
		ErrCoinDeleteFailed,
		ErrOperationParseFailed,
		ErrUnableToDetermineIfSkipOperation,
		ErrDuplicateCoinFound,
		ErrCoinRemoveFailed,
		ErrAccountIdentifierQueryFailed,
		ErrCurrentBlockGetFailed,
		ErrCoinLookupFailed,
		ErrUTXOBalanceGetFailed,
		ErrCoinParseFailed,
		ErrCoinImportFailed,
		ErrCoinNotFound,
	}
)

// Compressor Errors
var (
	ErrLoadDictFailed      = errors.New("unable to load dictionary")
	ErrObjectEncodeFailed  = errors.New("unable to encode object")
	ErrRawCompressFailed   = errors.New("unable to compress raw bytes")
	ErrRawDecompressFailed = errors.New("unable to decompress raw bytes")
	ErrRawDecodeFailed     = errors.New("unable to decode bytes")
	ErrBufferWriteFailed   = errors.New("unable to write to buffer")
	ErrWriterCloseFailed   = errors.New("unable to close writer")
	ErrObjectDecodeFailed  = errors.New("unable to decode object")
	ErrReaderCloseFailed   = errors.New("unable to close reader")
	ErrCopyBlockFailed     = errors.New("unable to copy block")

	CompressorErrs = []error{
		ErrLoadDictFailed,
		ErrObjectEncodeFailed,
		ErrRawCompressFailed,
		ErrRawDecompressFailed,
		ErrRawDecodeFailed,
		ErrBufferWriteFailed,
		ErrWriterCloseFailed,
		ErrObjectDecodeFailed,
		ErrReaderCloseFailed,
		ErrCopyBlockFailed,
	}
)

// Job Storage Errors
var (
	ErrJobsGetAllFailed              = errors.New("unable to get all jobs")
	ErrJobIdentifierDecodeFailed     = errors.New("unable to decode existing identifier")
	ErrJobGetFailed                  = errors.New("unable to get job")
	ErrJobIdentifierEncodeFailed     = errors.New("unable to encode job identifier")
	ErrJobIdentifierUpdateFailed     = errors.New("unable to update job identifier")
	ErrJobIdentifiersEncodeAllFailed = errors.New("unable to encode identifiers")
	ErrJobIdentifiersSetAllFailed    = errors.New("unable to set identifiers")
	ErrJobIdentifierRemoveFailed     = errors.New("unable to remove identifier")
	ErrJobIdentifierNotFound         = errors.New("identifier not found")
	ErrJobRemoveFailed               = errors.New("unable to remove job")
	ErrJobAddFailed                  = errors.New("unable to add job")
	ErrJobIdentifierGetFailed        = errors.New("unable to get next identifier")
	ErrJobUpdateOldFailed            = errors.New("unable to update terminal job")
	ErrJobEncodeFailed               = errors.New("unable to encode job")
	ErrJobUpdateFailed               = errors.New("unable to update job")
	ErrJobMetadataUpdateFailed       = errors.New("unable to update metadata")
	ErrJobDoesNotExist               = errors.New("job does not exist")
	ErrJobDecodeFailed               = errors.New("unable to decode job")

	JobStorageErrs = []error{
		ErrJobsGetAllFailed,
		ErrJobIdentifierDecodeFailed,
		ErrJobGetFailed,
		ErrJobIdentifierEncodeFailed,
		ErrJobIdentifierUpdateFailed,
		ErrJobIdentifiersEncodeAllFailed,
		ErrJobIdentifiersSetAllFailed,
		ErrJobIdentifierRemoveFailed,
		ErrJobIdentifierNotFound,
		ErrJobRemoveFailed,
		ErrJobAddFailed,
		ErrJobIdentifierGetFailed,
		ErrJobUpdateOldFailed,
		ErrJobEncodeFailed,
		ErrJobUpdateFailed,
		ErrJobMetadataUpdateFailed,
		ErrJobDoesNotExist,
		ErrJobDecodeFailed,
	}
)

// Key Storage Errors
var (
	// ErrAddrExists is returned when key storage already
	// contains an address.
	ErrAddrExists = errors.New("address already exists")

	ErrAddrCheckIfExistsFailed  = errors.New("unable to check if address exists")
	ErrSerializeKeyFailed       = errors.New("unable to serialize key")
	ErrStoreKeyFailed           = errors.New("unable to store key")
	ErrCommitKeyFailed          = errors.New("unable to commit new key to db")
	ErrAddrGetFailed            = errors.New("unable to get address")
	ErrAddrNotFound             = errors.New("address not found")
	ErrParseSavedKeyFailed      = errors.New("unable to parse saved key")
	ErrKeyScanFailed            = errors.New("database scan for keys failed")
	ErrParseKeyPairFailed       = errors.New("unable to parse key pair")
	ErrKeyGetFailed             = errors.New("unable to get key")
	ErrSignerCreateFailed       = errors.New("unable to create signer")
	ErrDetermineSigTypeFailed   = errors.New("cannot determine signature type for payload")
	ErrSignPayloadFailed        = errors.New("unable to to sign payload")
	ErrAddrsGetAllFailed        = errors.New("unable to get addresses")
	ErrNoAddrAvailable          = errors.New("no addresses available")
	ErrAddrImportFailed         = errors.New("unable to import prefunded account")
	ErrPrefundedAcctStoreFailed = errors.New("unable to store prefunded account")
	ErrRandomAddress            = errors.New("cannot select random address")

	KeyStorageErrs = []error{
		ErrAddrExists,
		ErrAddrCheckIfExistsFailed,
		ErrSerializeKeyFailed,
		ErrStoreKeyFailed,
		ErrCommitKeyFailed,
		ErrAddrGetFailed,
		ErrAddrNotFound,
		ErrParseSavedKeyFailed,
		ErrKeyScanFailed,
		ErrParseKeyPairFailed,
		ErrKeyGetFailed,
		ErrSignerCreateFailed,
		ErrDetermineSigTypeFailed,
		ErrSignPayloadFailed,
		ErrAddrsGetAllFailed,
		ErrNoAddrAvailable,
		ErrAddrImportFailed,
		ErrPrefundedAcctStoreFailed,
		ErrRandomAddress,
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

	ErrBlockGetFailed                  = errors.New("unable to get block")
	ErrTransactionGetFailed            = errors.New("could not get transaction")
	ErrBlockEncodeFailed               = errors.New("unable to encode block")
	ErrBlockStoreFailed                = errors.New("unable to store block")
	ErrBlockIndexStoreFailed           = errors.New("unable to store block index")
	ErrBlockIdentifierUpdateFailed     = errors.New("unable to update head block identifier")
	ErrBlockCopyFailed                 = errors.New("unable to copy block")
	ErrTransactionHashStoreFailed      = errors.New("unable to store transaction hash")
	ErrBlockDeleteFailed               = errors.New("unable to delete block")
	ErrBlockIndexDeleteFailed          = errors.New("unable to delete block index")
	ErrHeadBlockIdentifierUpdateFailed = errors.New("unable to update head block identifier")
	ErrLastProcessedBlockPrecedesStart = errors.New(
		"last processed block is less than start index",
	)
	ErrTransactionHashContentsDecodeFailed = errors.New(
		"could not decode transaction hash contents",
	)
	ErrTransactionDataEncodeFailed = errors.New("unable to encode transaction data")
	ErrTransactionDeleteFailed     = errors.New("could not remove transaction")
	ErrTransactionHashNotFound     = errors.New(
		"saved blocks at transaction does not contain transaction hash",
	)
	ErrTransactionDBQueryFailed = errors.New("unable to query database for transaction")
	ErrBlockDataDecodeFailed    = errors.New(
		"unable to decode block data for transaction",
	)
	ErrTransactionNotFound            = errors.New("unable to find transaction")
	ErrTransactionDoesNotExistInBlock = errors.New("transaction does not exist in block")
	ErrHeadBlockGetFailed             = errors.New("unable to get head block")
	ErrOldestIndexUpdateFailed        = errors.New("oldest index update failed")
	ErrOldestIndexMissing             = errors.New("oldest index missing")
	ErrOldestIndexRead                = errors.New("cannot read oldest index")
	ErrCannotRemoveOldest             = errors.New("cannot remove oldest index")
	ErrCannotAccessPrunedData         = errors.New("cannot access pruned data")
	ErrNothingToPrune                 = errors.New("nothing to prune")
	ErrPruningFailed                  = errors.New("pruning failed")
	ErrCannotPruneTransaction         = errors.New("cannot prune transaction")
	ErrCannotStoreBackwardRelation    = errors.New("cannot store backward relation")
	ErrCannotRemoveBackwardRelation   = errors.New("cannot remove backward relation")

	BlockStorageErrs = []error{
		ErrHeadBlockNotFound,
		ErrBlockNotFound,
		ErrDuplicateKey,
		ErrDuplicateTransactionHash,
		ErrBlockGetFailed,
		ErrTransactionGetFailed,
		ErrBlockEncodeFailed,
		ErrBlockStoreFailed,
		ErrBlockIndexStoreFailed,
		ErrBlockIdentifierUpdateFailed,
		ErrBlockCopyFailed,
		ErrTransactionHashStoreFailed,
		ErrBlockDeleteFailed,
		ErrBlockIndexDeleteFailed,
		ErrHeadBlockIdentifierUpdateFailed,
		ErrLastProcessedBlockPrecedesStart,
		ErrTransactionHashContentsDecodeFailed,
		ErrTransactionDataEncodeFailed,
		ErrTransactionDeleteFailed,
		ErrTransactionHashNotFound,
		ErrTransactionDBQueryFailed,
		ErrBlockDataDecodeFailed,
		ErrTransactionNotFound,
		ErrTransactionDoesNotExistInBlock,
		ErrHeadBlockGetFailed,
		ErrOldestIndexUpdateFailed,
		ErrOldestIndexMissing,
		ErrOldestIndexRead,
		ErrCannotRemoveOldest,
		ErrCannotAccessPrunedData,
		ErrNothingToPrune,
		ErrPruningFailed,
		ErrCannotPruneTransaction,
		ErrCannotStoreBackwardRelation,
		ErrCannotRemoveBackwardRelation,
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
