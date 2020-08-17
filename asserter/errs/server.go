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

package errs

import "errors"

var (
	ErrNoSupportedNetworks = errors.New("no supported networks")

	ErrSupportedNetworksDuplicate = errors.New("supported network duplicate")

	ErrRequestedNetworkNotSupported = errors.New("requestNetwork not supported")

	ErrAccountBalanceRequestIsNil = errors.New("AccountBalanceRequest is nil")

	ErrAccountBalanceRequestHistoricalBalanceLookupNotSupported = errors.New(
		"historical balance lookup is not supported",
	)

	ErrBlockRequestIsNil = errors.New("BlockRequest is nil")

	ErrBlockTransactionRequestIsNil = errors.New("BlockTransactionRequest is nil")

	ErrConstructionMetadataRequestIsNil = errors.New("ConstructionMetadataRequest is nil")

	ErrConstructionMetadataRequestOptionsIsNil = errors.New(
		"ConstructionMetadataRequest.Options is nil",
	)

	ErrConstructionSubmitRequestIsNil = errors.New("ConstructionSubmitRequest is nil")

	ErrConstructionSubmitRequestSignedTxEmpty = errors.New(
		"ConstructionSubmitRequest.SignedTransaction is empty",
	)

	ErrMempoolTransactionRequestIsNil = errors.New("MempoolTransactionRequest is nil")

	ErrMetadataRequestIsNil = errors.New("MetadataRequest is nil")

	ErrNetworkRequestIsNil = errors.New("NetworkRequest is nil")

	ErrConstructionDeriveRequestIsNil = errors.New("ConstructionDeriveRequest is nil")

	ErrConstructionPreprocessRequestIsNil = errors.New("ConstructionPreprocessRequest is nil")

	ErrConstructionPreprocessRequestSuggestedFeeMultiplierIsNeg = errors.New(
		"suggested fee multiplier cannot be less than 0",
	)

	ErrConstructionPayloadsRequestIsNil = errors.New("ConstructionPayloadsRequest is nil")

	ErrConstructionCombineRequestIsNil = errors.New("ConstructionCombineRequest is nil")

	ErrConstructionCombineRequestUnsignedTxEmpty = errors.New("UnsignedTransaction cannot be empty")

	ErrConstructionHashRequestIsNil = errors.New("ConstructionHashRequest is nil")

	ErrConstructionHashRequestSignedTxEmpty = errors.New("SignedTransaction cannot be empty")

	ErrConstructionParseRequestIsNil = errors.New("ConstructionParseRequest is nil")

	ErrConstructionParseRequestEmpty = errors.New("Transaction cannot be empty")
)
