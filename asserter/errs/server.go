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
