package errs

import "errors"

var (
	ErrConstructionMetadataResponseIsNil = errors.New("ConstructionMetadataResponse cannot be nil")

	ErrConstructionMetadataResponseMetadataMissing = errors.New("Metadata is nil")

	ErrTxIdentifierResponseIsNil = errors.New("TransactionIdentifierResponse cannot be nil")

	ErrConstructionCombineResponseIsNil = errors.New("construction combine response cannot be nil")

	ErrSignedTxEmpty = errors.New("signed transaction cannot be empty")

	ErrConstructionDeriveResponseIsNil = errors.New("construction derive response cannot be nil")

	ErrConstructionDeriveResponseAddrEmpty = errors.New("address cannot be empty")

	ErrConstructionParseResponseIsNil = errors.New("construction parse response cannot be nil")

	ErrConstructionParseResponseOperationsEmpty = errors.New("operations cannot be empty")

	ErrConstructionParseResponseSignersEmptyOnSignedTx = errors.New(
		"signers cannot be empty on signed transaction",
	)

	ErrConstructionParseResponseSignersNonEmptyOnUnsignedTx = errors.New(
		"signers should be empty for unsigned txs",
	)

	ErrConstructionParseResponseSignerEmpty = errors.New("signer cannot be empty string")

	ErrConstructionPayloadsResponseIsNil = errors.New(
		"construction payloads response cannot be nil",
	)

	ErrConstructionPayloadsResponseUnsignedTxEmpty = errors.New(
		"unsigned transaction cannot be empty",
	)

	ErrConstructionPayloadsResponsePayloadsEmpty = errors.New("signing payloads cannot be empty")

	ErrPublicKeyIsNil = errors.New("PublicKey cannot be nil")

	ErrPublicKeyBytesEmpty = errors.New("public key bytes cannot be empty")

	ErrCurveTypeNotSupported = errors.New("not a supported CurveType")

	ErrSigningPayloadIsNil = errors.New("signing payload cannot be nil")

	ErrSigningPayloadAddrEmpty = errors.New("signing payload address cannot be empty")

	ErrSigningPayloadBytesEmpty = errors.New("signing payload bytes cannot be empty")

	ErrSignaturesEmpty = errors.New("signatures cannot be empty")

	ErrSignaturesReturnedSigMismatch = errors.New(
		"requested signature type does not match returned signature type",
	)

	ErrSignatureBytesEmpty = errors.New("signature bytes cannot be empty")

	ErrSignatureTypeNotSupported = errors.New("not a supported SignatureType")
)
