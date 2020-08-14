package asserter

import "errors"

var (
	ErrAmountValueMissing = errors.New("Amount.Value is missing")

	ErrAmountCurrencyIsNil = errors.New("Amount.Currency is nil")

	ErrAmountCurrencySymbolEmpty = errors.New("Amount.Currency.Symbol is empty")

	ErrAmountCurrencyHasNegDecimals = errors.New("Amount.Currency.Decimals must be >= 0")

	ErrOperationIdentifierIndexIsNil = errors.New("Operation.OperationIdentifier.Index invalid")

	ErrOperationIdentifierNetworkIndexInvalid = errors.New(
		"Operation.OperationIdentifier.NetworkIndex invalid",
	)

	ErrAccountIsNil = errors.New("Account is nil")

	ErrAccountAddrMissing = errors.New("Account.Address is missing")

	ErrAccountSubAccountAddrMissing = errors.New("Account.SubAccount.Address is missing")

	ErrOperationStatusMissing = errors.New("operation.Status is empty")

	ErrOperationIsNil = errors.New("Operation is nil")

	ErrOperationStatusNotEmptyForConstruction = errors.New(
		"operation.Status must be empty for construction",
	)

	ErrBlockIdentifierIsNil = errors.New("BlockIdentifier is nil")

	ErrBlockIdentifierHashMissing = errors.New("BlockIdentifier.Hash is missing")

	ErrBlockIdentifierIndexIsNeg = errors.New("BlockIdentifier.Index is negative")

	ErrPartialBlockIdentifierIsNil = errors.New("PartialBlockIdentifier is nil")

	ErrPartialBlockIdentifierFieldsNotSet = errors.New(
		"neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set",
	)

	ErrTxIdentifierIsNil = errors.New("TransactionIdentifier is nil")

	ErrTxIdentifierHashMissing = errors.New("TransactionIdentifier.Hash is missing")

	ErrOperationsEmptyForConstruction = errors.New("operations cannot be empty for construction")

	ErrTxIsNil = errors.New("Transaction is nil")

	ErrBlockIsNil = errors.New("Block is nil")

	ErrBlockHashEqualsParentBlockHash = errors.New(
		"BlockIdentifier.Hash == ParentBlockIdentifier.Hash",
	)

	ErrBlockIndexPrecedesParentBlockIndex = errors.New(
		"BlockIdentifier.Index <= ParentBlockIdentifier.Index",
	)
)
