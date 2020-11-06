package asserter

import (
	"github.com/coinbase/rosetta-sdk-go/types"
)

// SearchTransactionsResponse ensures a
// *types.SearchTransactionsResponse is valid.
func (a *Asserter) SearchTransactionsResponse(
	response *types.SearchTransactionsResponse,
) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if response.NextOffset != nil && *response.NextOffset < 0 {
		return ErrNextOffsetInvalid
	}

	for _, blockTransaction := range response.Transactions {
		if err := BlockIdentifier(blockTransaction.BlockIdentifier); err != nil {
			return err
		}

		if err := a.Transaction(blockTransaction.Transaction); err != nil {
			return err
		}
	}

	return nil
}
