package parser

import (
	"github.com/coinbase/rosetta-sdk-go/types"
)

// FindExemptions returns all matching *types.BalanceExemption
// for a particular *types.AccountIdentifier and *types.Currency.
func (p *Parser) FindExemptions(
	account *types.AccountIdentifier,
	currency *types.Currency,
) []*types.BalanceExemption {
	matches := []*types.BalanceExemption{}
	for _, exemption := range p.BalanceExemptions {
		if exemption.Currency != nil && types.Hash(currency) != types.Hash(exemption.Currency) {
			continue
		}

		if exemption.SubAccountAddress != nil &&
			(account.SubAccount == nil || *exemption.SubAccountAddress != account.SubAccount.Address) {
			continue
		}

		matches = append(matches, exemption)
	}

	return matches
}
