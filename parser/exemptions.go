package parser

import (
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// findExemptions returns all matching *types.BalanceExemption
// for a particular *types.AccountIdentifier and *types.Currency.
func (p *Parser) findExemptions(
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

// MatchBalanceExemption returns a *types.BalanceExemption
// associated with the *types.AccountIdentifier, *types.Currency,
// and difference, if it exists.
func (p *Parser) MatchBalanceExemption(
	account *types.AccountIdentifier,
	currency *types.Currency,
	difference string, // live - computed
) *types.BalanceExemption {
	exemptions := p.findExemptions(account, currency)
	if len(exemptions) == 0 {
		return nil
	}

	// This should never error because we check for validity
	// before calling this method.
	bigDifference, ok := new(big.Int).SetString(difference, 10)
	if !ok {
		return nil
	}

	// Check if the reconciliation was exempt (supports compound exemptions)
	for _, exemption := range exemptions {
		if exemption.ExemptionType == types.BalanceDynamic ||
			(exemption.ExemptionType == types.BalanceGreaterOrEqual && bigDifference.Sign() >= 0) ||
			(exemption.ExemptionType == types.BalanceLessOrEqual && bigDifference.Sign() <= 0) {
			return exemption
		}
	}

	return nil
}
