package parser

import (
	"math/big"

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

// MatchBalanceExemption returns a *types.BalanceExemption
// associated with the *types.AccountIdentifier, *types.Currency,
// and difference, if it exists. The provided exemptions
// should be produced using FindExemptions.
func MatchBalanceExemption(
	matchedExemptions []*types.BalanceExemption,
	difference string, // live - computed
) *types.BalanceExemption {
	bigDifference, ok := new(big.Int).SetString(difference, 10)
	if !ok {
		return nil
	}

	for _, exemption := range matchedExemptions {
		if exemption.ExemptionType == types.BalanceDynamic ||
			(exemption.ExemptionType == types.BalanceGreaterOrEqual && bigDifference.Sign() >= 0) ||
			(exemption.ExemptionType == types.BalanceLessOrEqual && bigDifference.Sign() <= 0) {
			return exemption
		}
	}

	return nil
}
