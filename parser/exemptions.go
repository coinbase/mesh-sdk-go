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
	bigDifference, ok := new(big.Int).SetString(difference, 10) // nolint
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
