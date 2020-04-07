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

package asserter

import (
	"fmt"
	"reflect"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"
)

// containsAccountIdentifier returns a boolean indicating if a
// *rosetta.AccountIdentifier is contained within a slice of
// *rosetta.AccountIdentifier. The check for equality takes
// into account everything within the rosetta.AccountIdentifier
// struct (including the SubAccountIdentifier).
func containsAccountIdentifier(
	identifiers []*rosetta.AccountIdentifier,
	identifier *rosetta.AccountIdentifier,
) bool {
	for _, ident := range identifiers {
		if reflect.DeepEqual(ident, identifier) {
			return true
		}
	}

	return false
}

// containsCurrency returns a boolean indicating if a
// *rosetta.Currency is contained within a slice of
// *rosetta.Currency. The check for equality takes
// into account everything within the rosetta.Currency
// struct (including currency.Metadata).
func containsCurrency(currencies []*rosetta.Currency, currency *rosetta.Currency) bool {
	for _, curr := range currencies {
		if reflect.DeepEqual(curr, currency) {
			return true
		}
	}

	return false
}

// assertBalanceAmounts returns an error if a slice
// of rosetta.Amount returned as an rosetta.AccountIdentifier's
// balance is invalid. It is considered invalid if the same
// currency is returned multiple times (these shoould be
// consolidated) or if a rosetta.Amount is considered invalid.
func assertBalanceAmounts(amounts []*rosetta.Amount) error {
	currencies := make([]*rosetta.Currency, 0)
	for _, amount := range amounts {
		// Ensure a currency is used at most once in balance.Amounts
		if containsCurrency(currencies, amount.Currency) {
			return fmt.Errorf("currency %+v used in balance multiple times", amount.Currency)
		}
		currencies = append(currencies, amount.Currency)

		if err := Amount(amount); err != nil {
			return err
		}
	}

	return nil
}

// AccountBalance returns an error if the provided
// rosetta.BlockIdentifier is invalid, if the same
// rosetta.AccountIdentifier appears in multiple
// rosetta.Balance structs (should be consolidated),
// or if a rosetta.Balance is considered invalid.
func AccountBalance(
	block *rosetta.BlockIdentifier,
	balances []*rosetta.Balance,
) error {
	if err := BlockIdentifier(block); err != nil {
		return err
	}

	accounts := make([]*rosetta.AccountIdentifier, 0)
	for _, balance := range balances {
		if err := AccountIdentifier(balance.AccountIdentifier); err != nil {
			return err
		}

		// Ensure an account identifier is used at most once in a balance response
		if containsAccountIdentifier(accounts, balance.AccountIdentifier) {
			return fmt.Errorf(
				"account identifier %+v used in balance multiple times",
				balance.AccountIdentifier,
			)
		}
		accounts = append(accounts, balance.AccountIdentifier)

		if err := assertBalanceAmounts(balance.Amounts); err != nil {
			return err
		}
	}

	return nil
}
