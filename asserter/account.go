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

	"github.com/coinbase/rosetta-sdk-go/models"
)

// containsAccountIdentifier returns a boolean indicating if a
// *models.AccountIdentifier is contained within a slice of
// *models.AccountIdentifier. The check for equality takes
// into account everything within the models.AccountIdentifier
// struct (including the SubAccountIdentifier).
func containsAccountIdentifier(
	identifiers []*models.AccountIdentifier,
	identifier *models.AccountIdentifier,
) bool {
	for _, ident := range identifiers {
		if reflect.DeepEqual(ident, identifier) {
			return true
		}
	}

	return false
}

// containsCurrency returns a boolean indicating if a
// *models.Currency is contained within a slice of
// *models.Currency. The check for equality takes
// into account everything within the models.Currency
// struct (including currency.Metadata).
func containsCurrency(currencies []*models.Currency, currency *models.Currency) bool {
	for _, curr := range currencies {
		if reflect.DeepEqual(curr, currency) {
			return true
		}
	}

	return false
}

// assertBalanceAmounts returns an error if a slice
// of models.Amount returned as an models.AccountIdentifier's
// balance is invalid. It is considered invalid if the same
// currency is returned multiple times (these shoould be
// consolidated) or if a models.Amount is considered invalid.
func assertBalanceAmounts(amounts []*models.Amount) error {
	currencies := make([]*models.Currency, 0)
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
// models.BlockIdentifier is invalid, if the same
// models.AccountIdentifier appears in multiple
// models.Balance structs (should be consolidated),
// or if a models.Balance is considered invalid.
func AccountBalance(
	block *models.BlockIdentifier,
	balances []*models.Balance,
) error {
	if err := BlockIdentifier(block); err != nil {
		return err
	}

	accounts := make([]*models.AccountIdentifier, 0)
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
