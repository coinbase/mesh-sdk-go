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
	"context"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// BalanceChange represents a balance change that affected
// a *types.AccountIdentifier and a *types.Currency.
type BalanceChange struct {
	Account    *types.AccountIdentifier `json:"account_identifier,omitempty"`
	Currency   *types.Currency          `json:"currency,omitempty"`
	Block      *types.BlockIdentifier   `json:"block_identifier,omitempty"`
	Difference string                   `json:"difference,omitempty"`
}

// ExemptOperation is a function that returns a boolean indicating
// if the operation should be skipped eventhough it passes other
// checks indiciating it should be considered a balance change.
type ExemptOperation func(*types.Operation) bool

// skipOperation returns a boolean indicating whether
// an operation should be processed. An operation will
// not be processed if it is considered unsuccessful.
func (p *Parser) skipOperation(op *types.Operation) (bool, error) {
	successful, err := p.Asserter.OperationSuccessful(op)
	if err != nil {
		// Should only occur if responses not validated
		return false, fmt.Errorf(
			"failed to check the status of operation %s: %w",
			types.PrintStruct(op),
			err,
		)
	}

	if !successful {
		return true, nil
	}

	if op.Account == nil {
		return true, nil
	}

	if op.Amount == nil {
		return true, nil
	}

	// In some cases, it may be desirable to exempt certain operations from
	// balance changes.
	if p.ExemptFunc != nil && p.ExemptFunc(op) {
		return true, nil
	}

	return false, nil
}

// BalanceChanges returns all balance changes for
// a particular block. All balance changes for a
// particular account are summed into a single
// BalanceChanges struct. If a block is being
// orphaned, the opposite of each balance change is
// returned.
func (p *Parser) BalanceChanges(
	ctx context.Context,
	block *types.Block,
	blockRemoved bool,
) ([]*BalanceChange, error) {
	balanceChanges := map[string]*BalanceChange{}
	for _, tx := range block.Transactions {
		for _, op := range tx.Operations {
			skip, err := p.skipOperation(op)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to skip operation %s: %w",
					types.PrintStruct(op),
					err,
				)
			}
			if skip {
				continue
			}

			// We create a copy of Amount.Value
			// here to ensure we don't accidentally overwrite
			// the value of op.Amount.
			amountValue := op.Amount.Value
			blockIdentifier := block.BlockIdentifier
			if blockRemoved {
				negatedValue, err := types.NegateValue(amountValue)
				if err != nil {
					return nil, fmt.Errorf("failed to flip the sign of %s: %w", amountValue, err)
				}
				amountValue = negatedValue
			}

			// Merge values by account and currency
			key := fmt.Sprintf(
				"%s/%s",
				types.Hash(op.Account),
				types.Hash(op.Amount.Currency),
			)

			val, ok := balanceChanges[key]
			if !ok {
				balanceChanges[key] = &BalanceChange{
					Account:    op.Account,
					Currency:   op.Amount.Currency,
					Difference: amountValue,
					Block:      blockIdentifier,
				}
				continue
			}

			newDifference, err := types.AddValues(val.Difference, amountValue)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to add %s and %s: %w",
					val.Difference,
					amountValue,
					err,
				)
			}
			val.Difference = newDifference
			balanceChanges[key] = val
		}
	}

	i := 0
	allChanges := make([]*BalanceChange, len(balanceChanges))
	for _, change := range balanceChanges {
		allChanges[i] = change
		i++
	}

	return allChanges, nil
}
