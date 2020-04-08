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
	"context"
	"errors"
	"testing"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"

	"github.com/stretchr/testify/assert"
)

func TestAmount(t *testing.T) {
	var tests = map[string]struct {
		amount *rosetta.Amount
		err    error
	}{
		"valid amount": {
			amount: &rosetta.Amount{
				Value: "100000",
				Currency: &rosetta.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: nil,
		},
		"valid negative amount": {
			amount: &rosetta.Amount{
				Value: "-100000",
				Currency: &rosetta.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: nil,
		},
		"nil amount": {
			amount: nil,
			err:    errors.New("Amount.Value is missing"),
		},
		"nil currency": {
			amount: &rosetta.Amount{
				Value: "100000",
			},
			err: errors.New("Amount.Currency is nil"),
		},
		"invalid non-number": {
			amount: &rosetta.Amount{
				Value: "blah",
				Currency: &rosetta.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: errors.New("Amount.Value not an integer blah"),
		},
		"invalid integer format": {
			amount: &rosetta.Amount{
				Value: "1.0",
				Currency: &rosetta.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: errors.New("Amount.Value not an integer 1.0"),
		},
		"invalid non-integer": {
			amount: &rosetta.Amount{
				Value: "1.1",
				Currency: &rosetta.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: errors.New("Amount.Value not an integer 1.1"),
		},
		"invalid symbol": {
			amount: &rosetta.Amount{
				Value: "11",
				Currency: &rosetta.Currency{
					Decimals: 1,
				},
			},
			err: errors.New("Amount.Currency.Symbol is empty"),
		},
		"invalid decimals": {
			amount: &rosetta.Amount{
				Value: "111",
				Currency: &rosetta.Currency{
					Symbol: "BTC",
				},
			},
			err: errors.New("Amount.Currency.Decimals must be > 0"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Amount(test.amount)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestOperationIdentifier(t *testing.T) {
	var (
		validNetworkIndex   = int64(1)
		invalidNetworkIndex = int64(-1)
	)

	var tests = map[string]struct {
		identifier *rosetta.OperationIdentifier
		index      int64
		err        error
	}{
		"valid identifier": {
			identifier: &rosetta.OperationIdentifier{
				Index: 0,
			},
			index: 0,
			err:   nil,
		},
		"nil identifier": {
			identifier: nil,
			index:      0,
			err:        errors.New("Operation.OperationIdentifier.Index invalid"),
		},
		"out-of-order index": {
			identifier: &rosetta.OperationIdentifier{
				Index: 0,
			},
			index: 1,
			err:   errors.New("Operation.OperationIdentifier.Index invalid"),
		},
		"valid identifer with network index": {
			identifier: &rosetta.OperationIdentifier{
				Index:        0,
				NetworkIndex: &validNetworkIndex,
			},
			index: 0,
			err:   nil,
		},
		"invalid identifer with network index": {
			identifier: &rosetta.OperationIdentifier{
				Index:        0,
				NetworkIndex: &invalidNetworkIndex,
			},
			index: 0,
			err:   errors.New("Operation.OperationIdentifier.NetworkIndex invalid"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := OperationIdentifier(test.identifier, test.index)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestAccountIdentifier(t *testing.T) {
	var tests = map[string]struct {
		identifier *rosetta.AccountIdentifier
		err        error
	}{
		"valid identifier": {
			identifier: &rosetta.AccountIdentifier{
				Address: "acct1",
			},
			err: nil,
		},
		"invalid address": {
			identifier: &rosetta.AccountIdentifier{
				Address: "",
			},
			err: errors.New("Account.Address is missing"),
		},
		"valid identifer with subaccount": {
			identifier: &rosetta.AccountIdentifier{
				Address: "acct1",
				SubAccount: &rosetta.SubAccountIdentifier{
					Address: "acct2",
				},
			},
			err: nil,
		},
		"invalid identifer with subaccount": {
			identifier: &rosetta.AccountIdentifier{
				Address: "acct1",
				SubAccount: &rosetta.SubAccountIdentifier{
					Address: "",
				},
			},
			err: errors.New("Account.SubAccount.Address is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := AccountIdentifier(test.identifier)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestOperation(t *testing.T) {
	var (
		validAmount = &rosetta.Amount{
			Value: "1000",
			Currency: &rosetta.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		validAccount = &rosetta.AccountIdentifier{
			Address: "test",
		}
	)

	var tests = map[string]struct {
		operation  *rosetta.Operation
		index      int64
		successful bool
		err        error
	}{
		"valid operation": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:    "PAYMENT",
				Status:  "SUCCESS",
				Account: validAccount,
				Amount:  validAmount,
			},
			index:      int64(1),
			successful: true,
			err:        nil,
		},
		"valid operation no account": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: "SUCCESS",
			},
			index:      int64(1),
			successful: true,
			err:        nil,
		},
		"nil operation": {
			operation: nil,
			index:     int64(1),
			err:       errors.New("Operation is nil"),
		},
		"invalid operation no account": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: "SUCCESS",
				Amount: validAmount,
			},
			index: int64(1),
			err:   errors.New("Account is nil"),
		},
		"invalid operation empty account": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:    "PAYMENT",
				Status:  "SUCCESS",
				Account: &rosetta.AccountIdentifier{},
				Amount:  validAmount,
			},
			index: int64(1),
			err:   errors.New("Account.Address is missing"),
		},
		"invalid operation invalid index": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: "SUCCESS",
			},
			index: int64(2),
			err:   errors.New("Operation.OperationIdentifier.Index invalid"),
		},
		"invalid operation invalid type": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "STAKE",
				Status: "SUCCESS",
			},
			index: int64(1),
			err:   errors.New("Operation.Type STAKE is invalid"),
		},
		"unsuccessful operation": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: "FAILURE",
			},
			index:      int64(1),
			successful: false,
			err:        nil,
		},
		"invalid operation invalid status": {
			operation: &rosetta.Operation{
				OperationIdentifier: &rosetta.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: "DEFERRED",
			},
			index: int64(1),
			err:   errors.New("Operation.Status DEFERRED is invalid"),
		},
	}

	for name, test := range tests {
		asserter, err := New(
			context.Background(),
			&rosetta.NetworkStatusResponse{
				NetworkStatus: []*rosetta.NetworkStatus{
					{
						NetworkInformation: &rosetta.NetworkInformation{
							GenesisBlockIdentifier: &rosetta.BlockIdentifier{
								Index: 0,
							},
						},
					},
				},
				Options: &rosetta.Options{
					OperationStatuses: []*rosetta.OperationStatus{
						{
							Status:     "SUCCESS",
							Successful: true,
						},
						{
							Status:     "FAILURE",
							Successful: false,
						},
					},
					OperationTypes: []string{
						"PAYMENT",
					},
				},
			},
		)
		assert.NoError(t, err)
		t.Run(name, func(t *testing.T) {
			err := asserter.Operation(test.operation, test.index)
			assert.Equal(t, test.err, err)
			if err == nil {
				successful, err := asserter.OperationSuccessful(test.operation)
				assert.NoError(t, err)
				assert.Equal(t, test.successful, successful)
			}
		})
	}
}

func TestBlock(t *testing.T) {
	validBlockIdentifier := &rosetta.BlockIdentifier{
		Hash:  "blah",
		Index: 100,
	}
	validParentBlockIdentifier := &rosetta.BlockIdentifier{
		Hash:  "blah parent",
		Index: 99,
	}
	validTransaction := &rosetta.Transaction{
		TransactionIdentifier: &rosetta.TransactionIdentifier{
			Hash: "blah",
		},
	}
	var tests = map[string]struct {
		block        *rosetta.Block
		genesisIndex int64
		err          error
	}{
		"valid block": {
			block: &rosetta.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             1,
				Transactions:          []*rosetta.Transaction{validTransaction},
			},
			err: nil,
		},
		"genesis block": {
			block: &rosetta.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validBlockIdentifier,
				Timestamp:             1,
				Transactions:          []*rosetta.Transaction{validTransaction},
			},
			genesisIndex: validBlockIdentifier.Index,
			err:          nil,
		},
		"nil block": {
			block: nil,
			err:   errors.New("block is nil"),
		},
		"nil block hash": {
			block: &rosetta.Block{
				BlockIdentifier:       nil,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             1,
				Transactions:          []*rosetta.Transaction{validTransaction},
			},
			err: errors.New("BlockIdentifier.Hash is missing"),
		},
		"invalid block hash": {
			block: &rosetta.Block{
				BlockIdentifier:       &rosetta.BlockIdentifier{},
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             1,
				Transactions:          []*rosetta.Transaction{validTransaction},
			},
			err: errors.New("BlockIdentifier.Hash is missing"),
		},
		"block previous hash missing": {
			block: &rosetta.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: &rosetta.BlockIdentifier{},
				Timestamp:             1,
				Transactions:          []*rosetta.Transaction{validTransaction},
			},
			err: errors.New("BlockIdentifier.Hash is missing"),
		},
		"invalid parent block index": {
			block: &rosetta.Block{
				BlockIdentifier: validBlockIdentifier,
				ParentBlockIdentifier: &rosetta.BlockIdentifier{
					Hash:  validParentBlockIdentifier.Hash,
					Index: validBlockIdentifier.Index,
				},
				Timestamp:    1,
				Transactions: []*rosetta.Transaction{validTransaction},
			},
			err: errors.New("Block.BlockIdentifier.Index <= Block.ParentBlockIdentifier.Index"),
		},
		"invalid parent block hash": {
			block: &rosetta.Block{
				BlockIdentifier: validBlockIdentifier,
				ParentBlockIdentifier: &rosetta.BlockIdentifier{
					Hash:  validBlockIdentifier.Hash,
					Index: validParentBlockIdentifier.Index,
				},
				Timestamp:    1,
				Transactions: []*rosetta.Transaction{validTransaction},
			},
			err: errors.New("Block.BlockIdentifier.Hash == Block.ParentBlockIdentifier.Hash"),
		},
		"invalid block timestamp": {
			block: &rosetta.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Transactions:          []*rosetta.Transaction{validTransaction},
			},
			err: errors.New("Timestamp is invalid 0"),
		},
		"invalid block transaction": {
			block: &rosetta.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             1,
				Transactions: []*rosetta.Transaction{
					{},
				},
			},
			err: errors.New("Transaction.TransactionIdentifier.Hash is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := New(
				context.Background(),
				&rosetta.NetworkStatusResponse{
					NetworkStatus: []*rosetta.NetworkStatus{
						{
							NetworkInformation: &rosetta.NetworkInformation{
								GenesisBlockIdentifier: &rosetta.BlockIdentifier{
									Index: test.genesisIndex,
								},
							},
						},
					},
					Options: &rosetta.Options{
						OperationStatuses: []*rosetta.OperationStatus{},
						OperationTypes:    []string{},
					},
				},
			)
			assert.NoError(t, err)

			err = asserter.Block(context.Background(), test.block)
			assert.Equal(t, test.err, err)
		})
	}
}
