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

package asserter

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestBlockIdentifier(t *testing.T) {
	var tests = map[string]struct {
		identifier *types.BlockIdentifier
		err        error
	}{
		"valid identifier": {
			identifier: &types.BlockIdentifier{
				Index: int64(1),
				Hash:  "block 1",
			},
			err: nil,
		},
		"nil identifier": {
			identifier: nil,
			err:        ErrBlockIdentifierIsNil,
		},
		"invalid index": {
			identifier: &types.BlockIdentifier{
				Index: int64(-1),
				Hash:  "block 1",
			},
			err: ErrBlockIdentifierIndexIsNeg,
		},
		"invalid hash": {
			identifier: &types.BlockIdentifier{
				Index: int64(1),
				Hash:  "",
			},
			err: ErrBlockIdentifierHashMissing,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := BlockIdentifier(test.identifier)
			assert.True(t, errors.Is(err, test.err))
		})
	}
}

func TestAmount(t *testing.T) {
	var tests = map[string]struct {
		amount *types.Amount
		err    error
	}{
		"valid amount": {
			amount: &types.Amount{
				Value: "100000",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: nil,
		},
		"valid amount no decimals": {
			amount: &types.Amount{
				Value: "100000",
				Currency: &types.Currency{
					Symbol: "BTC",
				},
			},
			err: nil,
		},
		"valid negative amount": {
			amount: &types.Amount{
				Value: "-100000",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: nil,
		},
		"nil amount": {
			amount: nil,
			err:    ErrAmountValueMissing,
		},
		"nil currency": {
			amount: &types.Amount{
				Value: "100000",
			},
			err: ErrAmountCurrencyIsNil,
		},
		"invalid non-number": {
			amount: &types.Amount{
				Value: "blah",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: ErrAmountIsNotInt,
		},
		"invalid integer format": {
			amount: &types.Amount{
				Value: "1.0",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: ErrAmountIsNotInt,
		},
		"invalid non-integer": {
			amount: &types.Amount{
				Value: "1.1",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 1,
				},
			},
			err: ErrAmountIsNotInt,
		},
		"invalid symbol": {
			amount: &types.Amount{
				Value: "11",
				Currency: &types.Currency{
					Decimals: 1,
				},
			},
			err: ErrAmountCurrencySymbolEmpty,
		},
		"invalid decimals": {
			amount: &types.Amount{
				Value: "111",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: -1,
				},
			},
			err: ErrAmountCurrencyHasNegDecimals,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Amount(test.amount)
			assert.True(t, errors.Is(err, test.err))
		})
	}
}

func TestOperationIdentifier(t *testing.T) {
	var (
		validNetworkIndex   = int64(1)
		invalidNetworkIndex = int64(-1)
	)

	var tests = map[string]struct {
		identifier *types.OperationIdentifier
		index      int64
		err        error
	}{
		"valid identifier": {
			identifier: &types.OperationIdentifier{
				Index: 0,
			},
			index: 0,
			err:   nil,
		},
		"nil identifier": {
			identifier: nil,
			index:      0,
			err:        ErrOperationIdentifierIndexIsNil,
		},
		"out-of-order index": {
			identifier: &types.OperationIdentifier{
				Index: 0,
			},
			index: 1,
			err:   ErrOperationIdentifierIndexOutOfOrder,
		},
		"valid identifier with network index": {
			identifier: &types.OperationIdentifier{
				Index:        0,
				NetworkIndex: &validNetworkIndex,
			},
			index: 0,
			err:   nil,
		},
		"invalid identifier with network index": {
			identifier: &types.OperationIdentifier{
				Index:        0,
				NetworkIndex: &invalidNetworkIndex,
			},
			index: 0,
			err:   ErrOperationIdentifierNetworkIndexInvalid,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := OperationIdentifier(test.identifier, test.index)
			assert.True(t, errors.Is(err, test.err))
		})
	}
}

func TestAccountIdentifier(t *testing.T) {
	var tests = map[string]struct {
		identifier *types.AccountIdentifier
		err        error
	}{
		"valid identifier": {
			identifier: &types.AccountIdentifier{
				Address: "acct1",
			},
			err: nil,
		},
		"invalid address": {
			identifier: &types.AccountIdentifier{
				Address: "",
			},
			err: ErrAccountAddrMissing,
		},
		"valid identifier with subaccount": {
			identifier: &types.AccountIdentifier{
				Address: "acct1",
				SubAccount: &types.SubAccountIdentifier{
					Address: "acct2",
				},
			},
			err: nil,
		},
		"invalid identifier with subaccount": {
			identifier: &types.AccountIdentifier{
				Address: "acct1",
				SubAccount: &types.SubAccountIdentifier{
					Address: "",
				},
			},
			err: ErrAccountSubAccountAddrMissing,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := AccountIdentifier(test.identifier)
			assert.True(t, errors.Is(err, test.err))
		})
	}
}

func TestOperationsValidations(t *testing.T) {
	var (
		validDepositAmount = &types.Amount{
			Value: "1000",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		validWithdrawAmount = &types.Amount{
			Value: "-1000",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		validFeeAmount = &types.Amount{
			Value: "-100",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		invalidFeeAmount = &types.Amount{
			Value: "100",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		validAccount = &types.AccountIdentifier{
			Address: "test",
		}
	)
	var tests = map[string]struct {
		operations         []*types.Operation
		validationFilePath string
		asserter           *Asserter
		construction       bool
		err                error
	}{
		"valid operations based on validation file": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validWithdrawAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(2),
					},
					Type:    "FEE",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validFeeAmount,
				},
			},
			validationFilePath: "data/validation_fee_and_payment_balanced.json",
			construction:       false,
			err:                nil,
		},
		"throw error on missing fee operation": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validWithdrawAmount,
				},
			},
			validationFilePath: "data/validation_fee_and_payment_balanced.json",
			construction:       false,
			err:                ErrFeeCountMismatch,
		},
		"throw error on missing payment operation": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "FEE",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validFeeAmount,
				},
			},
			validationFilePath: "data/validation_fee_and_payment_balanced.json",
			construction:       false,
			err:                ErrPaymentCountMismatch,
		},
		"throw error on payment amount not balancing": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount: &types.Amount{
						Value: "-2000",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(2),
					},
					Type:    "FEE",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validFeeAmount,
				},
			},
			validationFilePath: "data/validation_fee_and_payment_balanced.json",
			construction:       false,
			err:                ErrPaymentAmountNotBalancing,
		},
		"valid operations based on validation file - unbalanced": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount: &types.Amount{
						Value: "-2000",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(2),
					},
					Type:    "FEE",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validFeeAmount,
				},
			},
			validationFilePath: "data/validation_fee_and_payment_unbalanced.json",
			construction:       false,
			err:                nil,
		},
		"fee operation shouldn't contain related_operation key": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount: &types.Amount{
						Value: "-2000",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(2),
					},
					Type:    "FEE",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validFeeAmount,
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 0,
						},
					},
				},
			},
			validationFilePath: "data/validation_fee_and_payment_unbalanced.json",
			construction:       false,
			err:                ErrRelatedOperationInFeeNotAllowed,
		},

		"fee amount is non-negative": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount: &types.Amount{
						Value: "-2000",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(2),
					},
					Type:    "FEE",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  invalidFeeAmount,
				},
			},
			validationFilePath: "data/validation_fee_and_payment_unbalanced.json",
			construction:       false,
			err:                ErrFeeAmountNotNegative,
		},

		"fee amount is negative as expected": {
			operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(0),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validDepositAmount,
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(1),
					},
					Type:    "PAYMENT",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount: &types.Amount{
						Value: "-2000",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: int64(2),
					},
					Type:    "FEE",
					Status:  types.String("SUCCESS"),
					Account: validAccount,
					Amount:  validFeeAmount,
				},
			},
			validationFilePath: "data/validation_fee_and_payment_unbalanced.json",
			construction:       false,
			err:                nil,
		},
	}

	for name, test := range tests {
		asserter, err := NewClientWithResponses(
			&types.NetworkIdentifier{
				Blockchain: "hello",
				Network:    "world",
			},
			&types.NetworkStatusResponse{
				GenesisBlockIdentifier: &types.BlockIdentifier{
					Index: 0,
					Hash:  "block 0",
				},
				CurrentBlockIdentifier: &types.BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				CurrentBlockTimestamp: MinUnixEpoch + 1,
				Peers: []*types.Peer{
					{
						PeerID: "peer 1",
					},
				},
			},
			&types.NetworkOptionsResponse{
				Version: &types.Version{
					RosettaVersion: "1.4.0",
					NodeVersion:    "1.0",
				},
				Allow: &types.Allow{
					OperationStatuses: []*types.OperationStatus{
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
						"FEE",
					},
				},
			},
			test.validationFilePath,
		)
		assert.NotNil(t, asserter)
		assert.NoError(t, err)

		t.Run(name, func(t *testing.T) {
			actualErr := asserter.Operations(test.operations, test.construction)
			if test.err != nil {
				assert.Contains(t, actualErr.Error(), test.err.Error())
			} else {
				assert.NoError(t, actualErr)
			}
		})
	}
}

func TestOperation(t *testing.T) {
	var (
		validAmount = &types.Amount{
			Value: "1000",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		validAccount = &types.AccountIdentifier{
			Address: "test",
		}
	)

	var tests = map[string]struct {
		operation    *types.Operation
		index        int64
		successful   bool
		construction bool
		err          error
	}{
		"valid operation": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			index:      int64(1),
			successful: true,
			err:        nil,
		},
		"valid operation no account": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: types.String("SUCCESS"),
			},
			index:      int64(1),
			successful: true,
			err:        nil,
		},
		"nil operation": {
			operation: nil,
			index:     int64(1),
			err:       ErrOperationIsNil,
		},
		"invalid operation no account": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: types.String("SUCCESS"),
				Amount: validAmount,
			},
			index: int64(1),
			err:   ErrAccountIsNil,
		},
		"invalid operation empty account": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: &types.AccountIdentifier{},
				Amount:  validAmount,
			},
			index: int64(1),
			err:   ErrAccountAddrMissing,
		},
		"invalid operation invalid index": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: types.String("SUCCESS"),
			},
			index: int64(2),
			err:   ErrOperationIdentifierIndexOutOfOrder,
		},
		"invalid operation invalid type": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "STAKE",
				Status: types.String("SUCCESS"),
			},
			index: int64(1),
			err:   ErrOperationTypeInvalid,
		},
		"unsuccessful operation": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: types.String("FAILURE"),
			},
			index:      int64(1),
			successful: false,
			err:        nil,
		},
		"invalid operation invalid status": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:   "PAYMENT",
				Status: types.String("DEFERRED"),
			},
			index: int64(1),
			err:   ErrOperationStatusInvalid,
		},
		"valid construction operation": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:    "PAYMENT",
				Account: validAccount,
				Amount:  validAmount,
			},
			index:        int64(1),
			successful:   false,
			construction: true,
			err:          nil,
		},
		"valid construction operation (empty status)": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:    "PAYMENT",
				Status:  types.String(""),
				Account: validAccount,
				Amount:  validAmount,
			},
			index:        int64(1),
			successful:   false,
			construction: true,
			err:          nil,
		},
		"invalid construction operation": {
			operation: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			index:        int64(1),
			successful:   false,
			construction: true,
			err:          ErrOperationStatusNotEmptyForConstruction,
		},
	}

	for name, test := range tests {
		asserter, err := NewClientWithResponses(
			&types.NetworkIdentifier{
				Blockchain: "hello",
				Network:    "world",
			},
			&types.NetworkStatusResponse{
				GenesisBlockIdentifier: &types.BlockIdentifier{
					Index: 0,
					Hash:  "block 0",
				},
				CurrentBlockIdentifier: &types.BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				CurrentBlockTimestamp: MinUnixEpoch + 1,
				Peers: []*types.Peer{
					{
						PeerID: "peer 1",
					},
				},
			},
			&types.NetworkOptionsResponse{
				Version: &types.Version{
					RosettaVersion: "1.4.0",
					NodeVersion:    "1.0",
				},
				Allow: &types.Allow{
					OperationStatuses: []*types.OperationStatus{
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
			"",
		)
		assert.NotNil(t, asserter)
		assert.NoError(t, err)

		t.Run(name, func(t *testing.T) {
			err := asserter.Operation(test.operation, test.index, test.construction)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}

			if err == nil && !test.construction {
				successful, err := asserter.OperationSuccessful(test.operation)
				assert.NoError(t, err)
				assert.Equal(t, test.successful, successful)
			}
		})
	}
}

func TestBlock(t *testing.T) {
	genesisIdentifier := &types.BlockIdentifier{
		Hash:  "gen",
		Index: 0,
	}
	validBlockIdentifier := &types.BlockIdentifier{
		Hash:  "blah",
		Index: 100,
	}
	validParentBlockIdentifier := &types.BlockIdentifier{
		Hash:  "blah parent",
		Index: 99,
	}
	validAmount := &types.Amount{
		Value: "1000",
		Currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}
	validAccount := &types.AccountIdentifier{
		Address: "test",
	}
	validTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
		RelatedTransactions: []*types.RelatedTransaction{
			{
				NetworkIdentifier: &types.NetworkIdentifier{
					Blockchain: "hello",
					Network:    "world",
				},
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "blah",
				},
				Direction: types.Forward,
			},
		},
	}
	relatedToSelfTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
	}
	outOfOrderTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
	}
	relatedToLaterTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(1),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
	}
	relatedDuplicateTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
	}
	relatedMissingTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{},
				Type:              "PAYMENT",
				Status:            types.String("SUCCESS"),
				Account:           validAccount,
				Amount:            validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(2),
				},
				RelatedOperations: []*types.OperationIdentifier{},
				Type:              "PAYMENT",
				Status:            types.String("SUCCESS"),
				Account:           validAccount,
				Amount:            validAmount,
			},
		},
	}
	invalidRelatedTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
		RelatedTransactions: []*types.RelatedTransaction{
			{
				NetworkIdentifier: &types.NetworkIdentifier{
					Blockchain: "hello",
					Network:    "world",
				},
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "blah",
				},
				Direction: "blah",
			},
		},
	}
	duplicateRelatedTransactions := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
		RelatedTransactions: []*types.RelatedTransaction{
			{
				NetworkIdentifier: &types.NetworkIdentifier{
					Blockchain: "hello",
					Network:    "world",
				},
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "blah",
				},
				Direction: types.Forward,
			},
			{
				NetworkIdentifier: &types.NetworkIdentifier{
					Blockchain: "hello",
					Network:    "world",
				},
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "blah",
				},
				Direction: types.Forward,
			},
		},
	}

	var tests = map[string]struct {
		block              *types.Block
		validationFilePath string
		genesisIndex       int64
		startIndex         *int64
		err                error
	}{
		"valid block": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{validTransaction},
			},
			err: nil,
		},
		"valid block (before start index)": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Transactions:          []*types.Transaction{validTransaction},
			},
			startIndex: types.Int64(validBlockIdentifier.Index + 1),
			err:        nil,
		},
		"genesis block (without start index)": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validBlockIdentifier,
				Transactions:          []*types.Transaction{validTransaction},
			},
			genesisIndex: validBlockIdentifier.Index,
			err:          nil,
		},
		"genesis block (with start index)": {
			block: &types.Block{
				BlockIdentifier:       genesisIdentifier,
				ParentBlockIdentifier: genesisIdentifier,
				Transactions:          []*types.Transaction{validTransaction},
			},
			genesisIndex: genesisIdentifier.Index,
			startIndex:   types.Int64(genesisIdentifier.Index + 1),
			err:          nil,
		},
		"invalid genesis block (with start index)": {
			block: &types.Block{
				BlockIdentifier:       genesisIdentifier,
				ParentBlockIdentifier: genesisIdentifier,
				Transactions:          []*types.Transaction{validTransaction},
			},
			genesisIndex: genesisIdentifier.Index,
			startIndex:   types.Int64(genesisIdentifier.Index),
			err:          ErrTimestampBeforeMin,
		},
		"out of order transaction operations": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{outOfOrderTransaction},
			},
			err: ErrOperationIdentifierIndexOutOfOrder,
		},
		"related to self transaction operations": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{relatedToSelfTransaction},
			},
			err: ErrRelatedOperationIndexOutOfOrder,
		},
		"related to later transaction operations": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{relatedToLaterTransaction},
			},
			err: ErrRelatedOperationIndexOutOfOrder,
		},
		"duplicate related transaction operations": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{relatedDuplicateTransaction},
			},
			err: ErrRelatedOperationIndexDuplicate,
		},
		"missing related transaction operations": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{relatedMissingTransaction},
			},
			err:                ErrRelatedOperationMissing,
			validationFilePath: "data/validation_balanced_related_ops.json",
		},
		"nil block": {
			block: nil,
			err:   ErrBlockIsNil,
		},
		"nil block hash": {
			block: &types.Block{
				BlockIdentifier:       nil,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{validTransaction},
			},
			err: ErrBlockIdentifierIsNil,
		},
		"invalid block hash": {
			block: &types.Block{
				BlockIdentifier:       &types.BlockIdentifier{},
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{validTransaction},
			},
			err: ErrBlockIdentifierHashMissing,
		},
		"block previous hash missing": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: &types.BlockIdentifier{},
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{validTransaction},
			},
			err: ErrBlockIdentifierHashMissing,
		},
		"invalid parent block index": {
			block: &types.Block{
				BlockIdentifier: validBlockIdentifier,
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  validParentBlockIdentifier.Hash,
					Index: validBlockIdentifier.Index,
				},
				Timestamp:    MinUnixEpoch + 1,
				Transactions: []*types.Transaction{validTransaction},
			},
			err: ErrBlockIndexPrecedesParentBlockIndex,
		},
		"invalid parent block hash": {
			block: &types.Block{
				BlockIdentifier: validBlockIdentifier,
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  validBlockIdentifier.Hash,
					Index: validParentBlockIdentifier.Index,
				},
				Timestamp:    MinUnixEpoch + 1,
				Transactions: []*types.Transaction{validTransaction},
			},
			err: ErrBlockHashEqualsParentBlockHash,
		},
		"invalid block timestamp less than MinUnixEpoch": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Transactions:          []*types.Transaction{validTransaction},
			},
			err: ErrTimestampBeforeMin,
		},
		"invalid block timestamp greater than MaxUnixEpoch": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Transactions:          []*types.Transaction{validTransaction},
				Timestamp:             MaxUnixEpoch + 1,
			},
			err: ErrTimestampAfterMax,
		},
		"invalid block transaction": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions: []*types.Transaction{
					{},
				},
			},
			err: ErrTxIdentifierIsNil,
		},
		"invalid related transaction": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{invalidRelatedTransaction},
			},
			err: ErrInvalidDirection,
		},
		"duplicate related transaction": {
			block: &types.Block{
				BlockIdentifier:       validBlockIdentifier,
				ParentBlockIdentifier: validParentBlockIdentifier,
				Timestamp:             MinUnixEpoch + 1,
				Transactions:          []*types.Transaction{duplicateRelatedTransactions},
			},
			err: ErrDuplicateRelatedTransaction,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := NewClientWithResponses(
				&types.NetworkIdentifier{
					Blockchain: "hello",
					Network:    "world",
				},
				&types.NetworkStatusResponse{
					GenesisBlockIdentifier: &types.BlockIdentifier{
						Index: test.genesisIndex,
						Hash:  fmt.Sprintf("block %d", test.genesisIndex),
					},
					CurrentBlockIdentifier: &types.BlockIdentifier{
						Index: 100,
						Hash:  "block 100",
					},
					CurrentBlockTimestamp: MinUnixEpoch + 1,
					Peers: []*types.Peer{
						{
							PeerID: "peer 1",
						},
					},
				},
				&types.NetworkOptionsResponse{
					Version: &types.Version{
						RosettaVersion: "1.4.0",
						NodeVersion:    "1.0",
					},
					Allow: &types.Allow{
						OperationStatuses: []*types.OperationStatus{
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
						TimestampStartIndex: test.startIndex,
					},
				},
				test.validationFilePath,
			)
			assert.NotNil(t, asserter)
			assert.NoError(t, err)

			err = asserter.Block(test.block)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
