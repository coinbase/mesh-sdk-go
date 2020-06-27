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
	"errors"
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

var (
	validNetworkIdentifier = &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Mainnet",
	}

	wrongNetworkIdentifier = &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Testnet",
	}

	validAccountIdentifier = &types.AccountIdentifier{
		Address: "acct1",
	}

	genesisBlockIndex      = int64(0)
	genesisBlockIdentifier = &types.BlockIdentifier{
		Index: genesisBlockIndex,
		Hash:  "block 0",
	}
	validBlockIndex             = int64(1000)
	validPartialBlockIdentifier = &types.PartialBlockIdentifier{
		Index: &validBlockIndex,
	}

	validBlockIdentifier = &types.BlockIdentifier{
		Index: validBlockIndex,
		Hash:  "block 1",
	}

	validTransactionIdentifier = &types.TransactionIdentifier{
		Hash: "tx1",
	}

	validPublicKey = &types.PublicKey{
		HexBytes:  "48656c6c6f20476f7068657221",
		CurveType: types.Secp256k1,
	}

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

	validOps = []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: int64(0),
			},
			Type:    "PAYMENT",
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
			Account: validAccount,
			Amount:  validAmount,
		},
	}

	unsupportedTypeOps = []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: int64(0),
			},
			Type:    "STAKE",
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
			Account: validAccount,
			Amount:  validAmount,
		},
	}

	invalidOps = []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: int64(0),
			},
			Type:    "PAYMENT",
			Status:  "SUCCESS",
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
			Status:  "SUCCESS",
			Account: validAccount,
			Amount:  validAmount,
		},
	}

	a, aErr = NewServer(
		[]string{"PAYMENT"},
		true,
		[]*types.NetworkIdentifier{validNetworkIdentifier},
	)
)

func TestNewWithOptions(t *testing.T) {
	t.Run("ensure asserter initialized", func(t *testing.T) {
		assert.NotNil(t, a)
		assert.NoError(t, aErr)
	})
}

func TestSupportedNetworks(t *testing.T) {
	var tests = map[string]struct {
		networks []*types.NetworkIdentifier

		err error
	}{
		"valid networks": {
			networks: []*types.NetworkIdentifier{
				validNetworkIdentifier,
				wrongNetworkIdentifier,
			},
			err: nil,
		},
		"no valid networks": {
			networks: []*types.NetworkIdentifier{},
			err:      errors.New("no supported networks"),
		},
		"invalid network": {
			networks: []*types.NetworkIdentifier{
				{
					Blockchain: "blah",
				},
			},
			err: errors.New("NetworkIdentifier.Network is missing"),
		},
		"duplicate networks": {
			networks: []*types.NetworkIdentifier{
				validNetworkIdentifier,
				validNetworkIdentifier,
			},
			err: fmt.Errorf("supported network duplicate %+v", validNetworkIdentifier),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, SupportedNetworks(test.networks))
		})
	}
}

func TestAccountBalanceRequest(t *testing.T) {
	var tests = map[string]struct {
		request         *types.AccountBalanceRequest
		allowHistorical bool
		err             error
	}{
		"valid request": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("AccountBalanceRequest is nil"),
		},
		"missing network": {
			request: &types.AccountBalanceRequest{
				AccountIdentifier: validAccountIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing account": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("Account is nil"),
		},
		"valid historical request": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier:   validPartialBlockIdentifier,
			},
			allowHistorical: true,
			err:             nil,
		},
		"invalid historical request": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier:   &types.PartialBlockIdentifier{},
			},
			allowHistorical: true,
			err:             errors.New("neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set"),
		},
		"valid historical request when not enabled": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier:   validPartialBlockIdentifier,
			},
			allowHistorical: false,
			err:             errors.New("historical balance lookup is not supported"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := NewServer(
				[]string{"PAYMENT"},
				test.allowHistorical,
				[]*types.NetworkIdentifier{validNetworkIdentifier},
			)
			assert.NotNil(t, asserter)
			assert.NoError(t, err)

			err = asserter.AccountBalanceRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestBlockRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.BlockRequest
		err     error
	}{
		"valid request": {
			request: &types.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier:   validPartialBlockIdentifier,
			},
			err: nil,
		},
		"valid request for block 0": {
			request: &types.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier: &types.PartialBlockIdentifier{
					Index: &genesisBlockIndex,
				},
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.BlockRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				BlockIdentifier:   validPartialBlockIdentifier,
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("BlockRequest is nil"),
		},
		"missing network": {
			request: &types.BlockRequest{
				BlockIdentifier: validPartialBlockIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing block identifier": {
			request: &types.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("PartialBlockIdentifier is nil"),
		},
		"invalid PartialBlockIdentifier request": {
			request: &types.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier:   &types.PartialBlockIdentifier{},
			},
			err: errors.New("neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.BlockRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestBlockTransactionRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.BlockTransactionRequest
		err     error
	}{
		"valid request": {
			request: &types.BlockTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				BlockIdentifier:       validBlockIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.BlockTransactionRequest{
				NetworkIdentifier:     wrongNetworkIdentifier,
				BlockIdentifier:       validBlockIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("BlockTransactionRequest is nil"),
		},
		"missing network": {
			request: &types.BlockTransactionRequest{
				BlockIdentifier:       validBlockIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing block identifier": {
			request: &types.BlockTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: errors.New("BlockIdentifier is nil"),
		},
		"invalid BlockIdentifier request": {
			request: &types.BlockTransactionRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier:   &types.BlockIdentifier{},
			},
			err: errors.New("BlockIdentifier.Hash is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.BlockTransactionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestConstructionMetadataRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionMetadataRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionMetadataRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Options:           map[string]interface{}{},
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionMetadataRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				Options:           map[string]interface{}{},
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("ConstructionMetadataRequest is nil"),
		},
		"missing network": {
			request: &types.ConstructionMetadataRequest{
				Options: map[string]interface{}{},
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing options": {
			request: &types.ConstructionMetadataRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("ConstructionMetadataRequest.Options is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionMetadataRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestConstructionSubmitRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionSubmitRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionSubmitRequest{
				NetworkIdentifier: validNetworkIdentifier,
				SignedTransaction: "tx",
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionSubmitRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				SignedTransaction: "tx",
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("ConstructionSubmitRequest is nil"),
		},
		"empty tx": {
			request: &types.ConstructionSubmitRequest{},
			err:     errors.New("NetworkIdentifier is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionSubmitRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestMempoolTransactionRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.MempoolTransactionRequest
		err     error
	}{
		"valid request": {
			request: &types.MempoolTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.MempoolTransactionRequest{
				NetworkIdentifier:     wrongNetworkIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("MempoolTransactionRequest is nil"),
		},
		"missing network": {
			request: &types.MempoolTransactionRequest{
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"invalid TransactionIdentifier request": {
			request: &types.MempoolTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: &types.TransactionIdentifier{},
			},
			err: errors.New("TransactionIdentifier.Hash is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.MempoolTransactionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestMetadataRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.MetadataRequest
		err     error
	}{
		"valid request": {
			request: &types.MetadataRequest{},
			err:     nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("MetadataRequest is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.MetadataRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestNetworkRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.NetworkRequest
		err     error
	}{
		"valid request": {
			request: &types.NetworkRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.NetworkRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("NetworkRequest is nil"),
		},
		"missing network": {
			request: &types.NetworkRequest{},
			err:     errors.New("NetworkIdentifier is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.NetworkRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestConstructionDeriveRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionDeriveRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionDeriveRequest{
				NetworkIdentifier: validNetworkIdentifier,
				PublicKey:         validPublicKey,
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionDeriveRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("ConstructionDeriveRequest is nil"),
		},
		"nil public key": {
			request: &types.ConstructionDeriveRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("PublicKey cannot be nil"),
		},
		"invalid hex public key": {
			request: &types.ConstructionDeriveRequest{
				NetworkIdentifier: validNetworkIdentifier,
				PublicKey: &types.PublicKey{
					HexBytes:  "hello",
					CurveType: types.Secp256k1,
				},
			},
			err: errors.New("not a valid hex string public key"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionDeriveRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstructionPreprocessRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionPreprocessRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        validOps,
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf("%+v is not supported", wrongNetworkIdentifier),
		},
		"nil request": {
			request: nil,
			err:     errors.New("ConstructionPreprocessRequest is nil"),
		},
		"nil operations": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("operations cannot be empty"),
		},
		"empty operations": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        []*types.Operation{},
			},
			err: errors.New("operations cannot be empty"),
		},
		"unsupported operation type": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        unsupportedTypeOps,
			},
			err: errors.New("Operation.Type STAKE is invalid"),
		},
		"invalid operations": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        invalidOps,
			},
			err: errors.New("must be empty for construction"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionPreprocessRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
