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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
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

	missingNetworkNetworkIdentifier = &types.NetworkIdentifier{
		Blockchain: "blah",
	}

	validAccountIdentifier = &types.AccountIdentifier{
		Address: "acct1",
	}

	genesisBlockIndex           = int64(0)
	validBlockIndex             = int64(1000)
	validPartialBlockIdentifier = &types.PartialBlockIdentifier{
		Index: &validBlockIndex,
	}

	validBlockIdentifier = &types.BlockIdentifier{
		Index: validBlockIndex,
		Hash:  "block 1",
	}

	emptyBlockIdentifier = &types.BlockIdentifier{}

	invalidBlockIdentifierHash  string = ""
	invalidBlockIdentifierIndex int64  = -1

	validTransactionIdentifier = &types.TransactionIdentifier{
		Hash: "tx1",
	}

	validPublicKey = &types.PublicKey{
		Bytes:     []byte("hello"),
		CurveType: types.Secp256k1,
	}

	missingBytesPublicKey = &types.PublicKey{
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
	}

	validSignatures = []*types.Signature{
		{
			SigningPayload: &types.SigningPayload{
				AccountIdentifier: validAccount,
				Bytes:             []byte("blah"),
			},
			PublicKey:     validPublicKey,
			SignatureType: types.Ed25519,
			Bytes:         []byte("hello"),
		},
	}

	signatureTypeMismatch = []*types.Signature{
		{
			SigningPayload: &types.SigningPayload{
				AccountIdentifier: validAccount,
				Bytes:             []byte("blah"),
				SignatureType:     types.EcdsaRecovery,
			},
			PublicKey:     validPublicKey,
			SignatureType: types.Ed25519,
			Bytes:         []byte("hello"),
		},
	}

	signatureTypeMatch = []*types.Signature{
		{
			SigningPayload: &types.SigningPayload{
				AccountIdentifier: validAccount,
				Bytes:             []byte("blah"),
				SignatureType:     types.Ed25519,
			},
			PublicKey:     validPublicKey,
			SignatureType: types.Ed25519,
			Bytes:         []byte("hello"),
		},
	}

	emptySignature = []*types.Signature{
		{
			SigningPayload: &types.SigningPayload{
				AccountIdentifier: validAccount,
				Bytes:             []byte("blah"),
				SignatureType:     types.Ed25519,
			},
			PublicKey:     validPublicKey,
			SignatureType: types.Ed25519,
		},
	}

	a, aErr = NewServer(
		[]string{"PAYMENT"},
		true,
		[]*types.NetworkIdentifier{validNetworkIdentifier},
		[]string{"eth_call"},
		false,
		"",
	)
)

func TestNewWithOptions(t *testing.T) {
	assert.NotNil(t, a)
	assert.NoError(t, aErr)
	tests := map[string]struct {
		supportedOperationTypes []string
		historicalBalanceLookup bool
		supportedNetworks       []*types.NetworkIdentifier
		callMethods             []string

		err error
	}{
		"basic": {
			supportedOperationTypes: []string{"PAYMENT"},
			historicalBalanceLookup: true,
			supportedNetworks:       []*types.NetworkIdentifier{validNetworkIdentifier},
			callMethods:             []string{"eth_call"},
		},
		"no call methods": {
			supportedOperationTypes: []string{"PAYMENT"},
			historicalBalanceLookup: true,
			supportedNetworks:       []*types.NetworkIdentifier{validNetworkIdentifier},
		},
		"duplicate operation types": {
			supportedOperationTypes: []string{"PAYMENT", "PAYMENT"},
			historicalBalanceLookup: true,
			supportedNetworks:       []*types.NetworkIdentifier{validNetworkIdentifier},
			callMethods:             []string{"eth_call"},
			err: errors.New(
				"Allow.OperationTypes contains a duplicate PAYMENT",
			),
		},
		"empty operation type": {
			supportedOperationTypes: []string{"PAYMENT", ""},
			historicalBalanceLookup: true,
			supportedNetworks:       []*types.NetworkIdentifier{validNetworkIdentifier},
			callMethods:             []string{"eth_call"},
			err:                     errors.New("Allow.OperationTypes has an empty string"),
		},
		"duplicate network identifier": {
			supportedOperationTypes: []string{"PAYMENT"},
			historicalBalanceLookup: true,
			supportedNetworks: []*types.NetworkIdentifier{
				validNetworkIdentifier,
				validNetworkIdentifier,
			},
			callMethods: []string{"eth_call"},
			err:         ErrSupportedNetworksDuplicate,
		},
		"nil network identifier": {
			supportedOperationTypes: []string{"PAYMENT"},
			historicalBalanceLookup: true,
			supportedNetworks:       []*types.NetworkIdentifier{validNetworkIdentifier, nil},
			callMethods:             []string{"eth_call"},
			err:                     ErrNetworkIdentifierIsNil,
		},
		"no supported networks": {
			supportedOperationTypes: []string{"PAYMENT"},
			historicalBalanceLookup: true,
			callMethods:             []string{"eth_call"},
			err:                     ErrNoSupportedNetworks,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			thisA, thisErr := NewServer(
				test.supportedOperationTypes,
				test.historicalBalanceLookup,
				test.supportedNetworks,
				test.callMethods,
				false,
				"",
			)
			if test.err == nil {
				assert.NotNil(t, thisA)
				assert.NoError(t, thisErr)
			} else {
				assert.Nil(t, thisA)
				assert.True(t, errors.Is(thisErr, test.err) || strings.Contains(thisErr.Error(), test.err.Error()))
			}
		})
	}
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
			err:      ErrNoSupportedNetworks,
		},
		"invalid network": {
			networks: []*types.NetworkIdentifier{
				{
					Blockchain: "blah",
				},
			},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(missingNetworkNetworkIdentifier),
				ErrNetworkIdentifierNetworkMissing,
			),
		},
		"duplicate networks": {
			networks: []*types.NetworkIdentifier{
				validNetworkIdentifier,
				validNetworkIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(validNetworkIdentifier),
				ErrSupportedNetworksDuplicate,
			),
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
		"valid request with currencies": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				Currencies: []*types.Currency{
					{
						Symbol:   "BTC",
						Decimals: 8,
					},
					{
						Symbol:   "ETH",
						Decimals: 18,
					},
				},
			},
			err: nil,
		},
		"valid request with duplicate currencies": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				Currencies: []*types.Currency{
					{
						Symbol:   "BTC",
						Decimals: 8,
					},
					{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			err: ErrDuplicateCurrency,
		},
		"invalid request wrong network": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: ErrRequestedNetworkNotSupported,
		},
		"nil request": {
			request: nil,
			err:     ErrAccountBalanceRequestIsNil,
		},
		"missing network": {
			request: &types.AccountBalanceRequest{
				AccountIdentifier: validAccountIdentifier,
			},
			err: ErrNetworkIdentifierIsNil,
		},
		"missing account": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrAccountIsNil,
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
		"invalid historical request when block identifier hash is invalid": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier: &types.PartialBlockIdentifier{
					Hash: &invalidBlockIdentifierHash,
				},
			},
			allowHistorical: true,
			err:             ErrPartialBlockIdentifierHashIsEmpty,
		},
		"invalid historical request when block identifier index is invalid": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier: &types.PartialBlockIdentifier{
					Index: &invalidBlockIdentifierIndex,
				},
			},
			allowHistorical: true,
			err:             ErrPartialBlockIdentifierIndexIsNegative,
		},
		"valid historical request when not enabled": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier:   validPartialBlockIdentifier,
			},
			allowHistorical: false,
			err:             ErrAccountBalanceRequestHistoricalBalanceLookupNotSupported,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := NewServer(
				[]string{"PAYMENT"},
				test.allowHistorical,
				[]*types.NetworkIdentifier{validNetworkIdentifier},
				nil,
				false,
				"",
			)
			assert.NotNil(t, asserter)
			assert.NoError(t, err)

			err = asserter.AccountBalanceRequest(test.request)
			if test.err != nil {
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
			}
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
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrBlockRequestIsNil,
		},
		"missing network": {
			request: &types.BlockRequest{
				BlockIdentifier: validPartialBlockIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(nil),
				ErrNetworkIdentifierIsNil,
			),
		},
		"missing block identifier": {
			request: &types.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrPartialBlockIdentifierIsNil,
		},
		"invalid block request when block identifier hash is invalid": {
			request: &types.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier: &types.PartialBlockIdentifier{
					Hash: &invalidBlockIdentifierHash,
				},
			},
			err: ErrPartialBlockIdentifierHashIsEmpty,
		},
		"invalid block request when block identifier index is invalid": {
			request: &types.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier: &types.PartialBlockIdentifier{
					Index: &invalidBlockIdentifierIndex,
				},
			},
			err: ErrPartialBlockIdentifierIndexIsNegative,
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
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrBlockTransactionRequestIsNil,
		},
		"missing network": {
			request: &types.BlockTransactionRequest{
				BlockIdentifier:       validBlockIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(nil),
				ErrNetworkIdentifierIsNil,
			),
		},
		"missing block identifier": {
			request: &types.BlockTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: fmt.Errorf(
				"block identifier %s is invalid: %w",
				types.PrintStruct(nil),
				ErrBlockIdentifierIsNil,
			),
		},
		"invalid BlockIdentifier request": {
			request: &types.BlockTransactionRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier:   &types.BlockIdentifier{},
			},
			err: fmt.Errorf(
				"block identifier %s is invalid: %w",
				types.PrintStruct(emptyBlockIdentifier),
				ErrBlockIdentifierHashMissing,
			),
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
		"valid request with public keys": {
			request: &types.ConstructionMetadataRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Options:           map[string]interface{}{},
				PublicKeys: []*types.PublicKey{
					{
						Bytes:     []byte("hello"),
						CurveType: types.Secp256k1,
					},
				},
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionMetadataRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				Options:           map[string]interface{}{},
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionMetadataRequestIsNil,
		},
		"missing network": {
			request: &types.ConstructionMetadataRequest{
				Options: map[string]interface{}{},
			},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(nil),
				ErrNetworkIdentifierIsNil,
			),
		},
		"missing options": {
			request: &types.ConstructionMetadataRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
		},
		"invalid request with public keys": {
			request: &types.ConstructionMetadataRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Options:           map[string]interface{}{},
				PublicKeys: []*types.PublicKey{
					{
						CurveType: types.Secp256k1,
					},
				},
			},
			err: fmt.Errorf(
				"public key %s is invalid: %w",
				types.PrintStruct(missingBytesPublicKey),
				ErrPublicKeyBytesEmpty,
			),
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
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionSubmitRequestIsNil,
		},
		"empty tx": {
			request: &types.ConstructionSubmitRequest{},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(nil),
				ErrNetworkIdentifierIsNil,
			),
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
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrMempoolTransactionRequestIsNil,
		},
		"missing network": {
			request: &types.MempoolTransactionRequest{
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(nil),
				ErrNetworkIdentifierIsNil,
			),
		},
		"invalid TransactionIdentifier request": {
			request: &types.MempoolTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: &types.TransactionIdentifier{},
			},
			err: ErrTxIdentifierHashMissing,
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
			err:     ErrMetadataRequestIsNil,
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
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrNetworkRequestIsNil,
		},
		"missing network": {
			request: &types.NetworkRequest{},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(nil),
				ErrNetworkIdentifierIsNil,
			),
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
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionDeriveRequestIsNil,
		},
		"nil public key": {
			request: &types.ConstructionDeriveRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrPublicKeyIsNil,
		},
		"empty public key bytes": {
			request: &types.ConstructionDeriveRequest{
				NetworkIdentifier: validNetworkIdentifier,
				PublicKey: &types.PublicKey{
					CurveType: types.Secp256k1,
				},
			},
			err: ErrPublicKeyBytesEmpty,
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
		"valid request with suggested fee multiplier": {
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
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionPreprocessRequestIsNil,
		},
		"nil operations": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrNoOperationsForConstruction,
		},
		"empty operations": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        []*types.Operation{},
			},
			err: ErrNoOperationsForConstruction,
		},
		"unsupported operation type": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        unsupportedTypeOps,
			},
			err: ErrOperationTypeInvalid,
		},
		"invalid operations": {
			request: &types.ConstructionPreprocessRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        invalidOps,
			},
			err: ErrOperationStatusNotEmptyForConstruction,
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

func TestConstructionPayloadsRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionPayloadsRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        validOps,
				Metadata:          map[string]interface{}{"test": "hello"},
			},
			err: nil,
		},
		"valid request with public keys": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        validOps,
				Metadata:          map[string]interface{}{"test": "hello"},
				PublicKeys: []*types.PublicKey{
					{
						Bytes:     []byte("hello"),
						CurveType: types.Secp256k1,
					},
				},
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionPayloadsRequestIsNil,
		},
		"nil operations": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrNoOperationsForConstruction,
		},
		"empty operations": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        []*types.Operation{},
			},
			err: ErrNoOperationsForConstruction,
		},
		"unsupported operation type": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        unsupportedTypeOps,
			},
			err: ErrOperationTypeInvalid,
		},
		"invalid operations": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        invalidOps,
			},
			err: ErrOperationStatusNotEmptyForConstruction,
		},
		"invalid request with public keys": {
			request: &types.ConstructionPayloadsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operations:        validOps,
				Metadata:          map[string]interface{}{"test": "hello"},
				PublicKeys: []*types.PublicKey{
					{
						CurveType: types.Secp256k1,
					},
				},
			},
			err: ErrPublicKeyBytesEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionPayloadsRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstructionCombineRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionCombineRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
				Signatures:          validSignatures,
			},
			err: nil,
		},
		"valid request 2": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
				Signatures: []*types.Signature{
					{
						SigningPayload: &types.SigningPayload{
							AccountIdentifier: validAccount,
							Bytes:             []byte("blah"),
						},
						PublicKey:     validPublicKey,
						SignatureType: types.Ed25519,
						Bytes:         []byte("hello"),
					},
				},
			},
			err: nil,
		},
		"valid request 3": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
				Signatures: []*types.Signature{
					{
						SigningPayload: &types.SigningPayload{
							AccountIdentifier: validAccount,
							Bytes:             []byte("blah"),
						},
						PublicKey:     validPublicKey,
						SignatureType: types.Ed25519,
						Bytes:         []byte("hello"),
					},
				},
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionCombineRequestIsNil,
		},
		"empty unsigned transaction": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Signatures:        validSignatures,
			},
			err: ErrConstructionCombineRequestUnsignedTxEmpty,
		},
		"nil signatures": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
			},
			err: ErrSignaturesEmpty,
		},
		"empty signatures": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
				Signatures:          []*types.Signature{},
			},
			err: ErrSignaturesEmpty,
		},
		"signature type mismatch": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
				Signatures:          signatureTypeMismatch,
			},
			err: ErrSignaturesReturnedSigMismatch,
		},
		"empty signature": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
				Signatures:          emptySignature,
			},
			err: ErrSignatureBytesEmpty,
		},
		"signature type match": {
			request: &types.ConstructionCombineRequest{
				NetworkIdentifier:   validNetworkIdentifier,
				UnsignedTransaction: "blah",
				Signatures:          signatureTypeMatch,
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionCombineRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstructionHashRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionHashRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionHashRequest{
				NetworkIdentifier: validNetworkIdentifier,
				SignedTransaction: "blah",
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionHashRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionHashRequestIsNil,
		},
		"empty signed transaction": {
			request: &types.ConstructionHashRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrConstructionHashRequestSignedTxEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionHashRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstructionParseRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.ConstructionParseRequest
		err     error
	}{
		"valid request": {
			request: &types.ConstructionParseRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Transaction:       "blah",
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.ConstructionParseRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrConstructionParseRequestIsNil,
		},
		"empty signed transaction": {
			request: &types.ConstructionParseRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrConstructionParseRequestEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.ConstructionParseRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCallRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.CallRequest
		err     error
	}{
		"valid request": {
			request: &types.CallRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Method:            "eth_call",
			},
			err: nil,
		},
		"valid request with params": {
			request: &types.CallRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Method:            "eth_call",
				Parameters: map[string]interface{}{
					"hello": "test",
				},
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.CallRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"unsupported method": {
			request: &types.CallRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Method:            "eth_debug",
			},
			err: ErrCallMethodUnsupported,
		},
		"nil request": {
			request: nil,
			err:     ErrCallRequestIsNil,
		},
		"empty method": {
			request: &types.CallRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrCallMethodEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.CallRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestAccountCoinsRequest(t *testing.T) {
	var tests = map[string]struct {
		request      *types.AccountCoinsRequest
		allowMempool bool
		err          error
	}{
		"valid request": {
			request: &types.AccountCoinsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: nil,
		},
		"valid request with currencies": {
			request: &types.AccountCoinsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				Currencies: []*types.Currency{
					{
						Symbol:   "BTC",
						Decimals: 8,
					},
					{
						Symbol:   "ETH",
						Decimals: 18,
					},
				},
			},
			err: nil,
		},
		"valid request with duplicate currencies": {
			request: &types.AccountCoinsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				Currencies: []*types.Currency{
					{
						Symbol:   "BTC",
						Decimals: 8,
					},
					{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			err: ErrDuplicateCurrency,
		},
		"invalid request wrong network": {
			request: &types.AccountCoinsRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: ErrRequestedNetworkNotSupported,
		},
		"nil request": {
			request: nil,
			err:     ErrAccountCoinsRequestIsNil,
		},
		"missing network": {
			request: &types.AccountCoinsRequest{
				AccountIdentifier: validAccountIdentifier,
			},
			err: ErrNetworkIdentifierIsNil,
		},
		"missing account": {
			request: &types.AccountCoinsRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: ErrAccountIsNil,
		},
		"valid mempool lookup request": {
			request: &types.AccountCoinsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			allowMempool: true,
			err:          nil,
		},
		"valid mempool lookup request when not enabled": {
			request: &types.AccountCoinsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				IncludeMempool:    true,
			},
			allowMempool: false,
			err:          ErrMempoolCoinsNotSupported,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := NewServer(
				[]string{"PAYMENT"},
				false,
				[]*types.NetworkIdentifier{validNetworkIdentifier},
				nil,
				test.allowMempool,
				"",
			)
			assert.NotNil(t, asserter)
			assert.NoError(t, err)

			err = asserter.AccountCoinsRequest(test.request)
			if test.err != nil {
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEventsBlocksRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.EventsBlocksRequest
		err     error
	}{
		"valid request": {
			request: &types.EventsBlocksRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.EventsBlocksRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrEventsBlocksRequestIsNil,
		},
		"negative offset": {
			request: &types.EventsBlocksRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Offset:            types.Int64(-1),
			},
			err: ErrOffsetIsNegative,
		},
		"negative limit": {
			request: &types.EventsBlocksRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Limit:             types.Int64(-1),
			},
			err: ErrLimitIsNegative,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.EventsBlocksRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSearchTransactionsRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.SearchTransactionsRequest
		err     error
	}{
		"valid request no operator": {
			request: &types.SearchTransactionsRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: nil,
		},
		"valid request": {
			request: &types.SearchTransactionsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operator:          types.OperatorP(types.AND),
			},
			err: nil,
		},
		"invalid request wrong network": {
			request: &types.SearchTransactionsRequest{
				NetworkIdentifier: wrongNetworkIdentifier,
				Operator:          types.OperatorP(types.OR),
			},
			err: fmt.Errorf(
				"network identifier %s is not supported: %w",
				types.PrintStruct(wrongNetworkIdentifier),
				ErrRequestedNetworkNotSupported,
			),
		},
		"nil request": {
			request: nil,
			err:     ErrSearchTransactionsRequestIsNil,
		},
		"negative max block": {
			request: &types.SearchTransactionsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operator:          types.OperatorP(types.OR),
				MaxBlock:          types.Int64(-1),
			},
			err: ErrMaxBlockInvalid,
		},
		"negative offset": {
			request: &types.SearchTransactionsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operator:          types.OperatorP(types.OR),
				Offset:            types.Int64(-1),
			},
			err: ErrOffsetIsNegative,
		},
		"negative limit": {
			request: &types.SearchTransactionsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operator:          types.OperatorP(types.OR),
				Limit:             types.Int64(-1),
			},
			err: ErrLimitIsNegative,
		},
		"invalid operator": {
			request: &types.SearchTransactionsRequest{
				NetworkIdentifier: validNetworkIdentifier,
				Operator:          types.OperatorP("NOR"),
			},
			err: ErrOperatorInvalid,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := a.SearchTransactionsRequest(test.request)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
