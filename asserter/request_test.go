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
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

var (
	validNetworkIdentifier = &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Mainnet",
	}

	validAccountIdentifier = &types.AccountIdentifier{
		Address: "acct1",
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

	validMethod = "transfer"
)

func TestAccountBalanceRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.AccountBalanceRequest
		err     error
	}{
		"valid request": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: nil,
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
			err: nil,
		},
		"invalid historical request": {
			request: &types.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier:   &types.PartialBlockIdentifier{},
			},
			err: errors.New("neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := AccountBalanceRequest(test.request)
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
			err := BlockRequest(test.request)
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
			err := BlockTransactionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestTransactionConstructionRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.TransactionConstructionRequest
		err     error
	}{
		"valid request": {
			request: &types.TransactionConstructionRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: nil,
		},
		"valid request with method": {
			request: &types.TransactionConstructionRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				Method:            &validMethod,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("TransactionConstructionRequest is nil"),
		},
		"missing network": {
			request: &types.TransactionConstructionRequest{
				AccountIdentifier: validAccountIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing account identifier": {
			request: &types.TransactionConstructionRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("Account is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := TransactionConstructionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestTransactionSubmitRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.TransactionSubmitRequest
		err     error
	}{
		"valid request": {
			request: &types.TransactionSubmitRequest{
				SignedTransaction: "tx",
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("TransactionSubmitRequest is nil"),
		},
		"empty tx": {
			request: &types.TransactionSubmitRequest{},
			err:     errors.New("TransactionSubmitRequest.SignedTransaction is empty"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := TransactionSubmitRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestMempoolRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.MempoolRequest
		err     error
	}{
		"valid request": {
			request: &types.MempoolRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("MempoolRequest is nil"),
		},
		"empty tx": {
			request: &types.MempoolRequest{},
			err:     errors.New("NetworkIdentifier is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := MempoolRequest(test.request)
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
			err := MempoolTransactionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestNetworkStatusRequest(t *testing.T) {
	var tests = map[string]struct {
		request *types.NetworkStatusRequest
		err     error
	}{
		"valid request": {
			request: &types.NetworkStatusRequest{},
			err:     nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("NetworkStatusRequest is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := NetworkStatusRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}
