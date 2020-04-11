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

	"github.com/coinbase/rosetta-sdk-go/types"
)

// AccountBalanceRequest ensures that a types.AccountBalanceRequest
// is well-formatted.
func AccountBalanceRequest(request *types.AccountBalanceRequest) error {
	if request == nil {
		return errors.New("AccountBalanceRequest is nil")
	}

	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	if err := AccountIdentifier(request.AccountIdentifier); err != nil {
		return err
	}

	if request.BlockIdentifier == nil {
		return nil
	}

	return PartialBlockIdentifier(request.BlockIdentifier)
}

// BlockRequest ensures that a types.BlockRequest
// is well-formatted.
func BlockRequest(request *types.BlockRequest) error {
	if request == nil {
		return errors.New("BlockRequest is nil")
	}

	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	return PartialBlockIdentifier(request.BlockIdentifier)
}

// BlockTransactionRequest ensures that a types.BlockTransactionRequest
// is well-formatted.
func BlockTransactionRequest(request *types.BlockTransactionRequest) error {
	if request == nil {
		return errors.New("BlockTransactionRequest is nil")
	}

	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	if err := BlockIdentifier(request.BlockIdentifier); err != nil {
		return err
	}

	return TransactionIdentifier(request.TransactionIdentifier)
}

// TransactionConstructionRequest ensures that a types.TransactionConstructionRequest
// is well-formatted.
func TransactionConstructionRequest(request *types.TransactionConstructionRequest) error {
	if request == nil {
		return errors.New("TransactionConstructionRequest is nil")
	}

	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	if err := AccountIdentifier(request.AccountIdentifier); err != nil {
		return err
	}

	if request.Method != nil && *request.Method == "" {
		return errors.New("TransactionConstructionRequest.Method is empty")
	}

	return nil
}

// TransactionSubmitRequest ensures that a types.TransactionSubmitRequest
// is well-formatted.
func TransactionSubmitRequest(request *types.TransactionSubmitRequest) error {
	if request == nil {
		return errors.New("TransactionSubmitRequest is nil")
	}

	if request.SignedTransaction == "" {
		return errors.New("TransactionSubmitRequest.SignedTransaction is empty")
	}

	return nil
}

// MempoolRequest ensures that a types.MempoolRequest
// is well-formatted.
func MempoolRequest(request *types.MempoolRequest) error {
	if request == nil {
		return errors.New("MempoolRequest is nil")
	}

	return NetworkIdentifier(request.NetworkIdentifier)
}

// MempoolTransactionRequest ensures that a types.MempoolTransactionRequest
// is well-formatted.
func MempoolTransactionRequest(request *types.MempoolTransactionRequest) error {
	if request == nil {
		return errors.New("MempoolTransactionRequest is nil")
	}

	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	return TransactionIdentifier(request.TransactionIdentifier)
}

// NetworkStatusRequest ensures that a types.NetworkStatusRequest
// is well-formatted.
func NetworkStatusRequest(request *types.NetworkStatusRequest) error {
	if request == nil {
		return errors.New("NetworkStatusRequest is nil")
	}

	return nil
}
