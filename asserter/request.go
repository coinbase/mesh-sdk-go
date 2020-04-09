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

	"github.com/coinbase/rosetta-sdk-go/models"
)

// AccountBalanceRequest ensures that a models.AccountBalanceRequest
// is well-formatted.
func AccountBalanceRequest(request *models.AccountBalanceRequest) error {
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

// BlockRequest ensures that a models.BlockRequest
// is well-formatted.
func BlockRequest(request *models.BlockRequest) error {
	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	return PartialBlockIdentifier(request.BlockIdentifier)
}

// BlockTransactionRequest ensures that a models.BlockTransactionRequest
// is well-formatted.
func BlockTransactionRequest(request *models.BlockTransactionRequest) error {
	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	if err := BlockIdentifier(request.BlockIdentifier); err != nil {
		return err
	}

	return TransactionIdentifier(request.TransactionIdentifier)
}

// TransactionConstructionRequest ensures that a models.TransactionConstructionRequest
// is well-formatted.
func TransactionConstructionRequest(request *models.TransactionConstructionRequest) error {
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

// TransactionSubmitRequest ensures that a models.TransactionSubmitRequest
// is well-formatted.
func TransactionSubmitRequest(request *models.TransactionSubmitRequest) error {
	if request.SignedTransaction == "" {
		return errors.New("TransactionSubmitRequest.SignedTransaction is empty")
	}

	return nil
}

// MempoolRequest ensures that a models.MempoolRequest
// is well-formatted.
func MempoolRequest(request *models.MempoolRequest) error {
	return NetworkIdentifier(request.NetworkIdentifier)
}

// MempoolTransactionRequest ensures that a models.MempoolTransactionRequest
// is well-formatted.
func MempoolTransactionRequest(request *models.MempoolTransactionRequest) error {
	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	return TransactionIdentifier(request.TransactionIdentifier)
}

// NetworkStatusRequest ensures that a models.NetworkStatusRequest
// is well-formatted.
func NetworkStatusRequest(request *models.NetworkStatusRequest) error {
	return nil
}
