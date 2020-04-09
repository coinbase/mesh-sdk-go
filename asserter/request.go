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
	"github.com/coinbase/rosetta-sdk-go/models"
)

// AccountBalanceRequest ensures that a models.AccountBalanceRequest
// is well-formatted.
func AccountBalanceRequest(request *models.AccountBalanceRequest) error {
	return nil
}

// BlockRequest ensures that a models.BlockRequest
// is well-formatted.
func BlockRequest(request *models.BlockRequest) error {
	return nil
}

// BlockTransactionRequest ensures that a models.BlockTransactionRequest
// is well-formatted.
func BlockTransactionRequest(request *models.BlockTransactionRequest) error {
	return nil
}

// TransactionConstructionRequest ensures that a models.TransactionConstructionRequest
// is well-formatted.
func TransactionConstructionRequest(request *models.TransactionConstructionRequest) error {
	return nil
}

// TransactionSubmitRequest ensures that a models.TransactionSubmitRequest
// is well-formatted.
func TransactionSubmitRequest(request *models.TransactionSubmitRequest) error {
	return nil
}

// MempoolRequest ensures that a models.MempoolRequest
// is well-formatted.
func MempoolRequest(request *models.MempoolRequest) error {
	return nil
}

// MempoolTransactionRequest ensures that a models.MempoolTransactionRequest
// is well-formatted.
func MempoolTransactionRequest(request *models.MempoolTransactionRequest) error {
	return nil
}

// NetworkStatusRequest ensures that a models.NetworkStatusRequest
// is well-formatted.
func NetworkStatusRequest(request *models.NetworkStatusRequest) error {
	return nil
}
