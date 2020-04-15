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

// ConstructionMetadataRequest ensures that a types.ConstructionMetadataRequest
// is well-formatted.
func ConstructionMetadataRequest(request *types.ConstructionMetadataRequest) error {
	if request == nil {
		return errors.New("ConstructionMetadataRequest is nil")
	}

	if err := NetworkIdentifier(request.NetworkIdentifier); err != nil {
		return err
	}

	if request.Options == nil {
		return errors.New("ConstructionMetadataRequest.Options is nil")
	}

	return nil
}

// ConstructionSubmitRequest ensures that a types.ConstructionSubmitRequest
// is well-formatted.
func ConstructionSubmitRequest(request *types.ConstructionSubmitRequest) error {
	if request == nil {
		return errors.New("ConstructionSubmitRequest is nil")
	}

	if request.SignedTransaction == "" {
		return errors.New("ConstructionSubmitRequest.SignedTransaction is empty")
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

// MetadataRequest ensures that a types.MetadataRequest
// is well-formatted.
func MetadataRequest(request *types.MetadataRequest) error {
	if request == nil {
		return errors.New("MetadataRequest is nil")
	}

	return nil
}

// NetworkRequest ensures that a types.NetworkRequest
// is well-formatted.
func NetworkRequest(request *types.NetworkRequest) error {
	if request == nil {
		return errors.New("NetworkRequest is nil")
	}

	return NetworkIdentifier(request.NetworkIdentifier)
}
