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

package errs

import "errors"

var (
	ErrSubNetworkIdentifierInvalid = errors.New(
		"NetworkIdentifier.SubNetworkIdentifier.Network is missing",
	)

	ErrNetworkIdentifierIsNil = errors.New("NetworkIdentifier is nil")

	ErrNetworkIdentifierBlockchainMissing = errors.New("NetworkIdentifier.Blockchain is missing")

	ErrNetworkIdentifierNetworkMissing = errors.New("NetworkIdentifier.Network is missing")

	ErrPeerIDMissing = errors.New("Peer.PeerID is missing")

	ErrVersionIsNil = errors.New("version is nil")

	ErrVersionNodeVersionMissing = errors.New("Version.NodeVersion is missing")

	ErrVersionMiddlewareVersionMissing = errors.New("Version.MiddlewareVersion is missing")

	ErrNetworkStatusResponseIsNil = errors.New("network status response is nil")

	ErrNoAllowedOperationStatuses = errors.New("no Allow.OperationStatuses found")

	ErrNoSuccessfulAllowedOperationStatuses = errors.New(
		"no successful Allow.OperationStatuses found",
	)

	ErrErrorIsNil = errors.New("Error is nil")

	ErrErrorCodeIsNeg = errors.New("Error.Code is negative")

	ErrErrorMessageMissing = errors.New("Error.Message is missing")

	ErrErrorCodeUsedMultipleTimes = errors.New("error code used multiple times")

	ErrAllowIsNil = errors.New("Allow is nil")

	ErrNetworkOptionsResponseIsNil = errors.New("options is nil")

	ErrNetworkListResponseIsNil = errors.New("NetworkListResponse is nil")

	ErrNetworkListResponseNetworksContinsDuplicates = errors.New(
		"NetworkListResponse.Networks contains duplicates",
	)
)
