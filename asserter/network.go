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

	"github.com/coinbase/rosetta-sdk-go/types"
)

// SubNetworkIdentifier asserts a types.SubNetworkIdentifer is valid (if not nil).
func SubNetworkIdentifier(subNetworkIdentifier *types.SubNetworkIdentifier) error {
	if subNetworkIdentifier == nil {
		return nil
	}

	if subNetworkIdentifier.Network == "" {
		return errors.New("NetworkIdentifier.SubNetworkIdentifier.Network is missing")
	}

	return nil
}

// NetworkIdentifier ensures a types.NetworkIdentifier has
// a valid blockchain and network.
func NetworkIdentifier(network *types.NetworkIdentifier) error {
	if network == nil {
		return errors.New("NetworkIdentifier is nil")
	}

	if network.Blockchain == "" {
		return errors.New("NetworkIdentifier.Blockchain is missing")
	}

	if network.Network == "" {
		return errors.New("NetworkIdentifier.Network is missing")
	}

	return SubNetworkIdentifier(network.SubNetworkIdentifier)
}

// Peer ensures a types.Peer has a valid peer_id.
func Peer(peer *types.Peer) error {
	if peer == nil || peer.PeerID == "" {
		return errors.New("Peer.PeerID is missing")
	}

	return nil
}

// Version ensures the version of the node is
// returned.
func Version(version *types.Version) error {
	if version == nil {
		return errors.New("version is nil")
	}

	if version.NodeVersion == "" {
		return errors.New("Version.NodeVersion is missing")
	}

	if version.MiddlewareVersion != nil && *version.MiddlewareVersion == "" {
		return errors.New("Version.MiddlewareVersion is missing")
	}

	return nil
}

// StringArray ensures all strings in an array
// are non-empty strings and not duplicates.
func StringArray(arrName string, arr []string) error {
	if len(arr) == 0 {
		return fmt.Errorf("no %s found", arrName)
	}

	parsed := make([]string, len(arr))
	for i, s := range arr {
		if s == "" {
			return fmt.Errorf("%s has an empty string", arrName)
		}

		if containsString(parsed, s) {
			return fmt.Errorf("%s contains a duplicate %s", arrName, s)
		}

		parsed[i] = s
	}

	return nil
}

// NetworkStatusResponse ensures any types.NetworkStatusResponse
// is valid.
func NetworkStatusResponse(response *types.NetworkStatusResponse) error {
	if response == nil {
		return errors.New("network status response is nil")
	}

	if err := BlockIdentifier(response.CurrentBlockIdentifier); err != nil {
		return err
	}

	if err := Timestamp(response.CurrentBlockTimestamp); err != nil {
		return err
	}

	if err := BlockIdentifier(response.GenesisBlockIdentifier); err != nil {
		return err
	}

	for _, peer := range response.Peers {
		if err := Peer(peer); err != nil {
			return err
		}
	}

	return nil
}

// OperationStatuses ensures all items in Options.Allow.OperationStatuses
// are valid and that there exists at least 1 successful status.
func OperationStatuses(statuses []*types.OperationStatus) error {
	if len(statuses) == 0 {
		return errors.New("no Allow.OperationStatuses found")
	}

	statusStatuses := make([]string, len(statuses))
	foundSuccessful := false
	for i, status := range statuses {
		if status.Status == "" {
			return errors.New("Operation.Status is missing")
		}

		if status.Successful {
			foundSuccessful = true
		}

		statusStatuses[i] = status.Status
	}

	if !foundSuccessful {
		return errors.New("no successful Allow.OperationStatuses found")
	}

	return StringArray("Allow.OperationStatuses", statusStatuses)
}

// OperationTypes ensures all items in Options.Allow.OperationStatuses
// are valid and that there are no repeats.
func OperationTypes(types []string) error {
	return StringArray("Allow.OperationTypes", types)
}

// Error ensures a types.Error is valid.
func Error(err *types.Error) error {
	if err == nil {
		return errors.New("Error is nil")
	}

	if err.Code < 0 {
		return errors.New("Error.Code is negative")
	}

	if err.Message == "" {
		return errors.New("Error.Message is missing")
	}

	return nil
}

// Errors ensures each types.Error in a slice is valid
// and that there is no error code collision.
func Errors(rosettaErrors []*types.Error) error {
	statusCodes := map[int32]struct{}{}

	for _, rosettaError := range rosettaErrors {
		if err := Error(rosettaError); err != nil {
			return err
		}

		_, exists := statusCodes[rosettaError.Code]
		if exists {
			return errors.New("error code used multiple times")
		}

		statusCodes[rosettaError.Code] = struct{}{}
	}

	return nil
}

// Allow ensures a types.Allow object is valid.
func Allow(allowed *types.Allow) error {
	if allowed == nil {
		return errors.New("Allow is nil")
	}

	if err := OperationStatuses(allowed.OperationStatuses); err != nil {
		return err
	}

	if err := OperationTypes(allowed.OperationTypes); err != nil {
		return err
	}

	if err := Errors(allowed.Errors); err != nil {
		return err
	}

	return nil
}

// NetworkOptionsResponse ensures a types.NetworkOptionsResponse object is valid.
func NetworkOptionsResponse(options *types.NetworkOptionsResponse) error {
	if options == nil {
		return errors.New("options is nil")
	}

	if err := Version(options.Version); err != nil {
		return err
	}

	return Allow(options.Allow)
}

// containsNetworkIdentifier returns a boolean indicating if a
// *types.NetworkIdentifier is contained within a slice of
// *types.NetworkIdentifier. The check for equality takes
// into account everything within the types.NetworkIdentifier
// struct (including currency.Metadata).
func containsNetworkIdentifier(
	networks []*types.NetworkIdentifier,
	network *types.NetworkIdentifier,
) bool {
	for _, net := range networks {
		if types.Hash(net) == types.Hash(network) {
			return true
		}
	}

	return false
}

// NetworkListResponse ensures a types.NetworkListResponse object is valid.
func NetworkListResponse(response *types.NetworkListResponse) error {
	if response == nil {
		return errors.New("NetworkListResponse is nil")
	}

	seen := make([]*types.NetworkIdentifier, 0)
	for _, network := range response.NetworkIdentifiers {
		if err := NetworkIdentifier(network); err != nil {
			return err
		}

		if containsNetworkIdentifier(seen, network) {
			return errors.New("NetworkListResponse.Networks contains duplicates")
		}

		seen = append(seen, network)
	}

	return nil
}
