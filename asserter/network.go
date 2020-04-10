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
		return errors.New("NetworkIdentifier is nil")
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
// are non-empty strings.
func StringArray(arrName string, arr []string) error {
	if len(arr) == 0 {
		return fmt.Errorf("no %s found", arrName)
	}

	for _, s := range arr {
		if s == "" {
			return fmt.Errorf("%s has an empty string", arrName)
		}
	}

	return nil
}

// NetworkInformation ensures any types.NetworkInformation
// included in types.NetworkStatus or types.SubNetworkStatus is valid.
func NetworkInformation(networkInformation *types.NetworkInformation) error {
	if networkInformation == nil {
		return errors.New("network information is nil")
	}

	if err := BlockIdentifier(networkInformation.CurrentBlockIdentifier); err != nil {
		return err
	}

	if err := Timestamp(networkInformation.CurrentBlockTimestamp); err != nil {
		return err
	}

	if err := BlockIdentifier(networkInformation.GenesisBlockIdentifier); err != nil {
		return err
	}

	for _, peer := range networkInformation.Peers {
		if err := Peer(peer); err != nil {
			return err
		}
	}

	return nil
}

// NetworkStatus ensures a types.NetworkStatus object is valid.
func NetworkStatus(networkStatus *types.NetworkStatus) error {
	if networkStatus == nil {
		return errors.New("network status is nil")
	}

	if err := NetworkIdentifier(networkStatus.NetworkIdentifier); err != nil {
		return err
	}

	if err := NetworkInformation(networkStatus.NetworkInformation); err != nil {
		return err
	}

	return nil
}

// OperationStatuses ensures all items in Options.OperationStatuses
// are valid and that there exists at least 1 successful status.
func OperationStatuses(statuses []*types.OperationStatus) error {
	if len(statuses) == 0 {
		return errors.New("no Options.OperationStatuses found")
	}

	foundSuccessful := false
	for _, status := range statuses {
		if status.Status == "" {
			return errors.New("Operation.Status is missing")
		}

		if status.Successful {
			foundSuccessful = true
		}
	}

	if !foundSuccessful {
		return errors.New("no successful Options.OperationStatuses found")
	}

	return nil
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

// NetworkOptions ensures a types.Options object is valid.
func NetworkOptions(options *types.Options) error {
	if options == nil {
		return errors.New("options is nil")
	}

	if err := OperationStatuses(options.OperationStatuses); err != nil {
		return err
	}

	if err := StringArray("Options.OperationTypes", options.OperationTypes); err != nil {
		return err
	}

	if err := Errors(options.Errors); err != nil {
		return err
	}

	return nil
}

// NetworkStatusResponse orchestrates assertions for all
// components of a types.NetworkStatus.
func NetworkStatusResponse(response *types.NetworkStatusResponse) error {
	for _, network := range response.NetworkStatus {
		if err := NetworkStatus(network); err != nil {
			return err
		}
	}

	if err := Version(response.Version); err != nil {
		return err
	}

	if err := NetworkOptions(response.Options); err != nil {
		return err
	}

	return nil
}
