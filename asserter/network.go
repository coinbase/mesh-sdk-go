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

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"
)

// SubNetworkIdentifier asserts a rosetta.SubNetworkIdentifer is valid (if not nil).
func SubNetworkIdentifier(subNetworkIdentifier *rosetta.SubNetworkIdentifier) error {
	if subNetworkIdentifier == nil {
		return nil
	}

	if subNetworkIdentifier.SubNetwork == "" {
		return errors.New("NetworkIdentifier.SubNetworkIdentifier.SubNetwork is missing")
	}

	return nil
}

// NetworkIdentifier ensures a rosetta.NetworkIdentifier has
// a valid blockchain and network.
func NetworkIdentifier(network *rosetta.NetworkIdentifier) error {
	if network == nil || network.Blockchain == "" {
		return errors.New("NetworkIdentifier.Blockchain is missing")
	}

	if network.Network == "" {
		return errors.New("NetworkIdentifier.Network is missing")
	}

	return SubNetworkIdentifier(network.SubNetworkIdentifier)
}

// PartialNetworkIdentifier ensures a rosetta.PartialNetworkIdentifier has
// a valid blockchain and network.
func PartialNetworkIdentifier(identifier *rosetta.PartialNetworkIdentifier) error {
	if identifier == nil || identifier.Blockchain == "" {
		return errors.New("PartialNetworkIdentifier.Blockchain is missing")
	}

	if identifier.Network == "" {
		return errors.New("PartialNetworkIdentifier.Network is missing")
	}

	return nil
}

// Peer ensures a rosetta.Peer has a valid peer_id.
func Peer(peer *rosetta.Peer) error {
	if peer == nil || peer.PeerID == "" {
		return errors.New("Peer.PeerID is missing")
	}

	return nil
}

// SubNetworkStatus ensures a rosetta.SubNetworkStatus is valid.
func SubNetworkStatus(subNetwork *rosetta.SubNetworkStatus) error {
	if err := SubNetworkIdentifier(subNetwork.SubNetworkIdentifier); err != nil {
		return err
	}

	if err := NetworkInformation(subNetwork.NetworkInformation); err != nil {
		return err
	}

	return nil
}

// Version ensures the version of the node is
// returned.
func Version(version *rosetta.Version) error {
	if version == nil {
		return errors.New("version is nil")
	}

	// Assert RosettaVersion against what client can assert
	// against. This could be multiple versions.
	if version.RosettaVersion != rosetta.APIVersion {
		return fmt.Errorf("Version.RosettaVersion %s is invalid", version.RosettaVersion)
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

// SupportedMethods ensures any methods
// returned by the Rosetta Interface server are valid.
func SupportedMethods(methods []string) error {
	if len(methods) == 0 {
		return errors.New("no Options.Methods found")
	}

	allowedMethods := []string{
		"/block",
		"/block/transaction",
		"/mempool",
		"/mempool/transaction",
		"/account/balance",
		"/account/transactions",
		"/construction/metadata",
		"/construction/submit",
	}

	for _, method := range methods {
		if !contains(allowedMethods, method) {
			return fmt.Errorf("%s is not a valid method", method)
		}
	}

	return nil
}

// NetworkInformation ensures any rosetta.NetworkInformation
// included in rosetta.NetworkStatus or rosetta.SubNetworkStatus is valid.
func NetworkInformation(networkInformation *rosetta.NetworkInformation) error {
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

// NetworkStatus ensures a rosetta.NetworkStatus object is valid.
func NetworkStatus(networkStatus *rosetta.NetworkStatus) error {
	if networkStatus == nil {
		return errors.New("network status is nil")
	}

	if err := PartialNetworkIdentifier(networkStatus.NetworkIdentifier); err != nil {
		return err
	}

	if err := NetworkInformation(networkStatus.NetworkInformation); err != nil {
		return err
	}

	return nil
}

// OperationStatuses ensures all items in Options.OperationStatuses
// are valid and that there exists at least 1 successful status.
func OperationStatuses(statuses []*rosetta.OperationStatus) error {
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

// SubmissionStatuses ensures all items in Options.SubmissionStatus
// are valid and that there exists at least 1 successful status.
func SubmissionStatuses(statuses []*rosetta.SubmissionStatus) error {
	if len(statuses) == 0 {
		return errors.New("no Options.SubmissionStatuses found")
	}

	foundSuccessful := false
	for _, status := range statuses {
		if status.Status == "" {
			return errors.New("submission status is missing")
		}

		if status.Successful {
			foundSuccessful = true
		}
	}

	if !foundSuccessful {
		return errors.New("no successful Options.SubmissionStatuses found")
	}

	return nil
}

// NetworkOptions ensures a rosetta.Options object is valid.
func NetworkOptions(options *rosetta.Options) error {
	if options == nil {
		return errors.New("options is nil")
	}

	if err := SupportedMethods(options.Methods); err != nil {
		return err
	}

	if err := OperationStatuses(options.OperationStatuses); err != nil {
		return err
	}

	if err := StringArray("Options.OperationTypes", options.OperationTypes); err != nil {
		return err
	}

	if err := SubmissionStatuses(options.SubmissionStatuses); err != nil {
		return err
	}

	return nil
}

// NetworkStatusResponse orchestrates assertions for all
// components of a rosetta.NetworkStatus.
func NetworkStatusResponse(response *rosetta.NetworkStatusResponse) error {
	if err := NetworkStatus(response.NetworkStatus); err != nil {
		return err
	}

	if response.SubNetworkStatus != nil {
		for _, subNetwork := range response.SubNetworkStatus {
			if err := SubNetworkStatus(subNetwork); err != nil {
				return err
			}
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
