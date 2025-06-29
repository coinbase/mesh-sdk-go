// Copyright 2025 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Generated by: OpenAPI Generator (https://openapi-generator.tech)

package types

// NetworkIdentifier The network_identifier specifies which network a particular object is
// associated with.
type NetworkIdentifier struct {
	Blockchain string `json:"blockchain"`
	// If a blockchain has a specific chain-id or network identifier, it should go in this field. It
	// is up to the client to determine which network-specific identifier is mainnet or testnet.
	Network              string                `json:"network"`
	SubNetworkIdentifier *SubNetworkIdentifier `json:"sub_network_identifier,omitempty"`
}
