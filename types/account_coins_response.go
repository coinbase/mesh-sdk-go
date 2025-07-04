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

// AccountCoinsResponse AccountCoinsResponse is returned on the /account/coins endpoint and includes
// all unspent Coins owned by an AccountIdentifier.
type AccountCoinsResponse struct {
	BlockIdentifier *BlockIdentifier `json:"block_identifier"`
	// If a blockchain is UTXO-based, all unspent Coins owned by an account_identifier should be
	// returned alongside the balance. It is highly recommended to populate this field so that users
	// of the Rosetta API implementation don't need to maintain their own indexer to track their
	// UTXOs.
	Coins []*Coin `json:"coins"`
	// Account-based blockchains that utilize a nonce or sequence number should include that number
	// in the metadata. This number could be unique to the identifier or global across the account
	// address.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}
