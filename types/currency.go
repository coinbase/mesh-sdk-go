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

// Currency Currency is composed of a canonical Symbol and Decimals. This Decimals value is used to
// convert an Amount.Value from atomic units (Satoshis) to standard units (Bitcoins).
type Currency struct {
	// Canonical symbol associated with a currency.
	Symbol string `json:"symbol"`
	// Number of decimal places in the standard unit representation of the amount. For example, BTC
	// has 8 decimals. Note that it is not possible to represent the value of some currency in
	// atomic units that is not base 10.
	Decimals int32 `json:"decimals"`
	// Any additional information related to the currency itself. For example, it would be useful to
	// populate this object with the contract address of an ERC-20 token.
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}
