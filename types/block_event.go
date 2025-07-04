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

// BlockEvent BlockEvent represents the addition or removal of a BlockIdentifier from storage.
// Streaming BlockEvents allows lightweight clients to update their own state without needing to
// implement their own syncing logic.
type BlockEvent struct {
	// sequence is the unique identifier of a BlockEvent within the context of a NetworkIdentifier.
	Sequence        int64            `json:"sequence"`
	BlockIdentifier *BlockIdentifier `json:"block_identifier"`
	Type            BlockEventType   `json:"type"`
}
