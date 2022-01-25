// Copyright 2022 Coinbase, Inc.
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

// Generated by: OpenAPI Generator (https://openapi-generator.tech)

package types

// SyncStatus SyncStatus is used to provide additional context about an implementation's sync
// status. This object is often used by implementations to indicate healthiness when block data
// cannot be queried until some sync phase completes or cannot be determined by comparing the
// timestamp of the most recent block with the current time.
type SyncStatus struct {
	// CurrentIndex is the index of the last synced block in the current stage. This is a separate
	// field from current_block_identifier in NetworkStatusResponse because blocks with indices up
	// to and including the current_index may not yet be queryable by the caller. To reiterate, all
	// indices up to and including current_block_identifier in NetworkStatusResponse must be
	// queryable via the /block endpoint (excluding indices less than oldest_block_identifier).
	CurrentIndex *int64 `json:"current_index,omitempty"`
	// TargetIndex is the index of the block that the implementation is attempting to sync to in the
	// current stage.
	TargetIndex *int64 `json:"target_index,omitempty"`
	// Stage is the phase of the sync process.
	Stage *string `json:"stage,omitempty"`
	// sycned is a boolean that indicates if an implementation has synced up to the most recent
	// block. If this field is not populated, the caller should rely on a traditional tip timestamp
	// comparison to determine if an implementation is synced. This field is particularly useful for
	// quiescent blockchains (blocks only produced when there are pending transactions). In these
	// blockchains, the most recent block could have a timestamp far behind the current time but the
	// node could be healthy and at tip.
	Synced *bool `json:"synced,omitempty"`
}
