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

package syncer

import (
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Option is used to overwrite default values in
// Syncer construction. Any Option not provided
// falls back to the default value.
type Option func(s *Syncer)

// WithConcurrency overrides the default block concurrency.
func WithConcurrency(concurrency uint64) Option {
	return func(s *Syncer) {
		s.concurrency = concurrency
	}
}

// WithPastBlocks provides the syncer with a cache
// of previously processed blocks to handle reorgs.
func WithPastBlocks(blocks []*types.BlockIdentifier) Option {
	return func(s *Syncer) {
		s.pastBlocks = blocks
	}
}
