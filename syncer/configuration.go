// Copyright 2024 Coinbase, Inc.
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

// WithCacheSize overrides the default cache size.
func WithCacheSize(cacheSize int) Option {
	return func(s *Syncer) {
		s.cacheSize = cacheSize
	}
}

// WithSizeMultiplier overrides the default size multiplier.
func WithSizeMultiplier(sizeMultiplier float64) Option {
	return func(s *Syncer) {
		s.sizeMultiplier = sizeMultiplier
	}
}

// WithPastBlocks provides the syncer with a cache
// of previously processed blocks to handle reorgs.
func WithPastBlocks(blocks []*types.BlockIdentifier) Option {
	return func(s *Syncer) {
		s.pastBlocks = blocks
	}
}

// WithPastBlockLimit overrides the default past block limit
func WithPastBlockLimit(blocks int) Option {
	return func(s *Syncer) {
		s.pastBlockLimit = blocks
	}
}

// WithMaxConcurrency overrides the default max concurrency.
func WithMaxConcurrency(concurrency int64) Option {
	return func(s *Syncer) {
		s.maxConcurrency = concurrency
	}
}

// WithAdjustmentWindow overrides the default adjustment window.
func WithAdjustmentWindow(adjustmentWindow int64) Option {
	return func(s *Syncer) {
		s.adjustmentWindow = adjustmentWindow
	}
}

// add a info map to Syncer
func WithMetaData(metaData string) Option {
	return func(s *Syncer) {
		s.metaData = metaData
	}
}
