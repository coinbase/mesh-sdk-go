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

package statefulsyncer

import (
	"time"
)

// Option is used to overwrite default values in
// StatefulSyncer construction. Any Option not provided
// falls back to the default value.
type Option func(s *StatefulSyncer)

// WithCacheSize overrides the default cache size.
func WithCacheSize(cacheSize int) Option {
	return func(s *StatefulSyncer) {
		s.cacheSize = cacheSize
	}
}

// WithPastBlockLimit overrides the default past block limit
func WithPastBlockLimit(blocks int) Option {
	return func(s *StatefulSyncer) {
		s.pastBlockLimit = blocks
	}
}

// WithMaxConcurrency overrides the default max concurrency.
func WithMaxConcurrency(concurrency int64) Option {
	return func(s *StatefulSyncer) {
		s.maxConcurrency = concurrency
	}
}

// WithAdjustmentWindow overrides the default adjustment window.
func WithAdjustmentWindow(adjustmentWindow int64) Option {
	return func(s *StatefulSyncer) {
		s.adjustmentWindow = adjustmentWindow
	}
}

// WithPruneSleepTime overrides the default prune sleep time.
func WithPruneSleepTime(sleepTime int) Option {
	return func(s *StatefulSyncer) {
		s.pruneSleepTime = time.Duration(sleepTime) * time.Second
	}
}

// WithSeenConcurrency overrides the number of concurrent
// invocations of BlockSeen we will handle. We default
// to the value of runtime.NumCPU().
func WithSeenConcurrency(concurrency int64) Option {
	return func(s *StatefulSyncer) {
		s.seenSemaphoreSize = concurrency
	}
}

// add a metaData map to fetcher
func WithMetaData(metaData string) Option {
	return func(s *StatefulSyncer) {
		s.metaData = metaData
	}
}
