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
