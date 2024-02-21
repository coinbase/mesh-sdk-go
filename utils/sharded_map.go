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

package utils

import (
	"github.com/segmentio/fasthash/fnv1a"
)

const (
	// DefaultShards is the default number of shards
	// to use in ShardedMap.
	DefaultShards = 256
)

// shardMapEntry governs access to the shard of
// the map contained at a particular index.
type shardMapEntry struct {
	mutex   *PriorityMutex
	entries map[string]interface{}
}

// ShardedMap allows concurrent writes
// to a map by sharding the map into some
// number of independently locked subsections.
type ShardedMap struct {
	shards []*shardMapEntry
}

// NewShardedMap creates a new *ShardedMap
// with some number of shards. The larger the
// number provided for shards, the less lock
// contention there will be.
//
// As a rule of thumb, shards should usually
// be set to the concurrency of the caller.
func NewShardedMap(shards int) *ShardedMap {
	m := &ShardedMap{
		shards: make([]*shardMapEntry, shards),
	}

	for i := 0; i < shards; i++ {
		m.shards[i] = &shardMapEntry{
			entries: map[string]interface{}{},
			mutex:   new(PriorityMutex),
		}
	}

	return m
}

// shardIndex returns the index of the shard
// that could contain the key.
func (m *ShardedMap) shardIndex(key string) int {
	return int(fnv1a.HashString32(key) % uint32(len(m.shards)))
}

// Lock acquires the lock for a shard that could contain
// the key. This syntax allows the caller to perform multiple
// operations while holding the lock for a single shard.
func (m *ShardedMap) Lock(key string, priority bool) map[string]interface{} {
	shardIndex := m.shardIndex(key)
	shard := m.shards[shardIndex]
	shard.mutex.Lock(priority)
	return shard.entries
}

// Unlock releases the lock for a shard that could contain
// the key.
func (m *ShardedMap) Unlock(key string) {
	shardIndex := m.shardIndex(key)
	shard := m.shards[shardIndex]
	shard.mutex.Unlock()
}
