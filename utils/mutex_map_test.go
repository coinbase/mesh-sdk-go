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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestMutexMap(t *testing.T) {
	arr := []string{}
	m := NewMutexMap(DefaultShards)
	g, _ := errgroup.WithContext(context.Background())

	// Lock while adding all locks
	m.GLock()

	// Add another GLock
	g.Go(func() error {
		m.GLock()
		arr = append(arr, "global-b")
		m.GUnlock()
		return nil
	})

	// To test locking, we use channels
	// that will cause deadlock if not executed
	// concurrently.
	a := make(chan struct{})
	b := make(chan struct{})

	g.Go(func() error {
		m.Lock("a", false)
		entry := m.entries.shards[m.entries.shardIndex("a")].entries["a"].(*mutexMapEntry)
		assert.Equal(t, entry.count, 1)
		<-a
		arr = append(arr, "a")
		close(b)
		m.Unlock("a")
		return nil
	})

	g.Go(func() error {
		m.Lock("b", false)
		entry := m.entries.shards[m.entries.shardIndex("b")].entries["b"].(*mutexMapEntry)
		assert.Equal(t, entry.count, 1)
		close(a)
		<-b
		arr = append(arr, "b")
		m.Unlock("b")
		return nil
	})

	time.Sleep(1 * time.Second)

	// Ensure number of expected locks is correct
	totalKeys := len(m.entries.shards[m.entries.shardIndex("a")].entries) +
		len(m.entries.shards[m.entries.shardIndex("b")].entries)
	assert.Equal(t, totalKeys, 0)
	arr = append(arr, "global-a")
	m.GUnlock()
	assert.NoError(t, g.Wait())

	// Check results array to ensure all of the high priority items processed first,
	// followed by all of the low priority items.
	assert.Equal(t, []string{
		"global-a",
		"a",
		"b",
		"global-b", // must wait until all other locks complete
	}, arr)

	// Ensure lock is no longer occupied
	totalKeys = len(m.entries.shards[m.entries.shardIndex("a")].entries) +
		len(m.entries.shards[m.entries.shardIndex("b")].entries)
	assert.Equal(t, totalKeys, 0)
}
