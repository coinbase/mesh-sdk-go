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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

// A wrapper around the PriorityMutex being tested to properly assert invariants without the
// possibility of race conditions. The `lockWg` completes after the low/high queue is appended to
// but before the blocking `wait` call so we can assert invariants on the size of the queue.
type wrappedPriorityMutex struct {
	*PriorityMutex
	lockWg sync.WaitGroup
}

func newWrappedPriorityMutex(numLockCalls int) *wrappedPriorityMutex {
	w := &wrappedPriorityMutex{PriorityMutex: new(PriorityMutex)}
	w.lockWg.Add(numLockCalls)
	return w
}

func (w *wrappedPriorityMutex) Lock(priority bool) {
	c := w.PriorityMutex.lockInternal(priority)
	if c != nil {
		w.lockWg.Done()
		<-c
	}
}

func (w *wrappedPriorityMutex) Wait() {
	w.lockWg.Wait()
}

func TestPriorityMutex(t *testing.T) {
	arr := []bool{}
	expected := make([]bool, 60)
	l := newWrappedPriorityMutex(60)
	g, _ := errgroup.WithContext(context.Background())

	// Lock while adding all locks
	l.Lock(true)

	// Add a bunch of low prio items
	for i := 0; i < 50; i++ {
		expected[i+10] = false
		g.Go(func() error {
			l.Lock(false)
			arr = append(arr, false)
			l.Unlock()
			return nil
		})
	}

	// Add a few high prio items
	for i := 0; i < 10; i++ {
		expected[i] = true
		g.Go(func() error {
			l.Lock(true)
			arr = append(arr, true)
			l.Unlock()
			return nil
		})
	}

	// Wait for all goroutines to ask for lock
	l.Wait()

	// Ensure number of expected locks is correct
	assert.Len(t, l.high, 10)
	assert.Len(t, l.low, 50)

	l.Unlock()
	assert.NoError(t, g.Wait())

	// Check results array to ensure all of the high priority items processed first,
	// followed by all of the low priority items.
	assert.Equal(t, expected, arr)

	// Ensure lock is no longer occupied
	assert.False(t, l.lock)
}
