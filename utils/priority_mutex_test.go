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

package utils

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestPriorityMutex(t *testing.T) {
	arr := []bool{}
	expected := make([]bool, 60)
	l := NewPriorityMutex()
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)

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
	time.Sleep(1 * time.Second)

	// Ensure number of expected locks is correct
	assert.Len(t, l.high, 10)
	assert.Len(t, l.low, 50)

	l.Unlock()
	assert.NoError(t, g.Wait())

	// Check array for all high prio, remaining low prio
	assert.Equal(t, expected, arr)
}
