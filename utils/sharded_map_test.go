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

func TestShardedMap(t *testing.T) {
	m := NewShardedMap(2)
	g, _ := errgroup.WithContext(context.Background())

	// To test locking, we use channels
	// that will cause deadlock if not executed
	// concurrently.
	a := make(chan struct{})
	b := make(chan struct{})

	g.Go(func() error {
		s := m.Lock("a", false)
		assert.Len(t, s, 0)
		s["test"] = "a"
		<-a
		close(b)
		m.Unlock("a")
		return nil
	})

	g.Go(func() error {
		s := m.Lock("b", false)
		assert.Len(t, s, 0)
		s["test"] = "b"
		close(a)
		<-b
		m.Unlock("b")
		return nil
	})

	time.Sleep(1 * time.Second)
	assert.NoError(t, g.Wait())

	// Ensure keys set correctly
	s := m.Lock("a", false)
	assert.Len(t, s, 1)
	assert.Equal(t, s["test"], "a")
	m.Unlock("a")

	s = m.Lock("b", false)
	assert.Len(t, s, 1)
	assert.Equal(t, s["test"], "b")
	m.Unlock("b")
}
