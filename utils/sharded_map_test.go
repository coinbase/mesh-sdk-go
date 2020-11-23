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
