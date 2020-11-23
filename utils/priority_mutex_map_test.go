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
	m := NewMutexMap()
	g, _ := errgroup.WithContext(context.Background())

	// Lock while adding all locks
	m.GLock()

	// Add another GLock
	g.Go(func() error {
		m.GLock()
		arr = append(arr, "global")
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
		assert.Equal(t, m.table["a"].count, 1)
		<-a
		arr = append(arr, "a")
		close(b)
		m.Unlock("a")
		return nil
	})

	g.Go(func() error {
		m.Lock("b", false)
		assert.Equal(t, m.table["b"].count, 1)
		close(a)
		<-b
		arr = append(arr, "b")
		m.Unlock("b")
		return nil
	})

	time.Sleep(1 * time.Second)

	// Ensure number of expected locks is correct
	assert.Len(t, m.table, 0)
	arr = append(arr, "global")
	m.GUnlock()
	assert.NoError(t, g.Wait())

	// Check results array to ensure all of the high priority items processed first,
	// followed by all of the low priority items.
	assert.Equal(t, []string{
		"global",
		"a",
		"b",
		"global", // must wait until all other locks complete
	}, arr)

	// Ensure lock is no longer occupied
	assert.Len(t, m.table, 0)
}
