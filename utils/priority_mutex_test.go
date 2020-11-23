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
