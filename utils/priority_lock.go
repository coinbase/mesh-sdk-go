package utils

import (
	"sync"
)

// https://github.com/platinummonkey/priority-lock

// PriorityPreferenceLock implements a simple triple-mutex priority lock
// patterns are like:
//   Low Priority would do: lock lowPriorityMutex, wait for high priority groups, lock nextToAccess, lock dataMutex, unlock nextToAccess, do stuff, unlock dataMutex, unlock lowPriorityMutex
//   High Priority would do: increment high priority waiting, lock nextToAccess, lock dataMutex, unlock nextToAccess, do stuff, unlock dataMutex, decrement high priority waiting
type PriorityPreferenceLock struct {
	highPrio []chan struct{}
	lowPrio  []chan struct{}

	m sync.Mutex
	l bool

	// TODO: add random choice of lowPrio to avoid
	// starvation (if desired)
}

func NewPriorityPreferenceLock() *PriorityPreferenceLock {
	lock := PriorityPreferenceLock{
		highPrio: []chan struct{}{},
		lowPrio:  []chan struct{}{},
	}
	return &lock
}

// Lock will acquire a low-priority lock
// it must wait until both low priority and all high priority lock holders are released.
func (lock *PriorityPreferenceLock) Lock() {
	lock.m.Lock()

	if !lock.l {
		lock.l = true
		lock.m.Unlock()
		return
	}

	c := make(chan struct{})
	lock.lowPrio = append(lock.lowPrio, c)

	lock.m.Unlock()
	<-c // don't set lock to false when closing channel
}

// HighPriorityLock will acquire a high-priority lock
// it must still wait until a low-priority lock has been released and then potentially other high priority lock contenders.
func (lock *PriorityPreferenceLock) HighPriorityLock() {
	lock.m.Lock()

	if !lock.l {
		lock.l = true
		lock.m.Unlock()
		return
	}

	c := make(chan struct{})
	lock.highPrio = append(lock.highPrio, c)

	lock.m.Unlock()
	<-c // don't set lock to false when closing channel
}

// Unlock will unlock the low-priority lock
func (lock *PriorityPreferenceLock) Unlock() {
	lock.m.Lock()

	if len(lock.highPrio) > 0 {
		c := lock.highPrio[0]
		lock.highPrio = lock.highPrio[1:]
		lock.m.Unlock()
		close(c)
		return
	}

	if len(lock.lowPrio) > 0 {
		c := lock.lowPrio[0]
		lock.lowPrio = lock.lowPrio[1:]
		lock.m.Unlock()
		close(c)
		return
	}

	lock.l = false
	lock.m.Unlock()
}
