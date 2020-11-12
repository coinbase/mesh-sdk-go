package utils

import (
	"sync"
)

// PriorityPreferenceLock implements a simple triple-mutex priority lock
// patterns are like:
//   Low Priority would do: lock lowPriorityMutex, wait for high priority groups, lock nextToAccess, lock dataMutex, unlock nextToAccess, do stuff, unlock dataMutex, unlock lowPriorityMutex
//   High Priority would do: increment high priority waiting, lock nextToAccess, lock dataMutex, unlock nextToAccess, do stuff, unlock dataMutex, decrement high priority waiting
type PriorityPreferenceLock struct {
	dataMutex           sync.Mutex
	nextToAccess        sync.Mutex
	lowPriorityMutex    sync.Mutex
	highPriorityWaiting sync.WaitGroup
}

func NewPriorityPreferenceLock() *PriorityPreferenceLock {
	lock := PriorityPreferenceLock{
		highPriorityWaiting: sync.WaitGroup{},
	}
	return &lock
}

// Lock will acquire a low-priority lock
// it must wait until both low priority and all high priority lock holders are released.
func (lock *PriorityPreferenceLock) Lock() {
	lock.lowPriorityMutex.Lock()
	lock.highPriorityWaiting.Wait()
	lock.nextToAccess.Lock()
	lock.dataMutex.Lock()
	lock.nextToAccess.Unlock()
}

// Unlock will unlock the low-priority lock
func (lock *PriorityPreferenceLock) Unlock() {
	lock.dataMutex.Unlock()
	lock.lowPriorityMutex.Unlock()
}

// HighPriorityLock will acquire a high-priority lock
// it must still wait until a low-priority lock has been released and then potentially other high priority lock contenders.
func (lock *PriorityPreferenceLock) HighPriorityLock() {
	lock.highPriorityWaiting.Add(1)
	lock.nextToAccess.Lock()
	lock.dataMutex.Lock()
	lock.nextToAccess.Unlock()
}

// HighPriorityUnlock will unlock the high-priority lock
func (lock *PriorityPreferenceLock) HighPriorityUnlock() {
	lock.dataMutex.Unlock()
	lock.highPriorityWaiting.Done()
}
