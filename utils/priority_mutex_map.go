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
	"sync"
)

// MutexMap is a struct that allows for
// acquiring a *PriorityMutex via a string identifier
// or for acquiring a global mutex that blocks
// the acquisition of any identifier mutexes.
//
// This is useful for coordinating concurrent, non-overlapping
// writes in the storage package.
type MutexMap struct {
	entries     map[string]*mutexMapEntry
	mutex       sync.Mutex
	globalMutex sync.RWMutex
}

// mutexMapEntry is the primitive used
// to track claimed *PriorityMutex.
type mutexMapEntry struct {
	lock  *PriorityMutex
	count int
}

// NewMutexMap returns a new *MutexMap.
func NewMutexMap() *MutexMap {
	return &MutexMap{
		entries: map[string]*mutexMapEntry{},
	}
}

// GLock acquires an exclusive lock across
// an entire *MutexMap.
func (m *MutexMap) GLock() {
	m.globalMutex.Lock()
}

// GUnlock releases an exclusive lock
// held for an entire *MutexMap.
func (m *MutexMap) GUnlock() {
	m.globalMutex.Unlock()
}

// Lock acquires a lock for a particular identifier, as long
// as no other caller has the global mutex or a lock
// by the same identifier.
func (m *MutexMap) Lock(identifier string, priority bool) {
	// We acquire a RLock on m.globalMutex before
	// acquiring our identifier lock to ensure no
	// goroutine holds an identifier mutex while
	// the m.globalMutex is also held.
	m.globalMutex.RLock()

	// We acquire m when adding items to m.table
	// so that we don't accidentally overwrite
	// lock created by another goroutine.
	m.mutex.Lock()
	l, ok := m.entries[identifier]
	if !ok {
		l = &mutexMapEntry{
			lock: new(PriorityMutex),
		}
		m.entries[identifier] = l
	}
	l.count++
	m.mutex.Unlock()

	// Once we have a m.globalMutex.RLock, it is
	// safe to acquire an identifier lock.
	l.lock.Lock(priority)
}

// Unlock releases a lock held for a particular identifier.
func (m *MutexMap) Unlock(identifier string) {
	// The lock at a particular identifier MUST
	// exist by the time we unlock, otherwise
	// it would not have been possible to get
	// the lock to begin with.
	m.mutex.Lock()
	entry := m.entries[identifier]
	if entry.count <= 1 { // this should never be < 0
		delete(m.entries, identifier)
	} else {
		entry.count--
		entry.lock.Unlock()
	}
	m.mutex.Unlock()

	// We release the globalMutex after unlocking
	// the identifier lock, otherwise it would be possible
	// for GLock to be acquired while still holding some
	// lock in the table.
	m.globalMutex.RUnlock()
}
