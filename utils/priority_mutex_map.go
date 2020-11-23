package utils

import (
	"sync"
)

type mutexMapEntry struct {
	l     *PriorityMutex
	count int
}

// MutexMap is a struct that allows for
// acquiring a *PriorityMutex via a string identifier
// or for acquiring a global mutex that blocks
// the acquisition of any identifier mutexes.
//
// This is useful for coordinating concurrent, non-overlapping
// writes in the storage package.
type MutexMap struct {
	table map[string]*mutexMapEntry
	m     sync.Mutex

	globalMutex sync.RWMutex
}

// NewMutexMap returns a new *MutexMap.
func NewMutexMap() *MutexMap {
	return &MutexMap{
		table: map[string]*mutexMapEntry{},
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
	m.m.Lock()
	l, ok := m.table[identifier]
	if !ok {
		l = &mutexMapEntry{
			l: new(PriorityMutex),
		}
		m.table[identifier] = l
	}
	l.count++
	m.m.Unlock()

	// Once we have a m.globalMutex.RLock, it is
	// safe to acquire an identifier lock.
	l.l.Lock(priority)
}

// Unlock releases a lock held for a particular identifier.
func (m *MutexMap) Unlock(identifier string) {
	// The lock at a particular identifier MUST
	// exist by the time we unlock, otherwise
	// it would not have been possible to get
	// the lock to begin with.
	m.m.Lock()
	entry := m.table[identifier]
	if entry.count <= 1 { // this should never be < 0
		delete(m.table, identifier)
	} else {
		entry.count--
		entry.l.Unlock()
	}
	m.m.Unlock()

	// We release the globalMutex after unlocking
	// the identifier lock, otherwise it would be possible
	// for GLock to be acquired while still holding some
	// lock in the table.
	m.globalMutex.RUnlock()
}
