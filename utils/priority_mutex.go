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
	"sync"
)

// PriorityMutex is a special type of mutex
// that allows callers to request priority
// over other callers. This can be useful
// if there is a "hot path" in an application
// that requires lock access.
//
// WARNING: It is possible to cause lock starvation
// if not careful (i.e. only high priority callers
// ever do work).
type PriorityMutex struct {
	high []chan struct{}
	low  []chan struct{}

	mutex sync.Mutex
	lock  bool
}

// Lock attempts to acquire either a high or low
// priority mutex. When priority is true, a lock
// will be granted before other low priority callers.
func (m *PriorityMutex) Lock(priority bool) {
	c := m.lockInternal(priority)
	if c != nil {
		<-c
	}
}

func (m *PriorityMutex) lockInternal(priority bool) <-chan struct{} {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.lock {
		m.lock = true
		return nil
	}

	c := make(chan struct{})
	if priority {
		m.high = append(m.high, c)
	} else {
		m.low = append(m.low, c)
	}

	return c
}

// Unlock selects the next highest priority lock
// to grant. If there are no locks to grant, it
// sets the value of m.lock to false.
func (m *PriorityMutex) Unlock() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.high) > 0 {
		c := m.high[0]
		m.high = m.high[1:]
		close(c)
		return
	}

	if len(m.low) > 0 {
		c := m.low[0]
		m.low = m.low[1:]
		close(c)
		return
	}

	// We only set m.lock to false when there are
	// no items to unlock because it could cause
	// lock contention for the next lock to fetch it.
	m.lock = false
}
