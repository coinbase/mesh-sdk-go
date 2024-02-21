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

package encoder

import (
	"bytes"
	"sync"
)

// BufferPool contains a sync.Pool
// of *bytes.Buffer.
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool returns a new *BufferPool.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Put resets the provided *bytes.Buffer and stores
// it in the pool for reuse.
func (p *BufferPool) Put(buffer *bytes.Buffer) {
	buffer.Reset()
	p.pool.Put(buffer)
}

// PutByteSlice creates a *bytes.Buffer from the provided
// []byte and stores it in the pool for reuse.
func (p *BufferPool) PutByteSlice(buffer []byte) {
	p.Put(bytes.NewBuffer(buffer))
}

// Get returns a new or reused *bytes.Buffer.
func (p *BufferPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}
