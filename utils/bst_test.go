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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBST(t *testing.T) {
	bst := &BST{}

	// Test empty BST
	assert.Nil(t, bst.Get(1))
	assert.Nil(t, bst.Min())
	assert.True(t, bst.Empty())

	// Set 1 key and ensure it is the min
	bst.Set(1, 10)
	assert.False(t, bst.Empty())
	assert.Equal(t, 10, bst.Get(1).Value)
	assert.Equal(t, int64(1), bst.Min().Key)
	assert.Nil(t, bst.Get(10))

	// Overwrite existing key
	bst.Set(1, 11)
	assert.Equal(t, 11, bst.Get(1).Value)
	assert.Equal(t, int64(1), bst.Min().Key)

	// Add a key that will be put in "left"
	// of root
	bst.Set(0, 11)
	assert.Equal(t, 11, bst.Get(0).Value)
	assert.Equal(t, int64(0), bst.Min().Key)

	// Delete root
	bst.Delete(1)
	assert.Equal(t, 11, bst.Get(0).Value)
	assert.Equal(t, int64(0), bst.Min().Key)
	assert.Nil(t, bst.Get(1))

	// Add keys to the "right" of new root
	bst.Set(3, 33)
	bst.Set(2, 22)

	// Delete already deleted item
	bst.Delete(1)
	assert.False(t, bst.Empty())

	// Delete root again
	bst.Delete(0)
	bst.Delete(0)
	assert.False(t, bst.Empty())

	// Ensure 2 is the min key after root
	// deleted
	assert.Equal(t, int64(2), bst.Min().Key)

	// Delete all items
	bst.Delete(3)
	bst.Delete(2)
	assert.True(t, bst.Empty())
	assert.Nil(t, bst.Min())
}
