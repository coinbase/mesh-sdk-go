package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBST(t *testing.T) {
	bst := &BST{}
	assert.Nil(t, bst.Get(1))
	assert.Nil(t, bst.Min())
	assert.True(t, bst.Empty())

	bst.Set(1, 10)
	assert.False(t, bst.Empty())
	assert.Equal(t, 10, bst.Get(1).Value)
	assert.Equal(t, int64(1), bst.Min().Key)
	assert.Nil(t, bst.Get(10))

	bst.Set(1, 11)
	assert.Equal(t, 11, bst.Get(1).Value)
	assert.Equal(t, int64(1), bst.Min().Key)

	bst.Set(0, 11)
	assert.Equal(t, 11, bst.Get(0).Value)
	assert.Equal(t, int64(0), bst.Min().Key)

	bst.Delete(1)
	assert.Equal(t, 11, bst.Get(0).Value)
	assert.Equal(t, int64(0), bst.Min().Key)
	assert.Nil(t, bst.Get(1))

	bst.Set(3, 33)
	bst.Set(2, 22)
	bst.Delete(1)
	assert.False(t, bst.Empty())
	bst.Delete(0)
	bst.Delete(0)
	assert.False(t, bst.Empty())
	assert.Equal(t, int64(2), bst.Min().Key)
	bst.Delete(3)
	bst.Delete(2)
	assert.True(t, bst.Empty())
	assert.Nil(t, bst.Min())
}
