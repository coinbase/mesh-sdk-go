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

// inspiration: https://github.com/puneeth8994/binary-tree-go-impl/blob/master/main.go

// BST is an implementation of a binary search tree.
type BST struct {
	root *Node
}

// Empty returns a boolean indicating
// if there are any nodes in the BST.
func (t *BST) Empty() bool {
	return t.root == nil
}

// Set stores the key and value in the BST
func (t *BST) Set(key int64, value int) {
	if t.root == nil {
		t.root = &Node{Key: key, Value: value}
		return
	}

	t.root.set(key, value)
}

// Get gets the key in the BST and returns
// its representative node (so that modifications
// can be done in constant time).
func (t *BST) Get(key int64) *Node {
	return t.root.get(key)
}

// Delete removes the key from the BST.
func (t *BST) Delete(key int64) {
	t.root = t.root.remove(key)
}

// Min returns the smallest node in the BST.
func (t *BST) Min() *Node {
	return t.root.findMin()
}

// Node is a Node in a BST
type Node struct {
	Key   int64
	Value int
	left  *Node
	right *Node
}

// set adds the key and value to the BST
// if they key doesn't exist. If it does
// exist, it is overwritten.
func (n *Node) set(key int64, value int) {
	if n.Key == key {
		n.Value = value
		return
	}

	if n.Key > key {
		if n.left == nil {
			n.left = &Node{Key: key, Value: value}
			return
		}

		n.left.set(key, value)
		return
	}

	if n.right == nil {
		n.right = &Node{Key: key, Value: value}
		return
	}

	n.right.set(key, value)
}

// get gets the *Node for a key in the BST,
// returning nil if it doesn't exist.
func (n *Node) get(key int64) *Node {
	if n == nil {
		return nil
	}

	switch {
	case key == n.Key:
		return n
	case key < n.Key:
		return n.left.get(key)
	default:
		return n.right.get(key)
	}
}

// remove deletes the *Node for a key in the BST
// and returns the new root.
func (n *Node) remove(key int64) *Node {
	if n == nil {
		return nil
	}

	if key < n.Key {
		n.left = n.left.remove(key)
		return n
	}

	if key > n.Key {
		n.right = n.right.remove(key)
		return n
	}

	if n.left == nil && n.right == nil {
		return nil
	}

	if n.left == nil {
		return n.right
	}

	if n.right == nil {
		return n.left
	}

	smallestKeyOnRight := n.right
	for {
		if smallestKeyOnRight != nil && smallestKeyOnRight.left != nil {
			smallestKeyOnRight = smallestKeyOnRight.left
		} else {
			break
		}
	}

	n.Key = smallestKeyOnRight.Key
	n.right = n.right.remove(n.Key)
	return n
}

// findMin returns the smallest key in
// the BST
func (n *Node) findMin() *Node {
	if n == nil {
		return nil
	}

	if n.left == nil {
		return n
	}
	return n.left.findMin()
}
