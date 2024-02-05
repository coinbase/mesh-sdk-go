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

package database

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/lucasjones/reggen"
	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/storage/encoder"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

func newTestBadgerDatabase(ctx context.Context, dir string) (Database, error) {
	return NewBadgerDatabase(
		ctx,
		dir,
		WithIndexCacheSize(TinyIndexCacheSize),
	)
}

func TestDatabase(t *testing.T) {
	for _, compress := range []bool{true, false} {
		t.Run(fmt.Sprintf("compress: %t", compress), func(t *testing.T) {
			ctx := context.Background()

			newDir, err := utils.CreateTempDir()
			assert.NoError(t, err)
			defer utils.RemoveTempDir(newDir)

			opts := []BadgerOption{
				WithIndexCacheSize(TinyIndexCacheSize),
			}
			if !compress {
				opts = append(opts, WithoutCompression())
			}

			database, err := NewBadgerDatabase(
				ctx,
				newDir,
				opts...,
			)
			assert.NoError(t, err)
			defer database.Close(ctx)

			t.Run("No key exists", func(t *testing.T) {
				txn := database.ReadTransaction(ctx)
				exists, value, err := txn.Get(ctx, []byte("hello"))
				assert.False(t, exists)
				assert.Nil(t, value)
				assert.NoError(t, err)
				txn.Discard(ctx)
			})

			t.Run("Set key", func(t *testing.T) {
				txn := database.Transaction(ctx)
				err := txn.Set(ctx, []byte("hello"), []byte("hola"), true)
				assert.NoError(t, err)
				assert.NoError(t, txn.Commit(ctx))
			})

			t.Run("Get key", func(t *testing.T) {
				txn := database.ReadTransaction(ctx)
				exists, value, err := txn.Get(ctx, []byte("hello"))
				assert.True(t, exists)
				assert.Equal(t, []byte("hola"), value)
				assert.NoError(t, err)
				txn.Discard(ctx)
			})

			t.Run("Many key set/get", func(t *testing.T) {
				for i := 0; i < 1000; i++ {
					txn := database.Transaction(ctx)
					k := []byte(fmt.Sprintf("blah/%d", i))
					v := []byte(fmt.Sprintf("%d", i))
					err := txn.Set(ctx, k, v, true)
					assert.NoError(t, err)
					assert.NoError(t, txn.Commit(ctx))

					for j := 0; j <= i; j++ {
						txn := database.ReadTransaction(ctx)
						jk := []byte(fmt.Sprintf("blah/%d", j))
						jv := []byte(fmt.Sprintf("%d", j))
						exists, value, err := txn.Get(ctx, jk)
						assert.True(t, exists)
						assert.Equal(t, jv, value)
						assert.NoError(t, err)
						txn.Discard(ctx)
					}
				}
			})

			t.Run("Scan", func(t *testing.T) {
				txn := database.Transaction(ctx)
				type scanItem struct {
					Key   []byte
					Value []byte
				}

				storedValues := []*scanItem{}
				for i := 0; i < 100; i++ {
					k := []byte(fmt.Sprintf("test/%d", i))
					v := []byte(fmt.Sprintf("%d", i))
					err := txn.Set(ctx, k, v, true)
					assert.NoError(t, err)

					storedValues = append(storedValues, &scanItem{
						Key:   k,
						Value: v,
					})
				}

				for i := 0; i < 100; i++ {
					k := []byte(fmt.Sprintf("testing/%d", i))
					v := []byte(fmt.Sprintf("%d", i))
					err := txn.Set(ctx, k, v, true)
					assert.NoError(t, err)
				}

				retrievedStoredValues := []*scanItem{}
				numValues, err := txn.Scan(
					ctx,
					[]byte("test/"),
					[]byte("test/"),
					func(k []byte, v []byte) error {
						thisK := make([]byte, len(k))
						thisV := make([]byte, len(v))

						copy(thisK, k)
						copy(thisV, v)

						retrievedStoredValues = append(retrievedStoredValues, &scanItem{
							Key:   thisK,
							Value: thisV,
						})

						return nil
					},
					false,
					false,
				)
				assert.NoError(t, err)
				assert.Equal(t, 100, numValues)
				assert.ElementsMatch(t, storedValues, retrievedStoredValues)
				assert.NoError(t, txn.Commit(ctx))
			})
		})
	}
}

func TestDatabaseTransaction(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	t.Run("Set and get within a transaction", func(t *testing.T) {
		txn := database.Transaction(ctx)
		assert.NoError(t, txn.Set(ctx, []byte("hello"), []byte("hola"), true))

		// Ensure tx does not affect db
		txn2 := database.ReadTransaction(ctx)
		exists, value, err := txn2.Get(ctx, []byte("hello"))
		assert.False(t, exists)
		assert.Nil(t, value)
		assert.NoError(t, err)
		txn2.Discard(ctx)

		assert.NoError(t, txn.Commit(ctx))

		txn3 := database.ReadTransaction(ctx)
		exists, value, err = txn3.Get(ctx, []byte("hello"))
		assert.True(t, exists)
		assert.Equal(t, []byte("hola"), value)
		assert.NoError(t, err)
		txn3.Discard(ctx)
	})

	t.Run("Discard transaction", func(t *testing.T) {
		txn := database.Transaction(ctx)
		assert.NoError(t, txn.Set(ctx, []byte("hello"), []byte("world"), true))
		txn.Discard(ctx)

		txn2 := database.ReadTransaction(ctx)
		exists, value, err := txn2.Get(ctx, []byte("hello"))
		txn2.Discard(ctx)
		assert.True(t, exists)
		assert.Equal(t, []byte("hola"), value)
		assert.NoError(t, err)
	})

	t.Run("Delete within a transaction", func(t *testing.T) {
		txn := database.Transaction(ctx)
		assert.NoError(t, txn.Delete(ctx, []byte("hello")))
		assert.NoError(t, txn.Commit(ctx))

		txn2 := database.ReadTransaction(ctx)
		exists, value, err := txn2.Get(ctx, []byte("hello"))
		assert.False(t, exists)
		assert.Nil(t, value)
		assert.NoError(t, err)
	})
}

type BogusEntry struct {
	Index int    `json:"index"`
	Stuff string `json:"stuff"`
}

func TestBadgerTrain_NoLimit(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)

	// Load storage with entries in namespace
	namespace := "bogus"
	txn := database.Transaction(ctx)
	for i := 0; i < 10000; i++ {
		entry := &BogusEntry{
			Index: i,
			Stuff: fmt.Sprintf("block %d", i),
		}
		compressedEntry, err := database.Encoder().Encode(namespace, entry)
		assert.NoError(t, err)
		assert.NoError(
			t,
			txn.Set(ctx, []byte(fmt.Sprintf("%s/%d", namespace, i)), compressedEntry, true),
		)
	}
	assert.NoError(t, txn.Commit(ctx))

	// Close DB
	database.Close(ctx)

	// Train
	normalSize, dictSize, err := BadgerTrain(
		ctx,
		namespace,
		newDir,
		path.Join(newDir, "bogus_dict"),
		-1,
		[]*encoder.CompressorEntry{},
	)
	assert.NoError(t, err)
	assert.True(t, normalSize > dictSize)
}

func TestBadgerTrain_Limit(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)

	// Load storage with entries in namespace
	namespace := "bogus"
	txn := database.Transaction(ctx)
	for i := 0; i < 10000; i++ {
		output, err := reggen.Generate(`[a-z]+`, 50)
		assert.NoError(t, err)
		entry := &BogusEntry{
			Index: i,
			Stuff: output,
		}
		compressedEntry, err := database.Encoder().Encode(namespace, entry)
		assert.NoError(t, err)
		assert.NoError(
			t,
			txn.Set(ctx, []byte(fmt.Sprintf("%s/%d", namespace, i)), compressedEntry, true),
		)
	}
	assert.NoError(t, txn.Commit(ctx))

	// Close DB
	database.Close(ctx)

	// Train
	dictionaryPath := path.Join(newDir, "bogus_dict")
	oldSize, newSize, err := BadgerTrain(
		ctx,
		namespace,
		newDir,
		dictionaryPath,
		50,
		[]*encoder.CompressorEntry{},
	)
	assert.NoError(t, err)
	assert.True(t, oldSize > newSize)

	// Train again using dictionary
	newDir2, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir2)

	entries := []*encoder.CompressorEntry{
		{
			Namespace:      namespace,
			DictionaryPath: dictionaryPath,
		},
	}
	database2, err := NewBadgerDatabase(
		ctx,
		newDir2,
		WithCompressorEntries(entries),
		WithIndexCacheSize(TinyIndexCacheSize),
	)
	assert.NoError(t, err)

	txn2 := database2.Transaction(ctx)
	for i := 0; i < 10000; i++ {
		output, err := reggen.Generate(`[a-z]+`, 50)
		assert.NoError(t, err)
		entry := &BogusEntry{
			Index: i,
			Stuff: output,
		}
		compressedEntry, err := database2.Encoder().Encode(namespace, entry)
		assert.NoError(t, err)
		assert.NoError(
			t,
			txn2.Set(ctx, []byte(fmt.Sprintf("%s/%d", namespace, i)), compressedEntry, true),
		)
	}
	assert.NoError(t, txn2.Commit(ctx))

	// Train from Dictionary
	database2.Close(ctx)
	oldSize2, newSize2, err := BadgerTrain(
		ctx,
		namespace,
		newDir2,
		path.Join(newDir2, "bogus_dict_2"),
		-1,
		entries,
	)
	assert.NoError(t, err)
	assert.True(t, oldSize2 > newSize2)
	assert.True(t, newSize > newSize2)
}
