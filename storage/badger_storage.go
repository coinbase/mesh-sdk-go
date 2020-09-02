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

package storage

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sync"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/DataDog/zstd"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

const (
	// DefaultCacheSize is 0 MB.
	DefaultCacheSize = 0

	// DefaultBfCacheSize is 10 MB.
	DefaultBfCacheSize = 10 << 20

	// DefaultValueLogFileSize is 100 MB.
	DefaultValueLogFileSize = 100 << 20

	// PerformanceMaxTableSize is 256 MB. The larger
	// this value is, the larger database transactions
	// storage can handle.
	PerformanceMaxTableSize = 256 << 20

	bytesInMb = 1000000

	logModulo = 5000
)

var (
	errMaxEntries = errors.New("max entries reached")
)

// BadgerStorage is a wrapper around Badger DB
// that implements the Database interface.
type BadgerStorage struct {
	limitMemory       bool
	compressorEntries []*CompressorEntry

	db         *badger.DB
	compressor *Compressor

	writer sync.Mutex
}

func defaultBadgerOptions(dir string) badger.Options {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	// LoadBloomsOnOpen=false will improve the db startup speed
	opts.LoadBloomsOnOpen = false

	// To allow writes at a faster speed, we create a new memtable as soon as
	// an existing memtable is filled up. This option determines how many
	// memtables should be kept in memory.
	opts.NumMemtables = 1

	// Don't keep multiple memtables in memory. With larger
	// memtable size, this explodes memory usage.
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2

	return opts
}

// lowMemoryOptions returns a set of BadgerDB configuration
// options that significantly reduce memory usage.
//
// Inspired by: https://github.com/dgraph-io/badger/issues/1304
func lowMemoryOptions(dir string) badger.Options {
	opts := defaultBadgerOptions(dir)

	// Don't load tables into memory.
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO

	// This option will have a significant effect the memory. If the level is kept
	// in-memory, read are faster but the tables will be kept in memory.
	opts.KeepL0InMemory = false

	// Bloom filters will be kept in memory if the following option is not set. Each
	// bloom filter takes up 5 MB of memory. A smaller bf cache would mean that
	// bloom filters will be evicted quickly from the cache and they will be read from
	// the disk (which is slow) and inserted into the cache.
	opts.MaxBfCacheSize = DefaultBfCacheSize

	// Don't cache blocks in memory. All reads should go to disk.
	opts.MaxCacheSize = DefaultCacheSize

	// Limit ValueLogFileSize as log files
	// must be read into memory during compaction.
	opts.ValueLogFileSize = DefaultValueLogFileSize

	return opts
}

// performanceOptions returns a set of BadgerDB configuration
// options that don't attempt to reduce memory usage (can
// improve performance).
func performanceOptions(dir string) badger.Options {
	opts := defaultBadgerOptions(dir)
	opts.MaxTableSize = PerformanceMaxTableSize

	return opts
}

// NewBadgerStorage creates a new BadgerStorage.
func NewBadgerStorage(ctx context.Context, dir string, options ...BadgerOption) (Database, error) {
	b := &BadgerStorage{}
	for _, opt := range options {
		opt(b)
	}

	dir = path.Clean(dir)
	dbOpts := lowMemoryOptions(dir)
	if !b.limitMemory {
		dbOpts = performanceOptions(dir)
	}

	db, err := badger.Open(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("%w: could not open badger database", err)
	}

	compressor, err := NewCompressor(b.compressorEntries)
	if err != nil {
		return nil, fmt.Errorf("%w: could not load compressor", err)
	}

	return &BadgerStorage{
		db:         db,
		compressor: compressor,
	}, nil
}

// Close closes the database to prevent corruption.
// The caller should defer this in main.
func (b *BadgerStorage) Close(ctx context.Context) error {
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("%w unable to close database", err)
	}

	return nil
}

// Compressor returns the BadgerStorage compressor.
func (b *BadgerStorage) Compressor() *Compressor {
	return b.compressor
}

// BadgerTransaction is a wrapper around a Badger
// DB transaction that implements the DatabaseTransaction
// interface.
type BadgerTransaction struct {
	db  *BadgerStorage
	txn *badger.Txn

	holdsLock bool
}

// NewDatabaseTransaction creates a new BadgerTransaction.
// If the transaction will not modify any values, pass
// in false for the write parameter (this allows for
// optimization within the Badger DB).
func (b *BadgerStorage) NewDatabaseTransaction(
	ctx context.Context,
	write bool,
) DatabaseTransaction {
	if write {
		// To avoid database commit conflicts,
		// we need to lock the writer.
		//
		// Because we process blocks serially,
		// this doesn't lead to much lock contention.
		b.writer.Lock()
	}

	return &BadgerTransaction{
		db:        b,
		txn:       b.db.NewTransaction(write),
		holdsLock: write,
	}
}

// Commit attempts to commit and discard the transaction.
func (b *BadgerTransaction) Commit(context.Context) error {
	err := b.txn.Commit()

	// It is possible that we may accidentally call commit twice.
	// In this case, we only unlock if we hold the lock to avoid a panic.
	if b.holdsLock {
		b.holdsLock = false
		b.db.writer.Unlock()
	}

	if err != nil {
		return fmt.Errorf("%w: unable to commit transaction", err)
	}

	return nil
}

// Discard discards an open transaction. All transactions
// must be either discarded or committed.
func (b *BadgerTransaction) Discard(context.Context) {
	b.txn.Discard()
	if b.holdsLock {
		b.db.writer.Unlock()
	}
}

// Set changes the value of the key to the value within a transaction.
func (b *BadgerTransaction) Set(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	return b.txn.Set(key, value)
}

// Get accesses the value of the key within a transaction.
func (b *BadgerTransaction) Get(
	ctx context.Context,
	key []byte,
) (bool, []byte, error) {
	var value []byte
	item, err := b.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	err = item.Value(func(v []byte) error {
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	if err != nil {
		return false, nil, err
	}

	return true, value, nil
}

// Delete removes the key and its value within the transaction.
func (b *BadgerTransaction) Delete(ctx context.Context, key []byte) error {
	return b.txn.Delete(key)
}

// Scan retrieves all elements with a given prefix in a database
// transaction.
func (b *BadgerTransaction) Scan(
	ctx context.Context,
	prefix []byte,
) ([]*ScanItem, error) {
	values := []*ScanItem{}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := b.txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to get value for key %s", err, string(k))
		}

		values = append(values, &ScanItem{
			Key:   k,
			Value: v,
		})
	}

	return values, nil
}

// Set changes the value of the key to the value in its own transaction.
func (b *BadgerStorage) Set(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get fetches the value of a key in its own transaction.
func (b *BadgerStorage) Get(
	ctx context.Context,
	key []byte,
) (bool, []byte, error) {
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(v []byte) error {
			value = make([]byte, len(v))
			copy(value, v)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	return true, value, nil
}

// Scan fetches all items at a given prefix. This is typically
// used to get all items in a namespace.
func (b *BadgerStorage) Scan(
	ctx context.Context,
	prefix []byte,
) ([]*ScanItem, error) {
	txn := b.NewDatabaseTransaction(ctx, false)
	defer txn.Discard(ctx)

	return txn.Scan(ctx, prefix)
}

// LimitedMemoryScan calls a worker for each item in a scan instead
// of reading all items into memory.
func (b *BadgerStorage) LimitedMemoryScan(
	ctx context.Context,
	prefix []byte,
	worker func([]byte, []byte) error,
) (int, error) {
	txn := b.db.NewTransaction(false)
	defer txn.Discard()

	entries := 0

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		err := item.Value(func(v []byte) error {
			if err := worker(k, v); err != nil {
				return fmt.Errorf("%w: worker failed for key %s", err, string(k))
			}

			return nil
		})
		if err != nil {
			return -1, fmt.Errorf("%w: unable to get value for key %s", err, string(k))
		}

		entries++
		if entries%logModulo == 0 {
			log.Printf("scanned %d entries for %s\n", entries, string(prefix))
		}
	}

	return entries, nil
}

func decompressAndSave(
	compressor *Compressor,
	namespace string,
	tmpDir string,
	k []byte,
	v []byte,
) (float64, float64, error) {
	// We use a compressor.DecodeRaw here because the v may be
	// encoded using dictionary compression.
	decompressed, err := compressor.DecodeRaw(namespace, v)
	if err != nil {
		return -1, -1, fmt.Errorf("%w: unable to decompress %s", err, string(k))
	}

	err = ioutil.WriteFile(
		path.Join(tmpDir, types.Hash(string(k))),
		decompressed,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return -1, -1, fmt.Errorf("%w: unable to store decompressed file", err)
	}

	return float64(len(decompressed)), float64(len(v)), nil
}

func decompressAndEncode(
	path string,
	namespace string,
	compressor *Compressor,
) (float64, float64, float64, error) {
	decompressed, err := ioutil.ReadFile(path) // #nosec G304
	if err != nil {
		return -1, -1, -1, fmt.Errorf("%w: unable to load file %s", err, path)
	}

	normalCompress, err := zstd.Compress(nil, decompressed)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("%w: unable to compress nomral", err)
	}

	dictCompress, err := compressor.Encode(namespace, decompressed)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("%w: unable to compress with dictionary", err)
	}

	// Ensure dict works
	var dictDecode []byte
	if err := compressor.Decode(namespace, dictCompress, &dictDecode); err != nil {
		return -1, -1, -1, fmt.Errorf("%w: unable to decompress with dictionary", err)
	}

	if types.Hash(decompressed) != types.Hash(dictDecode) {
		return -1, -1, -1, errors.New("decompressed dictionary output does not match")
	}

	return float64(len(decompressed)), float64(len(normalCompress)), float64(len(dictCompress)), nil
}

// recompress compares the new compressor versus
// what is already on-chain. It returns the old
// on-disk size vs the new on-disk size with the new
// compressor.
func recompress(
	ctx context.Context,
	badgerDb Database,
	namespace string,
	restrictedNamespace string,
	newCompressor *Compressor,
) (float64, float64, error) {
	onDiskSize := float64(0)
	newSize := float64(0)

	_, err := badgerDb.LimitedMemoryScan(
		ctx,
		[]byte(restrictedNamespace),
		func(k []byte, v []byte) error {
			decompressed, err := badgerDb.Compressor().DecodeRaw(namespace, v)
			if err != nil {
				return fmt.Errorf("%w: unable to decompress %s", err, string(k))
			}

			newCompressed, err := newCompressor.Encode(namespace, decompressed)
			if err != nil {
				return fmt.Errorf("%w: unable to compress with dictionary", err)
			}
			onDiskSize += float64(len(v))
			newSize += float64(len(newCompressed))

			return nil
		},
	)
	if err != nil {
		return -1, -1, fmt.Errorf("%w: unable to recompress", err)
	}

	// Negative savings here means that the new dictionary
	// is worse.
	savings := (onDiskSize - newSize) / onDiskSize
	log.Printf(
		"[OUT OF SAMPLE] Savings: %f%%)",
		savings*utils.OneHundred,
	)

	return onDiskSize, newSize, nil
}

// BadgerTrain creates a zstd dictionary for a given BadgerStorage DB namespace.
// Optionally, you can specify the maximum number of entries to load into
// storage (if -1 is provided, then all possible are loaded).
func BadgerTrain(
	ctx context.Context,
	namespace string,
	db string,
	output string,
	maxEntries int,
	compressorEntries []*CompressorEntry,
) (float64, float64, error) {
	badgerDb, err := NewBadgerStorage(
		ctx,
		path.Clean(db),
		WithCompressorEntries(compressorEntries),
	)
	if err != nil {
		return -1, -1, fmt.Errorf("%w: unable to load database", err)
	}
	defer badgerDb.Close(ctx)

	// Create directory to sore uncompressed files for training
	tmpDir, err := utils.CreateTempDir()
	if err != nil {
		return -1, -1, fmt.Errorf("%w: unable to create temporary directory", err)
	}
	defer utils.RemoveTempDir(tmpDir)

	// We must use a restricted namespace or we will inadvertently
	// fetch all namespaces that contain the namespace we care about.
	restrictedNamespace := fmt.Sprintf("%s/", namespace)

	totalUncompressedSize := float64(0)
	totalDiskSize := float64(0)
	entriesSeen := 0
	_, err = badgerDb.LimitedMemoryScan(
		ctx,
		[]byte(restrictedNamespace),
		func(k []byte, v []byte) error {
			decompressedSize, diskSize, err := decompressAndSave(
				badgerDb.Compressor(),
				namespace,
				tmpDir,
				k,
				v,
			)
			if err != nil {
				return fmt.Errorf("%w: unable to decompress and save", err)
			}

			totalUncompressedSize += decompressedSize
			totalDiskSize += diskSize
			entriesSeen++

			if entriesSeen > maxEntries-1 && maxEntries != -1 {
				return errMaxEntries
			}

			return nil
		},
	)
	if err != nil && !errors.Is(err, errMaxEntries) {
		return -1, -1, fmt.Errorf("%w: unable to scan for %s", err, namespace)
	}

	if entriesSeen == 0 {
		return -1, -1, fmt.Errorf("found 0 entries for %s", namespace)
	}

	log.Printf(
		"found %d entries for %s (average uncompressed size: %fB)\n",
		entriesSeen,
		namespace,
		totalUncompressedSize/float64(entriesSeen),
	)

	log.Printf(
		"found %d entries for %s (average disk size: %fB)\n",
		entriesSeen,
		namespace,
		totalDiskSize/float64(entriesSeen),
	)

	// Invoke ZSTD
	dictPath := path.Clean(output)
	log.Printf("creating dictionary %s\n", dictPath)
	cmd := exec.Command(
		"zstd",
		"--train",
		"-r",
		tmpDir,
		"-o",
		dictPath,
	) // #nosec G204
	if err := cmd.Start(); err != nil {
		return -1, -1, fmt.Errorf("%w: unable to start zstd", err)
	}

	if err := cmd.Wait(); err != nil {
		return -1, -1, fmt.Errorf("%w: unable to train zstd", err)
	}

	compressor, err := NewCompressor([]*CompressorEntry{
		{
			Namespace:      namespace,
			DictionaryPath: dictPath,
		},
	})
	if err != nil {
		return -1, -1, fmt.Errorf("%w: unable to load compressor", err)
	}

	sizeUncompressed := float64(0)
	sizeNormal := float64(0)
	sizeDictionary := float64(0)
	err = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		decompressed, normalCompress, dictCompress, err := decompressAndEncode(
			path,
			namespace,
			compressor,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to decompress and encode", err)
		}

		sizeUncompressed += decompressed
		sizeNormal += normalCompress
		sizeDictionary += dictCompress

		return nil
	})
	if err != nil {
		return -1, -1, fmt.Errorf("%w: unable to walk files", err)
	}

	log.Printf("[IN SAMPLE] Total Size Uncompressed: %fMB", sizeUncompressed/bytesInMb)
	normalSize := sizeNormal / sizeUncompressed
	log.Printf(
		"[IN SAMPLE] Total Size Compressed: %fMB (%% of original size %f%%)",
		sizeNormal/bytesInMb,
		normalSize*utils.OneHundred,
	)

	if len(compressorEntries) > 0 {
		oldDictionarySize := totalDiskSize / sizeUncompressed
		log.Printf(
			"[IN SAMPLE] Total Size Compressed (with old dictionary): %fMB (%% of original size %f%%)",
			totalDiskSize/bytesInMb,
			oldDictionarySize*utils.OneHundred,
		)
	}

	dictionarySize := sizeDictionary / sizeUncompressed
	log.Printf(
		"[IN SAMPLE] Total Size Compressed (with new dictionary): %fMB (%% of original size %f%%)",
		sizeDictionary/bytesInMb,
		dictionarySize*utils.OneHundred,
	)

	// Run through all values in scan and compare size
	return recompress(
		ctx,
		badgerDb,
		namespace,
		restrictedNamespace,
		compressor,
	)
}
