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
	"bytes"
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
	"time"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

const (
	// DefaultBlockCacheSize is 0 MB.
	DefaultBlockCacheSize = 0

	// DefaultIndexCacheSize is 2 GB.
	DefaultIndexCacheSize = 2000 << 20

	// DefaultMaxTableSize is 256 MB. The larger
	// this value is, the larger database transactions
	// storage can handle.
	DefaultMaxTableSize = 256 << 20

	// logModulo determines how often we should print
	// logs while scanning data.
	logModulo = 5000

	// Default GC settings for reclaiming
	// space in value logs.
	defaultGCInterval     = 1 * time.Minute
	defualtGCDiscardRatio = 0.1
	defaultGCSleep        = 10 * time.Second
)

// BadgerStorage is a wrapper around Badger DB
// that implements the Database interface.
type BadgerStorage struct {
	limitMemory       bool
	indexCacheSize    int64
	compressorEntries []*CompressorEntry

	pool       *BufferPool
	db         *badger.DB
	compressor *Compressor

	writer sync.Mutex

	// Track the closed status to ensure we exit garbage
	// collection when the db closes.
	closed chan struct{}
}

func defaultBadgerOptions(dir string) badger.Options {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	// We increase the MaxTableSize to support larger database
	// transactions (which are capped at 20% of MaxTableSize).
	opts.MaxTableSize = DefaultMaxTableSize

	// To allow writes at a faster speed, we create a new memtable as soon as
	// an existing memtable is filled up. This option determines how many
	// memtables should be kept in memory.
	opts.NumMemtables = 1

	// Don't keep multiple memtables in memory. With larger
	// memtable size, this explodes memory usage.
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2

	// By default, we set TableLoadingMode to use MemoryMap because
	// it uses much less memory than RAM but is much faster than
	// FileIO.
	opts.TableLoadingMode = options.MemoryMap

	// This option will have a significant effect the memory. If the level is kept
	// in-memory, read are faster but the tables will be kept in memory. By default,
	// this is set to false.
	opts.KeepL0InMemory = false

	// We don't compact L0 on close as this can greatly delay shutdown time.
	opts.CompactL0OnClose = false

	// This value specifies how much memory should be used by table indices. These
	// indices include the block offsets and the bloomfilters. Badger uses bloom
	// filters to speed up lookups. Each table has its own bloom
	// filter and each bloom filter is approximately of 5 MB. This defaults
	// to an unlimited size (and quickly balloons to GB with a large DB).
	opts.IndexCacheSize = DefaultIndexCacheSize

	// Don't cache blocks in memory. All reads should go to disk.
	opts.BlockCacheSize = DefaultBlockCacheSize

	return opts
}

// lowMemoryOptions returns a set of BadgerDB configuration
// options that significantly reduce memory usage.
//
// Inspired by: https://github.com/dgraph-io/badger/issues/1304
func lowMemoryOptions(dir string) badger.Options {
	opts := defaultBadgerOptions(dir)

	// LoadBloomsOnOpen=false greatly improves the db startup speed.
	opts.LoadBloomsOnOpen = false

	// ValueLogLoadingMode ensures we don't load large value logs into
	// memory.
	opts.ValueLogLoadingMode = options.FileIO

	// Don't load tables into memory.
	opts.TableLoadingMode = options.FileIO

	return opts
}

// NewBadgerStorage creates a new BadgerStorage.
func NewBadgerStorage(ctx context.Context, dir string, options ...BadgerOption) (Database, error) {
	b := &BadgerStorage{
		closed:         make(chan struct{}),
		indexCacheSize: DefaultIndexCacheSize,
	}
	for _, opt := range options {
		opt(b)
	}

	dir = path.Clean(dir)
	dbOpts := defaultBadgerOptions(dir)
	if b.limitMemory {
		dbOpts = lowMemoryOptions(dir)
	}
	dbOpts.IndexCacheSize = b.indexCacheSize
	log.Printf(
		"badger database index cache size: %f MB\n",
		utils.BtoMb(float64(dbOpts.IndexCacheSize)),
	)

	db, err := badger.Open(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDatabaseOpenFailed, err)
	}
	b.db = db

	b.pool = NewBufferPool()

	compressor, err := NewCompressor(b.compressorEntries, b.pool)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCompressorLoadFailed, err)
	}
	b.compressor = compressor

	// Start periodic ValueGC goroutine (up to user of BadgerDB to call
	// periodically to reclaim value logs on-disk).
	// go b.periodicGC(ctx)

	return b, nil
}

// periodicGC attempts to reclaim storage every
// defaultGCInterval.
//
// Inspired by:
// https://github.com/ipfs/go-ds-badger/blob/a69f1020ba3954680900097e0c9d0181b88930ad/datastore.go#L173-L199
func (b *BadgerStorage) periodicGC(ctx context.Context) {
	// We start the timeout with the default sleep to aggressively check
	// for space to reclaim on startup.
	gcTimeout := time.NewTimer(defaultGCSleep)
	defer func() {
		log.Println("exiting periodic garbage collector")
		gcTimeout.Stop()
	}()

	for {
		select {
		case <-b.closed:
			// Exit the periodic gc thread if the database is closed.
			return
		case <-ctx.Done():
			return
		case <-gcTimeout.C:
			start := time.Now()
			err := b.db.RunValueLogGC(defualtGCDiscardRatio)
			switch err {
			case badger.ErrNoRewrite, badger.ErrRejected:
				// No rewrite means we've fully garbage collected.
				// Rejected means someone else is running a GC
				// or we're closing.
				gcTimeout.Reset(defaultGCInterval)
			case nil:
				// Nil error means that we've successfully garbage
				// collected. We should sleep instead of waiting
				// the full GC collection interval to see if there
				// is anything else to collect.
				log.Printf("successful value log garbage collection (%s)", time.Since(start))
				gcTimeout.Reset(defaultGCSleep)
			default:
				// Not much we can do on a random error but log it and continue.
				log.Printf("error during a GC cycle: %s\n", err.Error())
				gcTimeout.Reset(defaultGCInterval)
			}
		}
	}
}

// Close closes the database to prevent corruption.
// The caller should defer this in main.
func (b *BadgerStorage) Close(ctx context.Context) error {
	close(b.closed)
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("%w: %v", ErrDBCloseFailed, err)
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

	// We MUST wait to reclaim any memory until after
	// the transaction is committed or discarded.
	// Source: https://godoc.org/github.com/dgraph-io/badger#Txn.Set
	//
	// It is also CRITICALLY IMPORTANT that the same
	// buffer is not added to the BufferPool multiple
	// times. This will almost certainly lead to a panic.
	reclaimLock      sync.Mutex
	buffersToReclaim []*bytes.Buffer
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
		db:               b,
		txn:              b.db.NewTransaction(write),
		holdsLock:        write,
		buffersToReclaim: []*bytes.Buffer{},
	}
}

// Commit attempts to commit and discard the transaction.
func (b *BadgerTransaction) Commit(context.Context) error {
	err := b.txn.Commit()

	// Reclaim all allocated buffers for future work.
	b.reclaimLock.Lock()
	for _, buf := range b.buffersToReclaim {
		b.db.pool.Put(buf)
	}

	// Ensure we don't attempt to reclaim twice.
	b.buffersToReclaim = nil
	b.reclaimLock.Unlock()

	// It is possible that we may accidentally call commit twice.
	// In this case, we only unlock if we hold the lock to avoid a panic.
	if b.holdsLock {
		b.holdsLock = false
		b.db.writer.Unlock()
	}

	if err != nil {
		return fmt.Errorf("%w: %v", ErrCommitFailed, err)
	}

	return nil
}

// Discard discards an open transaction. All transactions
// must be either discarded or committed.
func (b *BadgerTransaction) Discard(context.Context) {
	b.txn.Discard()

	// Reclaim all allocated buffers for future work.
	b.reclaimLock.Lock()
	for _, buf := range b.buffersToReclaim {
		b.db.pool.Put(buf)
	}

	// Ensure we don't attempt to reclaim twice.
	b.buffersToReclaim = nil
	b.reclaimLock.Unlock()

	if b.holdsLock {
		b.db.writer.Unlock()
	}
}

// Set changes the value of the key to the value within a transaction.
func (b *BadgerTransaction) Set(
	ctx context.Context,
	key []byte,
	value []byte,
	reclaimValue bool,
) error {
	if reclaimValue {
		b.buffersToReclaim = append(
			b.buffersToReclaim,
			bytes.NewBuffer(value),
		)
	}

	return b.txn.Set(key, value)
}

// Get accesses the value of the key within a transaction.
// It is up to the caller to reclaim any memory returned.
func (b *BadgerTransaction) Get(
	ctx context.Context,
	key []byte,
) (bool, []byte, error) {
	value := b.db.pool.Get()
	item, err := b.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	err = item.Value(func(v []byte) error {
		_, err := value.Write(v)
		return err
	})
	if err != nil {
		return false, nil, err
	}

	return true, value.Bytes(), nil
}

// Delete removes the key and its value within the transaction.
func (b *BadgerTransaction) Delete(ctx context.Context, key []byte) error {
	return b.txn.Delete(key)
}

// Scan calls a worker for each item in a scan instead
// of reading all items into memory.
func (b *BadgerTransaction) Scan(
	ctx context.Context,
	prefix []byte,
	worker func([]byte, []byte) error,
	logEntries bool,
) (int, error) {
	entries := 0
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := b.txn.NewIterator(opts)
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
		if logEntries && entries%logModulo == 0 {
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
		return -1, -1, fmt.Errorf("%w %s: %v", ErrDecompressFailed, string(k), err)
	}

	err = ioutil.WriteFile(
		path.Join(tmpDir, types.Hash(string(k))),
		decompressed,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return -1, -1, fmt.Errorf("%w: %v", ErrDecompressSaveUnsuccessful, err)
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
		return -1, -1, -1, fmt.Errorf("%w for file %s: %v", ErrLoadFileUnsuccessful, path, err)
	}

	normalCompress, err := compressor.EncodeRaw("", decompressed)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("%w: %v", ErrCompressNormalFailed, err)
	}

	dictCompress, err := compressor.EncodeRaw(namespace, decompressed)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("%w: %v", ErrCompressWithDictFailed, err)
	}

	// Ensure dict works
	decompressedDict, err := compressor.DecodeRaw(namespace, dictCompress)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("%w: %v", ErrDecompressWithDictFailed, err)
	}

	if types.Hash(decompressed) != types.Hash(decompressedDict) {
		return -1, -1, -1, ErrDecompressOutputMismatch
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

	txn := badgerDb.NewDatabaseTransaction(ctx, false)
	defer txn.Discard(ctx)
	_, err := txn.Scan(
		ctx,
		[]byte(restrictedNamespace),
		func(k []byte, v []byte) error {
			decompressed, err := badgerDb.Compressor().DecodeRaw(namespace, v)
			if err != nil {
				return fmt.Errorf("%w %s: %v", ErrDecompressFailed, string(k), err)
			}

			newCompressed, err := newCompressor.EncodeRaw(namespace, decompressed)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrCompressWithDictFailed, err)
			}
			onDiskSize += float64(len(v))
			newSize += float64(len(newCompressed))

			return nil
		},
		true,
	)
	if err != nil {
		return -1, -1, fmt.Errorf("%w: %v", ErrRecompressFailed, err)
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

	// Create directory to store uncompressed files for training
	tmpDir, err := utils.CreateTempDir()
	if err != nil {
		return -1, -1, fmt.Errorf("%w: %v", ErrCreateTempDirectoryFailed, err)
	}
	defer utils.RemoveTempDir(tmpDir)

	// We must use a restricted namespace or we will inadvertently
	// fetch all namespaces that contain the namespace we care about.
	restrictedNamespace := fmt.Sprintf("%s/", namespace)

	totalUncompressedSize := float64(0)
	totalDiskSize := float64(0)
	entriesSeen := 0
	txn := badgerDb.NewDatabaseTransaction(ctx, false)
	defer txn.Discard(ctx)
	_, err = txn.Scan(
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
				return ErrMaxEntries
			}

			return nil
		},
		true,
	)
	if err != nil && !errors.Is(err, ErrMaxEntries) {
		return -1, -1, fmt.Errorf("%w for %s: %v", ErrScanFailed, namespace, err)
	}

	if entriesSeen == 0 {
		return -1, -1, fmt.Errorf("%w %s", ErrNoEntriesFoundInNamespace, namespace)
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
		return -1, -1, fmt.Errorf("%w: %v", ErrInvokeZSTDFailed, err)
	}

	if err := cmd.Wait(); err != nil {
		return -1, -1, fmt.Errorf("%w: %v", ErrTrainZSTDFailed, err)
	}

	compressor, err := NewCompressor([]*CompressorEntry{
		{
			Namespace:      namespace,
			DictionaryPath: dictPath,
		},
	}, NewBufferPool())
	if err != nil {
		return -1, -1, fmt.Errorf("%w: %v", ErrCompressorLoadFailed, err)
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
		return -1, -1, fmt.Errorf("%w: %v", ErrWalkFilesFailed, err)
	}

	log.Printf(
		"[IN SAMPLE] Total Size Uncompressed: %fMB",
		utils.BtoMb(sizeUncompressed),
	)
	normalSize := sizeNormal / sizeUncompressed
	log.Printf(
		"[IN SAMPLE] Total Size Compressed: %fMB (%% of original size %f%%)",
		utils.BtoMb(sizeNormal),
		normalSize*utils.OneHundred,
	)

	if len(compressorEntries) > 0 {
		oldDictionarySize := totalDiskSize / sizeUncompressed
		log.Printf(
			"[IN SAMPLE] Total Size Compressed (with old dictionary): %fMB (%% of original size %f%%)",
			utils.BtoMb(totalDiskSize),
			oldDictionarySize*utils.OneHundred,
		)
	}

	dictionarySize := sizeDictionary / sizeUncompressed
	log.Printf(
		"[IN SAMPLE] Total Size Compressed (with new dictionary): %fMB (%% of original size %f%%)",
		utils.BtoMb(sizeDictionary),
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
