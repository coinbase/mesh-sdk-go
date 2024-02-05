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

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/fatih/color"

	"github.com/coinbase/rosetta-sdk-go/storage/encoder"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	// DefaultBlockCacheSize is 0 MB.
	DefaultBlockCacheSize = 0

	// DefaultIndexCacheSize is 2 GB.
	DefaultIndexCacheSize = 2000 << 20

	// TinyIndexCacheSize is 10 MB.
	TinyIndexCacheSize = 10 << 20

	// DefaultMaxTableSize is 256 MB. The larger
	// this value is, the larger database transactions
	// storage can handle (~15% of the max table size
	// == max commit size).
	DefaultMaxTableSize = 256 << 20

	// DefaultLogValueSize is 64 MB.
	DefaultLogValueSize = 64 << 20

	// PerformanceMaxTableSize is 3072 MB. The larger
	// this value is, the larger database transactions
	// storage can handle (~15% of the max table size
	// == max commit size).
	PerformanceMaxTableSize = 3072 << 20

	// PerformanceLogValueSize is 256 MB.
	PerformanceLogValueSize = 256 << 20

	// AllInMemoryTableSize is 2048 MB.
	AllInMemoryTableSize = 2048 << 20

	// PerformanceLogValueSize is 512 MB.
	AllInMemoryLogValueSize = 512 << 20

	// DefaultCompressionMode is the default block
	// compression setting.
	DefaultCompressionMode = options.None

	// logModulo determines how often we should print
	// logs while scanning data.
	logModulo = 5000

	// Default GC settings for reclaiming
	// space in value logs.
	defaultGCInterval     = 1 * time.Minute
	defualtGCDiscardRatio = 0.1
	defaultGCSleep        = 10 * time.Second
)

// BadgerDatabase is a wrapper around Badger DB
// that implements the Database interface.
type BadgerDatabase struct {
	badgerOptions     badger.Options
	compressorEntries []*encoder.CompressorEntry

	pool     *encoder.BufferPool
	db       *badger.DB
	encoder  *encoder.Encoder
	compress bool

	writer       *utils.MutexMap
	writerShards int

	metaData string

	// Track the closed status to ensure we exit garbage
	// collection when the db closes.
	closed chan struct{}
}

// DefaultBadgerOptions are the default options used to initialized
// a new BadgerDB. These settings override many of the default BadgerDB
// settings to restrict memory usage to ~6 GB. If constraining memory
// usage is not desired for your use case, you can provide your own
// BadgerDB settings with the configuration option WithCustomSettings.
//
// There are many threads about optimizing memory usage in Badger (which
// can grow to many GBs if left untuned). Our own research indicates
// that each MB increase in MaxTableSize and/or ValueLogFileSize corresponds
// to a 10 MB increase in RAM usage (all other settings equal). Our primary
// concern is large database transaction size, so we configure MaxTableSize
// to be 4 times the size of ValueLogFileSize (if we skewed any further to
// MaxTableSize, we would quickly hit the default open file limit on many OSes).
func DefaultBadgerOptions(dir string) badger.Options {
	opts := badger.DefaultOptions(dir)

	// By default, we do not compress the table at all. Doing so can
	// significantly increase memory usage.
	opts.Compression = DefaultCompressionMode

	// Use an extended table size for larger commits.
	opts.MaxTableSize = DefaultMaxTableSize
	opts.ValueLogFileSize = DefaultLogValueSize

	// Don't load tables into memory.
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO

	// To allow writes at a faster speed, we create a new memtable as soon as
	// an existing memtable is filled up. This option determines how many
	// memtables should be kept in memory.
	opts.NumMemtables = 1

	// Don't keep multiple memtables in memory. With larger
	// memtable size, this explodes memory usage.
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2

	// This option will have a significant effect the memory. If the level is kept
	// in-memory, read are faster but the tables will be kept in memory. By default,
	// this is set to false.
	opts.KeepL0InMemory = false

	// We don't compact L0 on close as this can greatly delay shutdown time.
	opts.CompactL0OnClose = false

	// LoadBloomsOnOpen=false will improve the db startup speed. This is also
	// a waste to enable with a limited index cache size (as many of the loaded bloom
	// filters will be immediately discarded from the cache).
	opts.LoadBloomsOnOpen = false

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

// PerformanceBadgerOptions are performance geared
// BadgerDB options that use much more RAM than the
// default settings.
func PerformanceBadgerOptions(dir string) badger.Options {
	opts := badger.DefaultOptions(dir)

	// By default, we do not compress the table at all. Doing so can
	// significantly increase memory usage.
	opts.Compression = DefaultCompressionMode

	// Use an extended table size for larger commits.
	opts.MaxTableSize = PerformanceMaxTableSize
	opts.ValueLogFileSize = PerformanceLogValueSize

	// Load tables into memory and memory map value logs.
	opts.TableLoadingMode = options.MemoryMap
	opts.ValueLogLoadingMode = options.MemoryMap

	// This option will have a significant effect the memory. If the level is kept
	// in-memory, read are faster but the tables will be kept in memory. By default,
	// this is set to false.
	opts.KeepL0InMemory = true

	// We don't compact L0 on close as this can greatly delay shutdown time.
	opts.CompactL0OnClose = false

	// LoadBloomsOnOpen=false will improve the db startup speed. This is also
	// a waste to enable with a limited index cache size (as many of the loaded bloom
	// filters will be immediately discarded from the cache).
	opts.LoadBloomsOnOpen = true

	return opts
}

// AllInMemoryBadgerOptions are performance geared
// BadgerDB options that use much more RAM than the
// default settings and PerformanceBadger settings
func AllInMemoryBadgerOptions(dir string) badger.Options {
	opts := badger.DefaultOptions("")

	// By default, we do not compress the table at all. Doing so can
	// significantly increase memory usage.
	opts.Compression = DefaultCompressionMode

	// Use an extended table size for larger commits.
	opts.MaxTableSize = AllInMemoryTableSize
	opts.ValueLogFileSize = AllInMemoryLogValueSize

	// Load tables into memory and memory map value logs.
	opts.TableLoadingMode = options.MemoryMap
	opts.ValueLogLoadingMode = options.MemoryMap

	// This option will have a significant effect the memory. If all the levels are kept
	// in-memory, read are faster but the tables will be kept in memory. By default,
	// this is set to false.
	opts.InMemory = true

	// We don't compact L0 on close as this can greatly delay shutdown time.
	opts.CompactL0OnClose = false

	// LoadBloomsOnOpen=false will improve the db startup speed. This is also
	// a waste to enable with a limited index cache size (as many of the loaded bloom
	// filters will be immediately discarded from the cache).
	opts.LoadBloomsOnOpen = true

	return opts
}

// NewBadgerDatabase creates a new BadgerDatabase.
func NewBadgerDatabase(
	ctx context.Context,
	dir string,
	storageOptions ...BadgerOption,
) (Database, error) {
	dir = path.Clean(dir)

	b := &BadgerDatabase{
		badgerOptions: DefaultBadgerOptions(dir),
		closed:        make(chan struct{}),
		pool:          encoder.NewBufferPool(),
		compress:      true,
		writerShards:  utils.DefaultShards,
	}
	for _, opt := range storageOptions {
		opt(b)
	}

	// Initialize utis.MutexMap used to track granular
	// write transactions.
	b.writer = utils.NewMutexMap(b.writerShards)

	db, err := badger.Open(b.badgerOptions)
	if err != nil {
		err = fmt.Errorf("unable to open database: %w%s", err, b.metaData)
		color.Red(err.Error())
		return nil, err
	}
	b.db = db

	encoder, err := encoder.NewEncoder(b.compressorEntries, b.pool, b.compress)
	if err != nil {
		err = fmt.Errorf("unable to load compressor: %w%s", err, b.metaData)
		color.Red(err.Error())
		return nil, err
	}
	b.encoder = encoder

	// Start periodic ValueGC goroutine (up to user of BadgerDB to call
	// periodically to reclaim value logs on-disk).
	go b.periodicGC(ctx)

	return b, nil
}

// Close closes the database to prevent corruption.
// The caller should defer this in main.
func (b *BadgerDatabase) Close(ctx context.Context) error {
	// Trigger shutdown for the garabage collector
	close(b.closed)

	if err := b.db.Close(); err != nil {
		err = fmt.Errorf("unable to close badger database: %w%s", err, b.metaData)
		color.Red(err.Error())
		return err
	}

	return nil
}

// periodicGC attempts to reclaim storage every
// defaultGCInterval.
//
// Inspired by:
// https://github.com/ipfs/go-ds-badger/blob/a69f1020ba3954680900097e0c9d0181b88930ad/datastore.go#L173-L199
func (b *BadgerDatabase) periodicGC(ctx context.Context) {
	// We start the timeout with the default sleep to aggressively check
	// for space to reclaim on startup.
	gcTimeout := time.NewTimer(defaultGCSleep)
	defer func() {
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
				msg := fmt.Sprintf(
					"successful value log garbage collection (%s)%s",
					time.Since(start),
					b.metaData,
				)
				color.Cyan(msg)
				log.Print(msg)
				gcTimeout.Reset(defaultGCSleep)
			default:
				// Not much we can do on a random error but log it and continue.
				msg := fmt.Sprintf("error during a GC cycle: %s%s\n", err.Error(), b.metaData)
				color.Cyan(msg)
				log.Print(msg)
				gcTimeout.Reset(defaultGCInterval)
			}
		}
	}
}

// Encoder returns the BadgerDatabase encoder.
func (b *BadgerDatabase) Encoder() *encoder.Encoder {
	return b.encoder
}

// BadgerTransaction is a wrapper around a Badger
// DB transaction that implements the DatabaseTransaction
// interface.
type BadgerTransaction struct {
	db     *BadgerDatabase
	txn    *badger.Txn
	rwLock sync.RWMutex

	holdGlobal bool
	identifier string

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

// Transaction creates a new exclusive write BadgerTransaction.
func (b *BadgerDatabase) Transaction(
	ctx context.Context,
) Transaction {
	b.writer.GLock()

	return &BadgerTransaction{
		db:               b,
		txn:              b.db.NewTransaction(true),
		holdGlobal:       true,
		buffersToReclaim: []*bytes.Buffer{},
	}
}

// ReadTransaction creates a new read BadgerTransaction.
func (b *BadgerDatabase) ReadTransaction(
	ctx context.Context,
) Transaction {
	return &BadgerTransaction{
		db:               b,
		txn:              b.db.NewTransaction(false),
		buffersToReclaim: []*bytes.Buffer{},
	}
}

// WriteTransaction creates a new write BadgerTransaction
// for a particular identifier.
func (b *BadgerDatabase) WriteTransaction(
	ctx context.Context,
	identifier string,
	priority bool,
) Transaction {
	b.writer.Lock(identifier, priority)

	return &BadgerTransaction{
		db:               b,
		txn:              b.db.NewTransaction(true),
		identifier:       identifier,
		buffersToReclaim: []*bytes.Buffer{},
	}
}

func (b *BadgerTransaction) releaseLocks() {
	if b.holdGlobal {
		b.holdGlobal = false
		b.db.writer.GUnlock()
	}
	if len(b.identifier) > 0 {
		b.db.writer.Unlock(b.identifier)
		b.identifier = ""
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
	b.releaseLocks()

	if err != nil {
		err = fmt.Errorf("unable to commit transaction: %w%s", err, b.db.metaData)
		color.Red(err.Error())
		return err
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

	b.releaseLocks()
}

// Set changes the value of the key to the value within a transaction.
func (b *BadgerTransaction) Set(
	ctx context.Context,
	key []byte,
	value []byte,
	reclaimValue bool,
) error {
	b.rwLock.Lock()
	defer b.rwLock.Unlock()

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
	b.rwLock.RLock()
	defer b.rwLock.RUnlock()

	value := b.db.pool.Get()
	item, err := b.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		err = fmt.Errorf("unable to get the item of key %s within a transaction: %w%s", string(key), err, b.db.metaData)
		color.Red(err.Error())
		return false, nil, err
	}

	err = item.Value(func(v []byte) error {
		_, err := value.Write(v)
		return err
	})
	if err != nil {
		err = fmt.Errorf(
			" %s: %w%s",
			string(key),
			err,
			b.db.metaData,
		)
		color.Red(err.Error())
		return false, nil, err
	}

	return true, value.Bytes(), nil
}

// Delete removes the key and its value within the transaction.
func (b *BadgerTransaction) Delete(ctx context.Context, key []byte) error {
	b.rwLock.Lock()
	defer b.rwLock.Unlock()

	return b.txn.Delete(key)
}

// Scan calls a worker for each item in a scan instead
// of reading all items into memory.
func (b *BadgerTransaction) Scan(
	ctx context.Context,
	prefix []byte,
	seekStart []byte,
	worker func([]byte, []byte) error,
	logEntries bool,
	reverse bool, // reverse == true means greatest to least
) (int, error) {
	b.rwLock.RLock()
	defer b.rwLock.RUnlock()

	entries := 0
	opts := badger.DefaultIteratorOptions
	opts.Reverse = reverse
	it := b.txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(seekStart); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.Key()
		err := item.Value(func(v []byte) error {
			if err := worker(k, v); err != nil {
				return fmt.Errorf("worker failed for key %s: %w", string(k), err)
			}

			return nil
		})
		if err != nil {
			return -1, fmt.Errorf(
				"unable to get the value from the item for key %s: %w",
				string(k),
				err,
			)
		}

		entries++
		if logEntries && entries%logModulo == 0 {
			log.Printf("scanned %d entries for %s\n", entries, string(prefix))
		}
	}

	return entries, nil
}

func decompressAndSave(
	encoder *encoder.Encoder,
	namespace string,
	tmpDir string,
	k []byte,
	v []byte,
) (float64, float64, error) {
	// We use a compressor.DecodeRaw here because the v may be
	// encoded using dictionary compression.
	decompressed, err := encoder.DecodeRaw(namespace, v)
	if err != nil {
		return -1, -1, fmt.Errorf(
			"unable to decompress for namespace %s and input %s: %w",
			namespace,
			string(v),
			err,
		)
	}

	err = ioutil.WriteFile(
		path.Join(tmpDir, types.Hash(string(k))),
		decompressed,
		os.FileMode(utils.DefaultFilePermissions),
	)
	if err != nil {
		return -1, -1, fmt.Errorf(
			"unable to write decompress file %s: %w",
			path.Join(tmpDir, types.Hash(string(k))),
			err,
		)
	}

	return float64(len(decompressed)), float64(len(v)), nil
}

// GetInfo returns customized metaData for db's metaData
func (b *BadgerDatabase) GetMetaData() string {
	return b.metaData
}

func decompressAndEncode(
	path string,
	namespace string,
	encoder *encoder.Encoder,
) (float64, float64, float64, error) {
	decompressed, err := ioutil.ReadFile(path) // #nosec G304
	if err != nil {
		return -1, -1, -1, fmt.Errorf(
			"unable to read decompress file %s: %w",
			path,
			err,
		)
	}

	normalCompress, err := encoder.EncodeRaw("", decompressed)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("unable to compress normal: %w", err)
	}

	dictCompress, err := encoder.EncodeRaw(namespace, decompressed)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("unable to compress with dictionary: %w", err)
	}

	// Ensure dict works
	decompressedDict, err := encoder.DecodeRaw(namespace, dictCompress)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("unable to decompress with dictionary: %w", err)
	}

	if types.Hash(decompressed) != types.Hash(decompressedDict) {
		return -1, -1, -1, storageErrs.ErrDecompressOutputMismatch
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
	newCompressor *encoder.Encoder,
) (float64, float64, error) {
	onDiskSize := float64(0)
	newSize := float64(0)

	txn := badgerDb.ReadTransaction(ctx)
	defer txn.Discard(ctx)
	_, err := txn.Scan(
		ctx,
		[]byte(restrictedNamespace),
		[]byte(restrictedNamespace),
		func(k []byte, v []byte) error {
			decompressed, err := badgerDb.Encoder().DecodeRaw(namespace, v)
			if err != nil {
				return fmt.Errorf(
					"unable to decompress for namespace %s and input %s: %w",
					namespace,
					string(v),
					err,
				)
			}

			newCompressed, err := newCompressor.EncodeRaw(namespace, decompressed)
			if err != nil {
				return fmt.Errorf("unable to compress with dictionary: %w", err)
			}
			onDiskSize += float64(len(v))
			newSize += float64(len(newCompressed))

			return nil
		},
		true,
		false,
	)
	if err != nil {
		return -1, -1, fmt.Errorf("unable to recompress: %w", err)
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

// BadgerTrain creates a zstd dictionary for a given BadgerDatabase DB namespace.
// Optionally, you can specify the maximum number of entries to load into
// storage (if -1 is provided, then all possible are loaded).
func BadgerTrain(
	ctx context.Context,
	namespace string,
	db string,
	output string,
	maxEntries int,
	compressorEntries []*encoder.CompressorEntry,
) (float64, float64, error) {
	badgerDb, err := NewBadgerDatabase(
		ctx,
		path.Clean(db),
		WithCompressorEntries(compressorEntries),
	)
	if err != nil {
		return -1, -1, fmt.Errorf("unable to load database: %w", err)
	}
	defer badgerDb.Close(ctx)

	// Create directory to store uncompressed files for training
	tmpDir, err := utils.CreateTempDir()
	if err != nil {
		return -1, -1, fmt.Errorf("unable to create temporary directory: %w", err)
	}
	defer utils.RemoveTempDir(tmpDir)

	// We must use a restricted namespace or we will inadvertently
	// fetch all namespaces that contain the namespace we care about.
	restrictedNamespace := fmt.Sprintf("%s/", namespace)

	totalUncompressedSize := float64(0)
	totalDiskSize := float64(0)
	entriesSeen := 0
	txn := badgerDb.ReadTransaction(ctx)
	defer txn.Discard(ctx)
	_, err = txn.Scan(
		ctx,
		[]byte(restrictedNamespace),
		[]byte(restrictedNamespace),
		func(k []byte, v []byte) error {
			decompressedSize, diskSize, err := decompressAndSave(
				badgerDb.Encoder(),
				namespace,
				tmpDir,
				k,
				v,
			)
			if err != nil {
				return fmt.Errorf("unable to decompress and save: %w", err)
			}

			totalUncompressedSize += decompressedSize
			totalDiskSize += diskSize
			entriesSeen++

			if entriesSeen > maxEntries-1 && maxEntries != -1 {
				return storageErrs.ErrMaxEntries
			}

			return nil
		},
		true,
		false,
	)
	if err != nil && !errors.Is(err, storageErrs.ErrMaxEntries) {
		return -1, -1, fmt.Errorf("%w for %s: %v", storageErrs.ErrScanFailed, namespace, err)
	}

	if entriesSeen == 0 {
		return -1, -1, fmt.Errorf("%w %s", storageErrs.ErrNoEntriesFoundInNamespace, namespace)
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
		return -1, -1, fmt.Errorf("unable to start zstd: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return -1, -1, fmt.Errorf("unable to train zstd: %w", err)
	}

	encoder, err := encoder.NewEncoder([]*encoder.CompressorEntry{
		{
			Namespace:      namespace,
			DictionaryPath: dictPath,
		},
	}, encoder.NewBufferPool(), true)
	if err != nil {
		return -1, -1, fmt.Errorf("unable to load compressor: %w", err)
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
			encoder,
		)
		if err != nil {
			return fmt.Errorf("unable to decompress and encode: %w", err)
		}

		sizeUncompressed += decompressed
		sizeNormal += normalCompress
		sizeDictionary += dictCompress

		return nil
	})
	if err != nil {
		return -1, -1, fmt.Errorf("unable to walk files: %w", err)
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
		encoder,
	)
}
