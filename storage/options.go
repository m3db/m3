// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package storage

import (
	"io"
	"os"
	"time"

	"github.com/m3db/m3db/encoding/tsz"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/metrics"
)

const (
	// defaultBlockSize is the default block size
	defaultBlockSize = 2 * time.Hour

	// defaultBufferFuture is the default buffer future limit
	defaultBufferFuture = 2 * time.Minute

	// defaultBufferPast is the default buffer past limit
	defaultBufferPast = 10 * time.Minute

	// defaultBufferDrain is the default buffer drain
	defaultBufferDrain = 1 * time.Minute

	// defaultRetentionPeriod is how long we keep data in memory by default.
	defaultRetentionPeriod = 2 * 24 * time.Hour

	// defaultBufferBucketAllocSize is the size to allocate for values for each
	// bucket in the buffer, this should match the size of expected encoded values
	// per buffer drain duration
	defaultBufferBucketAllocSize = 256

	// defaultDatabaseBlockAllocSize is the size to allocate for values for each
	// database block, this should match the size of expected encoded values per
	// block size.
	defaultDatabaseBlockAllocSize = 1024

	// defaultMaxFlushRetries is the default number of retries when flush fails.
	defaultMaxFlushRetries = 3
)

var (
	// defaultFilePathPrefix is the default path prefix for local TSDB files.
	defaultFilePathPrefix = os.TempDir()

	// defaultFileWriterOptions is the default file writing options.
	defaultFileWriterOptions = fs.NewFileWriterOptions()

	// defaultFileSetReaderFn is the default function for creating a TSDB fileset reader.
	defaultFileSetReaderFn = func(filePathPrefix string) m3db.FileSetReader {
		return fs.NewReader(filePathPrefix)
	}

	// defaultFileSetWriterFn is the default function for creating a TSDB fileset writer.
	defaultFileSetWriterFn = func(blockSize time.Duration, filePathPrefix string) m3db.FileSetWriter {
		return fs.NewWriter(blockSize, filePathPrefix, defaultFileWriterOptions)
	}

	// defaultPersistenceManagerFn is the default function for creating a new persistence manager.
	defaultPersistenceManagerFn = func(opts m3db.DatabaseOptions) m3db.PersistenceManager {
		return fs.NewPersistenceManager(opts)
	}

	timeZero time.Time
)

type dbOptions struct {
	logger                  xlog.Logger
	scope                   xmetrics.Scope
	blockSize               time.Duration
	newEncoderFn            m3db.NewEncoderFn
	newDecoderFn            m3db.NewDecoderFn
	nowFn                   m3db.NowFn
	bufferFuture            time.Duration
	bufferPast              time.Duration
	bufferDrain             time.Duration
	bufferBucketAllocSize   int
	databaseBlockAllocSize  int
	retentionPeriod         time.Duration
	newBootstrapFn          m3db.NewBootstrapFn
	bytesPool               m3db.BytesPool
	contextPool             m3db.ContextPool
	databaseBlockPool       m3db.DatabaseBlockPool
	encoderPool             m3db.EncoderPool
	segmentReaderPool       m3db.SegmentReaderPool
	readerIteratorPool      m3db.ReaderIteratorPool
	multiReaderIteratorPool m3db.MultiReaderIteratorPool
	maxFlushRetries         int
	filePathPrefix          string
	fileWriterOptions       m3db.FileWriterOptions
	newFileSetReaderFn      m3db.NewFileSetReaderFn
	newFileSetWriterFn      m3db.NewFileSetWriterFn
	newPersistenceManagerFn m3db.NewPersistenceManagerFn
}

// NewDatabaseOptions creates a new set of database options with defaults
// TODO(r): add an "IsValid()" method and ensure buffer future and buffer past are
// less than blocksize and check when opening database
func NewDatabaseOptions() m3db.DatabaseOptions {
	opts := &dbOptions{
		logger:                  xlog.SimpleLogger,
		scope:                   xmetrics.NoopScope,
		blockSize:               defaultBlockSize,
		nowFn:                   time.Now,
		retentionPeriod:         defaultRetentionPeriod,
		bufferFuture:            defaultBufferFuture,
		bufferPast:              defaultBufferPast,
		bufferDrain:             defaultBufferDrain,
		maxFlushRetries:         defaultMaxFlushRetries,
		filePathPrefix:          defaultFilePathPrefix,
		fileWriterOptions:       defaultFileWriterOptions,
		newFileSetReaderFn:      defaultFileSetReaderFn,
		newFileSetWriterFn:      defaultFileSetWriterFn,
		newPersistenceManagerFn: defaultPersistenceManagerFn,
	}
	return opts.EncodingTszPooled(defaultBufferBucketAllocSize, defaultDatabaseBlockAllocSize)
}

func (o *dbOptions) EncodingTszPooled(bufferBucketAllocSize, databaseBlockAllocSize int) m3db.DatabaseOptions {
	opts := *o
	opts.bufferBucketAllocSize = bufferBucketAllocSize
	opts.databaseBlockAllocSize = databaseBlockAllocSize

	// NB(r): don't enable byte pooling just yet
	buckets := []m3db.PoolBucket{}
	bytesPool := pool.NewBytesPool(buckets)
	bytesPool.Init()
	opts.bytesPool = bytesPool

	// initialize context pool
	contextPool := pool.NewContextPool(0)
	contextPool.Init()
	opts.contextPool = contextPool

	// initialize database block pool
	databaseBlockPool := pool.NewDatabaseBlockPool(0)
	databaseBlockPool.Init(func() m3db.DatabaseBlock {
		return NewDatabaseBlock(timeZero, nil, &opts)
	})
	opts.databaseBlockPool = databaseBlockPool

	// initialize segment reader pool
	segmentReaderPool := pool.NewSegmentReaderPool(0)
	segmentReaderPool.Init()
	opts.segmentReaderPool = segmentReaderPool

	encoderPool := pool.NewEncoderPool(0)
	readerIteratorPool := pool.NewReaderIteratorPool(0)
	multiReaderIteratorPool := pool.NewMultiReaderIteratorPool(0)

	encodingOpts := tsz.NewOptions().
		BytesPool(bytesPool).
		EncoderPool(encoderPool).
		ReaderIteratorPool(readerIteratorPool).
		SegmentReaderPool(segmentReaderPool)

	// initialize encoder pool
	encoderPool.Init(func() m3db.Encoder {
		return tsz.NewEncoder(timeZero, nil, encodingOpts)
	})
	opts.encoderPool = encoderPool

	// initialize single reader iterator pool
	readerIteratorPool.Init(func(r io.Reader) m3db.ReaderIterator {
		return tsz.NewReaderIterator(r, encodingOpts)
	})
	opts.readerIteratorPool = readerIteratorPool

	// initialize multi reader iterator pool
	multiReaderIteratorPool.Init(func(r io.Reader) m3db.ReaderIterator {
		return tsz.NewReaderIterator(r, encodingOpts)
	})
	opts.multiReaderIteratorPool = multiReaderIteratorPool

	return (&opts).encodingTsz(encodingOpts)
}

func (o *dbOptions) EncodingTsz() m3db.DatabaseOptions {
	return o.encodingTsz(tsz.NewOptions())
}

func (o *dbOptions) encodingTsz(encodingOpts tsz.Options) m3db.DatabaseOptions {
	opts := *o

	newEncoderFn := func(start time.Time, bytes []byte) m3db.Encoder {
		return tsz.NewEncoder(start, bytes, encodingOpts)
	}
	opts.newEncoderFn = newEncoderFn

	newDecoderFn := func() m3db.Decoder {
		return tsz.NewDecoder(encodingOpts)
	}
	opts.newDecoderFn = newDecoderFn

	return &opts
}

func (o *dbOptions) Logger(value xlog.Logger) m3db.DatabaseOptions {
	opts := *o
	opts.logger = value
	return &opts
}

func (o *dbOptions) GetLogger() xlog.Logger {
	return o.logger
}

func (o *dbOptions) MetricsScope(value xmetrics.Scope) m3db.DatabaseOptions {
	opts := *o
	opts.scope = value
	return &opts
}

func (o *dbOptions) GetMetricsScope() xmetrics.Scope {
	return o.scope
}

func (o *dbOptions) BlockSize(value time.Duration) m3db.DatabaseOptions {
	opts := *o
	opts.blockSize = value
	return &opts
}

func (o *dbOptions) GetBlockSize() time.Duration {
	return o.blockSize
}

func (o *dbOptions) NewEncoderFn(value m3db.NewEncoderFn) m3db.DatabaseOptions {
	opts := *o
	opts.newEncoderFn = value
	return &opts
}

func (o *dbOptions) GetNewEncoderFn() m3db.NewEncoderFn {
	return o.newEncoderFn
}

func (o *dbOptions) NewDecoderFn(value m3db.NewDecoderFn) m3db.DatabaseOptions {
	opts := *o
	opts.newDecoderFn = value
	return &opts
}

func (o *dbOptions) GetNewDecoderFn() m3db.NewDecoderFn {
	return o.newDecoderFn
}

func (o *dbOptions) NowFn(value m3db.NowFn) m3db.DatabaseOptions {
	opts := *o
	opts.nowFn = value
	return &opts
}

func (o *dbOptions) GetNowFn() m3db.NowFn {
	return o.nowFn
}

func (o *dbOptions) BufferFuture(value time.Duration) m3db.DatabaseOptions {
	opts := *o
	opts.bufferFuture = value
	return &opts
}

func (o *dbOptions) GetBufferFuture() time.Duration {
	return o.bufferFuture
}

func (o *dbOptions) BufferPast(value time.Duration) m3db.DatabaseOptions {
	opts := *o
	opts.bufferPast = value
	return &opts
}

func (o *dbOptions) GetBufferPast() time.Duration {
	return o.bufferPast
}

func (o *dbOptions) BufferDrain(value time.Duration) m3db.DatabaseOptions {
	opts := *o
	opts.bufferDrain = value
	return &opts
}

func (o *dbOptions) GetBufferDrain() time.Duration {
	return o.bufferDrain
}

func (o *dbOptions) BufferBucketAllocSize(value int) m3db.DatabaseOptions {
	opts := *o
	opts.bufferBucketAllocSize = value
	return &opts
}

func (o *dbOptions) GetBufferBucketAllocSize() int {
	return o.bufferBucketAllocSize
}

func (o *dbOptions) DatabaseBlockAllocSize(value int) m3db.DatabaseOptions {
	opts := *o
	opts.databaseBlockAllocSize = value
	return &opts
}

func (o *dbOptions) GetDatabaseBlockAllocSize() int {
	return o.databaseBlockAllocSize
}

// RetentionPeriod sets how long we intend to keep data in memory.
func (o *dbOptions) RetentionPeriod(value time.Duration) m3db.DatabaseOptions {
	opts := *o
	opts.retentionPeriod = value
	return &opts
}

// GetRetentionPeriod returns how long we intend to keep raw xmetrics in memory.
func (o *dbOptions) GetRetentionPeriod() time.Duration {
	return o.retentionPeriod
}

func (o *dbOptions) NewBootstrapFn(value m3db.NewBootstrapFn) m3db.DatabaseOptions {
	opts := *o
	opts.newBootstrapFn = value
	return &opts
}

func (o *dbOptions) GetBootstrapFn() m3db.NewBootstrapFn {
	return o.newBootstrapFn
}

func (o *dbOptions) BytesPool(value m3db.BytesPool) m3db.DatabaseOptions {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *dbOptions) GetBytesPool() m3db.BytesPool {
	return o.bytesPool
}

func (o *dbOptions) ContextPool(value m3db.ContextPool) m3db.DatabaseOptions {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *dbOptions) GetContextPool() m3db.ContextPool {
	return o.contextPool
}

func (o *dbOptions) DatabaseBlockPool(value m3db.DatabaseBlockPool) m3db.DatabaseOptions {
	opts := *o
	opts.databaseBlockPool = value
	return &opts
}

func (o *dbOptions) GetDatabaseBlockPool() m3db.DatabaseBlockPool {
	return o.databaseBlockPool
}

func (o *dbOptions) EncoderPool(value m3db.EncoderPool) m3db.DatabaseOptions {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *dbOptions) GetEncoderPool() m3db.EncoderPool {
	return o.encoderPool
}

func (o *dbOptions) SegmentReaderPool(value m3db.SegmentReaderPool) m3db.DatabaseOptions {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *dbOptions) GetSegmentReaderPool() m3db.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *dbOptions) ReaderIteratorPool(value m3db.ReaderIteratorPool) m3db.DatabaseOptions {
	opts := *o
	opts.readerIteratorPool = value
	return &opts
}

func (o *dbOptions) GetReaderIteratorPool() m3db.ReaderIteratorPool {
	return o.readerIteratorPool
}

func (o *dbOptions) MultiReaderIteratorPool(value m3db.MultiReaderIteratorPool) m3db.DatabaseOptions {
	opts := *o
	opts.multiReaderIteratorPool = value
	return &opts
}

func (o *dbOptions) GetMultiReaderIteratorPool() m3db.MultiReaderIteratorPool {
	return o.multiReaderIteratorPool
}

func (o *dbOptions) MaxFlushRetries(value int) m3db.DatabaseOptions {
	opts := *o
	opts.maxFlushRetries = value
	return &opts
}

func (o *dbOptions) GetMaxFlushRetries() int {
	return o.maxFlushRetries
}

func (o *dbOptions) FilePathPrefix(value string) m3db.DatabaseOptions {
	opts := *o
	opts.filePathPrefix = value
	return &opts
}

func (o *dbOptions) GetFilePathPrefix() string {
	return o.filePathPrefix
}

func (o *dbOptions) FileWriterOptions(value m3db.FileWriterOptions) m3db.DatabaseOptions {
	opts := *o
	opts.fileWriterOptions = value
	return &opts
}

func (o *dbOptions) GetFileWriterOptions() m3db.FileWriterOptions {
	return o.fileWriterOptions
}

func (o *dbOptions) NewFileSetReaderFn(value m3db.NewFileSetReaderFn) m3db.DatabaseOptions {
	opts := *o
	opts.newFileSetReaderFn = value
	return &opts
}

func (o *dbOptions) GetNewFileSetReaderFn() m3db.NewFileSetReaderFn {
	return o.newFileSetReaderFn
}

func (o *dbOptions) NewFileSetWriterFn(value m3db.NewFileSetWriterFn) m3db.DatabaseOptions {
	opts := *o
	opts.newFileSetWriterFn = value
	return &opts
}

func (o *dbOptions) GetNewFileSetWriterFn() m3db.NewFileSetWriterFn {
	return o.newFileSetWriterFn
}

func (o *dbOptions) NewPersistenceManagerFn(value m3db.NewPersistenceManagerFn) m3db.DatabaseOptions {
	opts := *o
	opts.newPersistenceManagerFn = value
	return &opts
}

func (o *dbOptions) GetNewPersistenceManagerFn() m3db.NewPersistenceManagerFn {
	return o.newPersistenceManagerFn
}
