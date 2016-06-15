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
	"time"

	"github.com/m3db/m3db"
	"github.com/m3db/m3db/encoding/tsz"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/x/logging"
	"github.com/m3db/m3db/x/metrics"
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

	// defaultSeriesPooling is the default series expected to base pooling on
	defaultSeriesPooling = 1000

	// defaultMaxFlushRetries is the default number of retries when flush fails.
	defaultMaxFlushRetries = 3

	// defaultFilePathPrefix is the default path prefix for local TSDB files.
	defaultFilePathPrefix = "/var/log/memtsdb"
)

var (
	timeZero time.Time
)

type dbOptions struct {
	logger                logging.Logger
	scope                 metrics.Scope
	blockSize             time.Duration
	newEncoderFn          memtsdb.NewEncoderFn
	newDecoderFn          memtsdb.NewDecoderFn
	nowFn                 memtsdb.NowFn
	bufferFuture          time.Duration
	bufferPast            time.Duration
	bufferDrain           time.Duration
	bufferBucketAllocSize int
	retentionPeriod       time.Duration
	newBootstrapFn        memtsdb.NewBootstrapFn
	bytesPool             memtsdb.BytesPool
	contextPool           memtsdb.ContextPool
	encoderPool           memtsdb.EncoderPool
	iteratorPool          memtsdb.IteratorPool
	maxFlushRetries       int
	filePathPrefix        string
	newWriterFn           fs.NewWriterFn
}

// NewDatabaseOptions creates a new set of database options with defaults
// TODO(r): add an "IsValid()" method and ensure buffer future and buffer past are
// less than blocksize and check when opening database
func NewDatabaseOptions() memtsdb.DatabaseOptions {
	opts := &dbOptions{
		logger:          logging.SimpleLogger,
		scope:           metrics.NoopScope,
		blockSize:       defaultBlockSize,
		nowFn:           time.Now,
		retentionPeriod: defaultRetentionPeriod,
		bufferFuture:    defaultBufferFuture,
		bufferPast:      defaultBufferPast,
		bufferDrain:     defaultBufferDrain,
		maxFlushRetries: defaultMaxFlushRetries,
		filePathPrefix:  defaultFilePathPrefix,
		newWriterFn: func(blockSize time.Duration, filePathPrefix string) fs.Writer {
			return fs.NewWriter(blockSize, filePathPrefix, fs.NewWriterOptions())
		},
	}
	return opts.EncodingTszPooled(defaultBufferBucketAllocSize, defaultSeriesPooling)
}

func (o *dbOptions) EncodingTszPooled(bufferBucketAllocSize, series int) memtsdb.DatabaseOptions {
	// NB(r): don't enable byte pooling just yet
	buckets := []memtsdb.PoolBucket{}

	segmentReaderPool := pool.NewSegmentReaderPool(0)
	encoderPool := pool.NewEncoderPool(0)
	bytesPool := pool.NewBytesPool(buckets)
	contextPool := pool.NewContextPool(0)
	iteratorPool := pool.NewIteratorPool(0)

	segmentReaderPool.Init()
	bytesPool.Init()
	contextPool.Init()

	encodingOpts := tsz.NewOptions().
		Pool(encoderPool).
		IteratorPool(iteratorPool).
		BytesPool(bytesPool).
		SegmentReaderPool(segmentReaderPool)

	encoderPool.Init(func() memtsdb.Encoder {
		return tsz.NewEncoder(timeZero, nil, encodingOpts)
	})

	iteratorPool.Init(func() memtsdb.Iterator {
		return tsz.NewIterator(nil, encodingOpts)
	})

	newEncoderFn := func(start time.Time, bytes []byte) memtsdb.Encoder {
		return tsz.NewEncoder(start, bytes, encodingOpts)
	}
	newDecoderFn := func() memtsdb.Decoder {
		return tsz.NewDecoder(encodingOpts)
	}

	opts := *o
	opts.newEncoderFn = newEncoderFn
	opts.newDecoderFn = newDecoderFn
	opts.bufferBucketAllocSize = bufferBucketAllocSize
	opts.bytesPool = bytesPool
	opts.contextPool = contextPool
	opts.encoderPool = encoderPool
	opts.iteratorPool = iteratorPool
	return &opts
}

func (o *dbOptions) Logger(value logging.Logger) memtsdb.DatabaseOptions {
	opts := *o
	opts.logger = value
	return &opts
}

func (o *dbOptions) GetLogger() logging.Logger {
	return o.logger
}

func (o *dbOptions) MetricsScope(value metrics.Scope) memtsdb.DatabaseOptions {
	opts := *o
	opts.scope = value
	return &opts
}

func (o *dbOptions) GetMetricsScope() metrics.Scope {
	return o.scope
}

func (o *dbOptions) BlockSize(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.blockSize = value
	return &opts
}

func (o *dbOptions) GetBlockSize() time.Duration {
	return o.blockSize
}

func (o *dbOptions) NewEncoderFn(value memtsdb.NewEncoderFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.newEncoderFn = value
	return &opts
}

func (o *dbOptions) GetNewEncoderFn() memtsdb.NewEncoderFn {
	return o.newEncoderFn
}

func (o *dbOptions) NewDecoderFn(value memtsdb.NewDecoderFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.newDecoderFn = value
	return &opts
}

func (o *dbOptions) GetNewDecoderFn() memtsdb.NewDecoderFn {
	return o.newDecoderFn
}

func (o *dbOptions) NowFn(value memtsdb.NowFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.nowFn = value
	return &opts
}

func (o *dbOptions) GetNowFn() memtsdb.NowFn {
	return o.nowFn
}

func (o *dbOptions) BufferFuture(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferFuture = value
	return &opts
}

func (o *dbOptions) GetBufferFuture() time.Duration {
	return o.bufferFuture
}

func (o *dbOptions) BufferPast(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferPast = value
	return &opts
}

func (o *dbOptions) GetBufferPast() time.Duration {
	return o.bufferPast
}

func (o *dbOptions) BufferDrain(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferDrain = value
	return &opts
}

func (o *dbOptions) GetBufferDrain() time.Duration {
	return o.bufferDrain
}

func (o *dbOptions) BufferBucketAllocSize(value int) memtsdb.DatabaseOptions {
	opts := *o
	opts.bufferBucketAllocSize = value
	return &opts
}

func (o *dbOptions) GetBufferBucketAllocSize() int {
	return o.bufferBucketAllocSize
}

// RetentionPeriod sets how long we intend to keep data in memory.
func (o *dbOptions) RetentionPeriod(value time.Duration) memtsdb.DatabaseOptions {
	opts := *o
	opts.retentionPeriod = value
	return &opts
}

// GetRetentionPeriod returns how long we intend to keep raw metrics in memory.
func (o *dbOptions) GetRetentionPeriod() time.Duration {
	return o.retentionPeriod
}

func (o *dbOptions) NewBootstrapFn(value memtsdb.NewBootstrapFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.newBootstrapFn = value
	return &opts
}

func (o *dbOptions) GetBootstrapFn() memtsdb.NewBootstrapFn {
	return o.newBootstrapFn
}

func (o *dbOptions) BytesPool(value memtsdb.BytesPool) memtsdb.DatabaseOptions {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *dbOptions) GetBytesPool() memtsdb.BytesPool {
	return o.bytesPool
}

func (o *dbOptions) ContextPool(value memtsdb.ContextPool) memtsdb.DatabaseOptions {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *dbOptions) GetContextPool() memtsdb.ContextPool {
	return o.contextPool
}

func (o *dbOptions) EncoderPool(value memtsdb.EncoderPool) memtsdb.DatabaseOptions {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *dbOptions) GetEncoderPool() memtsdb.EncoderPool {
	return o.encoderPool
}

func (o *dbOptions) IteratorPool(value memtsdb.IteratorPool) memtsdb.DatabaseOptions {
	opts := *o
	opts.iteratorPool = value
	return &opts
}

func (o *dbOptions) GetIteratorPool() memtsdb.IteratorPool {
	return o.iteratorPool
}

func (o *dbOptions) MaxFlushRetries(value int) memtsdb.DatabaseOptions {
	opts := *o
	opts.maxFlushRetries = value
	return &opts
}

func (o *dbOptions) GetMaxFlushRetries() int {
	return o.maxFlushRetries
}

func (o *dbOptions) FilePathPrefix(value string) memtsdb.DatabaseOptions {
	opts := *o
	opts.filePathPrefix = value
	return &opts
}

func (o *dbOptions) GetFilePathPrefix() string {
	return o.filePathPrefix
}

func (o *dbOptions) NewWriterFn(value fs.NewWriterFn) memtsdb.DatabaseOptions {
	opts := *o
	opts.newWriterFn = value
	return &opts
}

func (o *dbOptions) GetNewWriterFn() fs.NewWriterFn {
	return o.newWriterFn
}
