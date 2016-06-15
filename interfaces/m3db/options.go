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

package m3db

import (
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/x/logging"
	"github.com/m3db/m3db/x/metrics"
)

// NowFn is the function supplied to determine "now"
type NowFn func() time.Time

// DatabaseOptions is a set of database options
type DatabaseOptions interface {
	// EncodingTszPooled sets tsz encoding with pooling and returns a new DatabaseOptions
	EncodingTszPooled(bufferBucketAllocSize, series int) DatabaseOptions

	// Logger sets the logger and returns a new DatabaseOptions
	Logger(value logging.Logger) DatabaseOptions

	// GetLogger returns the logger
	GetLogger() logging.Logger

	// MetricsScope sets the metricsScope and returns a new DatabaseOptions
	MetricsScope(value metrics.Scope) DatabaseOptions

	// GetMetricsScope returns the metricsScope
	GetMetricsScope() metrics.Scope

	// BlockSize sets the blockSize and returns a new DatabaseOptions
	BlockSize(value time.Duration) DatabaseOptions

	// GetBlockSize returns the blockSize
	GetBlockSize() time.Duration

	// NewEncoderFn sets the newEncoderFn and returns a new DatabaseOptions
	// TODO(r): now that we have an encoder pool consider removing newencoderfn being required
	NewEncoderFn(value NewEncoderFn) DatabaseOptions

	// GetNewEncoderFn returns the newEncoderFn
	GetNewEncoderFn() NewEncoderFn

	// NewDecoderFn sets the newDecoderFn and returns a new DatabaseOptions
	NewDecoderFn(value NewDecoderFn) DatabaseOptions

	// GetNewDecoderFn returns the newDecoderFn
	GetNewDecoderFn() NewDecoderFn

	// NowFn sets the nowFn and returns a new DatabaseOptions
	NowFn(value NowFn) DatabaseOptions

	// GetNowFn returns the nowFn
	GetNowFn() NowFn

	// BufferFuture sets the bufferFuture and returns a new DatabaseOptions
	BufferFuture(value time.Duration) DatabaseOptions

	// GetBufferFuture returns the bufferFuture
	GetBufferFuture() time.Duration

	// BufferPast sets the bufferPast and returns a new DatabaseOptions
	BufferPast(value time.Duration) DatabaseOptions

	// GetBufferPast returns the bufferPast
	GetBufferPast() time.Duration

	// BufferDrain sets the bufferDrain and returns a new DatabaseOptions
	BufferDrain(value time.Duration) DatabaseOptions

	// GetBufferDrain returns the bufferDrain
	GetBufferDrain() time.Duration

	// BufferBucketAllocSize sets the bufferBucketAllocSize and returns a new DatabaseOptions
	BufferBucketAllocSize(value int) DatabaseOptions

	// GetBufferBucketAllocSize returns the bufferBucketAllocSize
	GetBufferBucketAllocSize() int

	// RetentionPeriod sets how long we intend to keep data in memory.
	RetentionPeriod(value time.Duration) DatabaseOptions

	// RetentionPeriod is how long we intend to keep data in memory.
	GetRetentionPeriod() time.Duration

	// NewBootstrapFn sets the newBootstrapFn and returns a new DatabaseOptions
	NewBootstrapFn(value NewBootstrapFn) DatabaseOptions

	// GetBootstrapFn returns the newBootstrapFn
	GetBootstrapFn() NewBootstrapFn

	// BytesPool sets the bytesPool and returns a new DatabaseOptions
	BytesPool(value BytesPool) DatabaseOptions

	// GetBytesPool returns the bytesPool
	GetBytesPool() BytesPool

	// ContextPool sets the contextPool and returns a new DatabaseOptions
	ContextPool(value ContextPool) DatabaseOptions

	// GetContextPool returns the contextPool
	GetContextPool() ContextPool

	// EncoderPool sets the encoderPool and returns a new DatabaseOptions
	EncoderPool(value EncoderPool) DatabaseOptions

	// GetEncoderPool returns the encoderPool
	GetEncoderPool() EncoderPool

	// IteratorPool sets the iteratorPool and returns a new DatabaseOptions
	IteratorPool(value IteratorPool) DatabaseOptions

	// GetIteratorPool returns the iteratorPool
	GetIteratorPool() IteratorPool

	// MaxFlushRetries sets the maximum number of retries when data flushing fails.
	MaxFlushRetries(value int) DatabaseOptions

	// GetMaxFlushRetries returns the maximum number of retries when data flushing fails.
	GetMaxFlushRetries() int

	// FilePathPrefix sets the file path prefix for sharded TSDB files.
	FilePathPrefix(value string) DatabaseOptions

	// GetFilePathPrefix returns the file path prefix for sharded TSDB files.
	GetFilePathPrefix() string

	// NewWriter sets the function for creating a new writer.
	NewWriterFn(value fs.NewWriterFn) DatabaseOptions

	// GetNewWriterFn returns the function for creating a new writer.
	GetNewWriterFn() fs.NewWriterFn
}
