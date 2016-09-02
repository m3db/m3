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

package block

import (
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xtime "github.com/m3db/m3x/time"
)

// NewDatabaseBlockFn creates a new database block.
type NewDatabaseBlockFn func() DatabaseBlock

// DatabaseBlock represents a data block.
type DatabaseBlock interface {
	// StartTime returns the start time of the block.
	StartTime() time.Time

	// IsSealed returns whether the block is sealed.
	IsSealed() bool

	// Write writes a datapoint to the block along with time unit and annotation.
	Write(timestamp time.Time, value float64, unit xtime.Unit, annotation ts.Annotation) error

	// Stream returns the encoded byte stream.
	Stream(blocker context.Context) (xio.SegmentReader, error)

	// Reset resets the block start time and the encoder.
	Reset(startTime time.Time, encoder encoding.Encoder)

	// Close closes the block.
	Close()

	// Seal seals the block.
	Seal()
}

// DatabaseSeriesBlocks represents a collection of data blocks.
type DatabaseSeriesBlocks interface {
	// Options returns the blocks options.
	Options() Options

	// Len returns the number of blocks contained in the collection.
	Len() int

	// AddBlock adds a data block.
	AddBlock(block DatabaseBlock)

	// AddSeries adds a raw series.
	AddSeries(other DatabaseSeriesBlocks)

	// GetMinTime returns the min time of the blocks contained.
	GetMinTime() time.Time

	// GetMaxTime returns the max time of the blocks contained.
	GetMaxTime() time.Time

	// GetBlockAt returns the block at a given time if any.
	GetBlockAt(t time.Time) (DatabaseBlock, bool)

	// GetBlockAt returns the block at a given time, add it if it doesn't exist.
	GetBlockOrAdd(t time.Time) DatabaseBlock

	// AllBlocks returns all the blocks in the series.
	AllBlocks() map[time.Time]DatabaseBlock

	// RemoveBlockAt removes the block at a given time if any.
	RemoveBlockAt(t time.Time)

	// Close closes all the blocks.
	Close()
}

// DatabaseBlockAllocate allocates a database block for a pool.
type DatabaseBlockAllocate func() DatabaseBlock

// DatabaseBlockPool provides a pool for database blocks.
type DatabaseBlockPool interface {
	// Init initializes the pool.
	Init(alloc DatabaseBlockAllocate)

	// Get provides a database block from the pool.
	Get() DatabaseBlock

	// Put returns a database block to the pool.
	Put(block DatabaseBlock)
}

// Options represents the options for a database block
type Options interface {
	// DatabaseBlockAllocSize sets the databaseBlockAllocSize
	DatabaseBlockAllocSize(value int) Options

	// GetDatabaseBlockAllocSize returns the databaseBlockAllocSize
	GetDatabaseBlockAllocSize() int

	// DatabaseBlockPool sets the databaseBlockPool
	DatabaseBlockPool(value DatabaseBlockPool) Options

	// GetDatabaseBlockPool returns the databaseBlockPool
	GetDatabaseBlockPool() DatabaseBlockPool

	// ContextPool sets the contextPool
	ContextPool(value context.Pool) Options

	// GetContextPool returns the contextPool
	GetContextPool() context.Pool

	// EncoderPool sets the contextPool
	EncoderPool(value encoding.EncoderPool) Options

	// GetEncoderPool returns the contextPool
	GetEncoderPool() encoding.EncoderPool

	// ReaderIteratorPool sets the readerIteratorPool
	ReaderIteratorPool(value encoding.ReaderIteratorPool) Options

	// GetReaderIteratorPool returns the readerIteratorPool
	GetReaderIteratorPool() encoding.ReaderIteratorPool

	// MultiReaderIteratorPool sets the multiReaderIteratorPool
	MultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options

	// GetMultiReaderIteratorPool returns the multiReaderIteratorPool
	GetMultiReaderIteratorPool() encoding.MultiReaderIteratorPool

	// SegmentReaderPool sets the contextPool
	SegmentReaderPool(value xio.SegmentReaderPool) Options

	// GetSegmentReaderPool returns the contextPool
	GetSegmentReaderPool() xio.SegmentReaderPool

	// BytesPool sets the bytesPool
	BytesPool(value pool.BytesPool) Options

	// GetBytesPool returns the bytesPool
	GetBytesPool() pool.BytesPool
}
