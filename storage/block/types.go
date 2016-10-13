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

// Metadata captures block metadata
type Metadata interface {
	// Start is the block start time
	Start() time.Time

	// Size is the block size
	Size() int64

	// Checksum is the block checksum if available
	Checksum() *uint32
}

// BlocksMetadata contains blocks metadata from a peer
type BlocksMetadata interface {
	// ID associated with the blocks
	ID() ts.ID

	// Blocks returns the metadata for blocks
	Blocks() []Metadata
}

// FetchBlockMetadataResult captures the block start time, the block size, and any errors encountered
type FetchBlockMetadataResult interface {
	// Start returns the start time of a block
	Start() time.Time

	// Size returns the size of the block, or nil if not available.
	Size() *int64

	// Checksum returns the checksum of the block, or nil if not available.
	Checksum() *uint32

	// Err returns the error encountered if any
	Err() error
}

// FetchBlocksMetadataResult captures the fetch results for multiple blocks.
type FetchBlocksMetadataResult interface {
	// ID returns id associated with the blocks
	ID() ts.ID

	// Blocks returns the metadata fetch result of blocks
	Blocks() []FetchBlockMetadataResult
}

// FilteredBlocksMetadataIter iterates over a list of blocks metadata results with filtering applied
type FilteredBlocksMetadataIter interface {
	// Next returns the next item if available
	Next() bool

	// Current returns the current id and block metadata
	Current() (ts.ID, Metadata)
}

// FetchBlockResult captures the block start time, the readers for the underlying streams, and any errors encountered.
type FetchBlockResult interface {
	// Start returns the start time of an encoded block
	Start() time.Time

	// Readers returns the readers for the underlying streams.
	Readers() []xio.SegmentReader

	// Err returns the error encountered when fetching the block.
	Err() error
}

// NewDatabaseBlockFn creates a new database block.
type NewDatabaseBlockFn func() DatabaseBlock

// DatabaseBlock represents a data block.
type DatabaseBlock interface {
	// IsSealed returns whether the block is sealed.
	IsSealed() bool

	// StartTime returns the start time of the block.
	StartTime() time.Time

	// Checksum returns the block checksum if available
	Checksum() *uint32

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

	// IsSealed returns whether all the blocks are sealed
	IsSealed() bool

	// MarkSealed marks the series as sealed
	MarkSealed()

	// AddBlock adds a data block.
	AddBlock(block DatabaseBlock)

	// AddSeries adds a raw series.
	AddSeries(other DatabaseSeriesBlocks)

	// MinTime returns the min time of the blocks contained.
	MinTime() time.Time

	// MaxTime returns the max time of the blocks contained.
	MaxTime() time.Time

	// BlockAt returns the block at a given time if any.
	BlockAt(t time.Time) (DatabaseBlock, bool)

	// BlockOrAdd returns the block at a given time, add it if it doesn't exist.
	BlockOrAdd(t time.Time) DatabaseBlock

	// AllBlocks returns all the blocks in the series.
	AllBlocks() map[time.Time]DatabaseBlock

	// RemoveBlockAt removes the block at a given time if any.
	RemoveBlockAt(t time.Time)

	// RemoveAll removes all blocks.
	RemoveAll()

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
	// SetDatabaseBlockAllocSize sets the databaseBlockAllocSize
	SetDatabaseBlockAllocSize(value int) Options

	// DatabaseBlockAllocSize returns the databaseBlockAllocSize
	DatabaseBlockAllocSize() int

	// SetDatabaseBlockPool sets the databaseBlockPool
	SetDatabaseBlockPool(value DatabaseBlockPool) Options

	// DatabaseBlockPool returns the databaseBlockPool
	DatabaseBlockPool() DatabaseBlockPool

	// SetContextPool sets the contextPool
	SetContextPool(value context.Pool) Options

	// ContextPool returns the contextPool
	ContextPool() context.Pool

	// SetEncoderPool sets the contextPool
	SetEncoderPool(value encoding.EncoderPool) Options

	// EncoderPool returns the contextPool
	EncoderPool() encoding.EncoderPool

	// SetReaderIteratorPool sets the readerIteratorPool
	SetReaderIteratorPool(value encoding.ReaderIteratorPool) Options

	// ReaderIteratorPool returns the readerIteratorPool
	ReaderIteratorPool() encoding.ReaderIteratorPool

	// SetMultiReaderIteratorPool sets the multiReaderIteratorPool
	SetMultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options

	// MultiReaderIteratorPool returns the multiReaderIteratorPool
	MultiReaderIteratorPool() encoding.MultiReaderIteratorPool

	// SetSegmentReaderPool sets the contextPool
	SetSegmentReaderPool(value xio.SegmentReaderPool) Options

	// SegmentReaderPool returns the contextPool
	SegmentReaderPool() xio.SegmentReaderPool

	// SetBytesPool sets the bytesPool
	SetBytesPool(value pool.BytesPool) Options

	// BytesPool returns the bytesPool
	BytesPool() pool.BytesPool
}
