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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

// Metadata captures block metadata
type Metadata struct {
	ID       ident.ID
	Tags     ident.Tags
	Start    time.Time
	Size     int64
	Checksum *uint32
	LastRead time.Time
}

// ReplicaMetadata captures block metadata along with corresponding peer identifier
// for a single replica of a block
type ReplicaMetadata struct {
	Metadata

	Host topology.Host
}

// FilteredBlocksMetadataIter iterates over a list of blocks metadata results with filtering applied
type FilteredBlocksMetadataIter interface {
	// Next returns the next item if available
	Next() bool

	// Current returns the current id and block metadata
	Current() (ident.ID, Metadata)

	//  Error returns an error if encountered
	Err() error
}

// FetchBlockResult captures the block start time, the readers for the underlying streams, the
// corresponding checksum and any errors encountered.
type FetchBlockResult struct {
	Start  time.Time
	Blocks []xio.BlockReader
	Err    error
}

// FetchBlocksMetadataOptions are options used when fetching blocks metadata.
type FetchBlocksMetadataOptions struct {
	IncludeSizes     bool
	IncludeChecksums bool
	IncludeLastRead  bool
}

// FetchBlockMetadataResult captures the block start time, the block size, and any errors encountered
type FetchBlockMetadataResult struct {
	Start    time.Time
	Size     int64
	Checksum *uint32
	LastRead time.Time
	Err      error
}

// FetchBlockMetadataResults captures a collection of FetchBlockMetadataResult
type FetchBlockMetadataResults interface {
	// Add adds a result to the slice
	Add(res FetchBlockMetadataResult)

	// Results returns the result slice
	Results() []FetchBlockMetadataResult

	// SortByTimeAscending sorts the results in time ascending order
	Sort()

	// Reset resets the results
	Reset()

	// Close performs cleanup
	Close()
}

// FetchBlocksMetadataResult captures the fetch results for multiple blocks.
type FetchBlocksMetadataResult struct {
	ID     ident.ID
	Tags   ident.TagIterator
	Blocks FetchBlockMetadataResults
}

// FetchBlocksMetadataResults captures a collection of FetchBlocksMetadataResult
type FetchBlocksMetadataResults interface {
	// Add adds a result to the slice
	Add(res FetchBlocksMetadataResult)

	// Results returns the result slice
	Results() []FetchBlocksMetadataResult

	// Reset resets the results
	Reset()

	// Close performs cleanup
	Close()
}

// NewDatabaseBlockFn creates a new database block.
type NewDatabaseBlockFn func() DatabaseBlock

// DatabaseBlock is the interface for a DatabaseBlock
type DatabaseBlock interface {
	// StartTime returns the start time of the block.
	StartTime() time.Time

	// BlockSize returns the duration of the block.
	BlockSize() time.Duration

	// SetLastReadTime sets the last read time of the block.
	SetLastReadTime(value time.Time)

	// LastReadTime returns the last read time of the block.
	LastReadTime() time.Time

	// Len returns the block length.
	Len() int

	// Checksum returns the block checksum.
	Checksum() (uint32, error)

	// Stream returns the encoded byte stream.
	Stream(blocker context.Context) (xio.BlockReader, error)

	// Merge will merge the current block with the specified block
	// when this block is read. Note: calling this twice
	// will simply overwrite the target for the block to merge with
	// rather than merging three blocks together.
	Merge(other DatabaseBlock) error

	// HasMergeTarget returns whether the block requires multiple blocks to be
	// merged during Stream().
	HasMergeTarget() bool

	// Reset resets the block start time, duration, and the segment.
	Reset(startTime time.Time, blockSize time.Duration, segment ts.Segment)

	// ResetFromDisk resets the block start time, duration, segment, and id.
	ResetFromDisk(
		startTime time.Time,
		blockSize time.Duration,
		segment ts.Segment,
		id ident.ID,
	)

	// Discard closes the block, but returns the (unfinalized) segment.
	Discard() ts.Segment

	// Close closes the block.
	Close()

	// SetOnEvictedFromWiredList sets the owner of the block
	SetOnEvictedFromWiredList(OnEvictedFromWiredList)

	// OnEvictedFromWiredList returns the owner of the block
	OnEvictedFromWiredList() OnEvictedFromWiredList

	// Private methods because only the Wired List itself should use them.
	databaseBlock
}

// databaseBlock is the private portion of the DatabaseBlock interface
type databaseBlock interface {
	next() DatabaseBlock
	setNext(block DatabaseBlock)
	prev() DatabaseBlock
	setPrev(block DatabaseBlock)
	enteredListAtUnixNano() int64
	setEnteredListAtUnixNano(value int64)
	wiredListEntry() wiredListEntry
}

// OnEvictedFromWiredList is implemented by a struct that wants to be notified
// when a block is evicted from the wired list.
type OnEvictedFromWiredList interface {
	// OnEvictedFromWiredList is called when a block is evicted from the wired list.
	OnEvictedFromWiredList(id ident.ID, blockStart time.Time)
}

// OnRetrieveBlock is an interface to callback on when a block is retrieved.
type OnRetrieveBlock interface {
	OnRetrieveBlock(
		id ident.ID,
		tags ident.TagIterator,
		startTime time.Time,
		segment ts.Segment,
	)
}

// OnReadBlock is an interface to callback on when a block is read.
type OnReadBlock interface {
	OnReadBlock(b DatabaseBlock)
}

// OnRetrieveBlockFn is a function implementation for the
// OnRetrieveBlock interface.
type OnRetrieveBlockFn func(
	id ident.ID,
	startTime time.Time,
	segment ts.Segment,
)

// OnRetrieveBlock implements the OnRetrieveBlock interface.
func (fn OnRetrieveBlockFn) OnRetrieveBlock(
	id ident.ID,
	startTime time.Time,
	segment ts.Segment,
) {
	fn(id, startTime, segment)
}

// RetrievableBlockMetadata describes a retrievable block.
type RetrievableBlockMetadata struct {
	ID       ident.ID
	Length   int
	Checksum uint32
}

// DatabaseBlockRetriever is a block retriever.
type DatabaseBlockRetriever interface {
	// CacheShardIndices will pre-parse the indexes for given shards
	// to improve times when streaming a block.
	CacheShardIndices(shards []uint32) error

	// Stream will stream a block for a given shard, id and start.
	Stream(
		ctx context.Context,
		shard uint32,
		id ident.ID,
		blockStart time.Time,
		onRetrieve OnRetrieveBlock,
	) (xio.BlockReader, error)
}

// DatabaseShardBlockRetriever is a block retriever bound to a shard.
type DatabaseShardBlockRetriever interface {
	// Stream will stream a block for a given id and start.
	Stream(
		ctx context.Context,
		id ident.ID,
		blockStart time.Time,
		onRetrieve OnRetrieveBlock,
	) (xio.BlockReader, error)
}

// DatabaseBlockRetrieverManager creates and holds block retrievers
// for different namespaces.
type DatabaseBlockRetrieverManager interface {
	Retriever(nsMetadata namespace.Metadata) (DatabaseBlockRetriever, error)
}

// DatabaseShardBlockRetrieverManager creates and holds shard block
// retrievers binding shards to an existing retriever.
type DatabaseShardBlockRetrieverManager interface {
	ShardRetriever(shard uint32) DatabaseShardBlockRetriever
}

// DatabaseSeriesBlocks represents a collection of data blocks.
type DatabaseSeriesBlocks interface {
	// Len returns the number of blocks contained in the collection.
	Len() int

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

	// AllBlocks returns all the blocks in the series.
	AllBlocks() map[xtime.UnixNano]DatabaseBlock

	// RemoveBlockAt removes the block at a given time if any.
	RemoveBlockAt(t time.Time)

	// RemoveAll removes all blocks.
	RemoveAll()

	// Reset resets the DatabaseSeriesBlocks so they can be re-used
	Reset()

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

// FetchBlockMetadataResultsPool provides a pool for fetchBlockMetadataResults
type FetchBlockMetadataResultsPool interface {
	// Get returns an FetchBlockMetadataResults
	Get() FetchBlockMetadataResults

	// Put puts an FetchBlockMetadataResults back to pool
	Put(res FetchBlockMetadataResults)
}

// FetchBlocksMetadataResultsPool provides a pool for fetchBlocksMetadataResults
type FetchBlocksMetadataResultsPool interface {
	// Get returns an fetchBlocksMetadataResults
	Get() FetchBlocksMetadataResults

	// Put puts an fetchBlocksMetadataResults back to pool
	Put(res FetchBlocksMetadataResults)
}

// Options represents the options for a database block
type Options interface {
	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetDatabaseBlockAllocSize sets the databaseBlockAllocSize
	SetDatabaseBlockAllocSize(value int) Options

	// DatabaseBlockAllocSize returns the databaseBlockAllocSize
	DatabaseBlockAllocSize() int

	// SetCloseContextWorkers sets the workers for closing contexts
	SetCloseContextWorkers(value xsync.WorkerPool) Options

	// CloseContextWorkers returns the workers for closing contexts
	CloseContextWorkers() xsync.WorkerPool

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
	SetBytesPool(value pool.CheckedBytesPool) Options

	// BytesPool returns the bytesPool
	BytesPool() pool.CheckedBytesPool

	// SetWiredList sets the database block wired list
	SetWiredList(value *WiredList) Options

	// WiredList returns the database block wired list
	WiredList() *WiredList
}
