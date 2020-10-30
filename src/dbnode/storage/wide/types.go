// Copyright (c) 2020 Uber Technologies, Inc.
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

package wide

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// SeriesMetadata describes metadata for a series within a block.
type SeriesMetadata struct {
	// ID is block ID.
	ID ident.BytesID
	// EncodedTags are the tags for the series.
	EncodedTags ts.EncodedTags
}

// BorrowedBuffer is a pre-allocated buffer backed by a fixed buffer manager.
type BorrowedBuffer interface {
	// Finalize releases the bytes.
	Finalize()
}

// FixedBufferManager yields pre-alloced buffers of fixed size.
type FixedBufferManager interface {
	// Copy copies the source bytes into a fixed buffer, returning a copy backed
	// by this buffer manager, and a finalizer for these bytes.
	Copy(src []byte) ([]byte, BorrowedBuffer, error)
}

// ShardIteratorRecord is a record for a series for a shard in a wide query.
type ShardIteratorRecord struct {
	ID               []byte
	EncodedTags      []byte
	MetadataChecksum int64
	Data             []byte
}

// wideIter is the base type for wide iterators.
type wideIter interface {
	// BlockStart is the start of this iterator's block.
	BlockStart() time.Time
	// Next moves the iterator to the next element.
	Next() bool
	// Err returns any errors encountered.
	Err() error
	// Close closes the iterator.
	Close()
}

// QueryIterator is a wide query iterator.
type QueryIterator interface {
	wideIter

	// Shards are the shards this iterator is responsible for.
	Shards() []uint32
	// Current is the current shard iterator, invalidated on calls to Next().
	Current() QueryShardIterator
	// ShardIter is used to write into the given shard.
	ShardIter(shard uint32) (QueryShardIterator, error)
	// SetDoneError indicates no more writes, optionally taking writer side errors.
	SetDoneError(error)
}

// QueryShardIterator is a wide query shard iterator.
type QueryShardIterator interface {
	wideIter

	// PushRecord pushes a recoerd into this iterator.
	PushRecord(record ShardIteratorRecord) error

	// Shard is the shard for this series.
	Shard() uint32
	// Current is the current series iterator.
	// This is invalidated on calls to Next().
	Current() QuerySeriesIterator
}

// QuerySeriesIterator is a wide query series iterator.
type QuerySeriesIterator interface {
	encoding.Iterator

	// SeriesMetadata contains metadata for this series.
	SeriesMetadata() SeriesMetadata
}

// CrossShardIterator iterates across series across all shards in a block record.
type CrossShardIterator interface {
	// Next moves to the next series.
	Next() bool

	// Current is the current series iterator, invalidated on calls to Next().
	Current() QuerySeriesIterator

	// Err returns the error encountered.
	Err() error

	// Close closes the iterator and if pooled will return to the pool.
	Close()
}

// CrossBlockIterator iterates across series in a block record.
type CrossBlockIterator interface {
	// Next moves to the next series.
	Next() bool

	// Current is the current series iterator, invalidated on calls to Next().
	Current() QuerySeriesIterator

	// Err returns the error encountered.
	Err() error

	// Close closes the iterator and if pooled will return to the pool.
	Close()
}

// CrossBlockSeries is a cross-block series.
type CrossBlockSeries struct {
	ID          ident.ID
	EncodedTags ts.EncodedTags
	Datapoints  []ts.Datapoint
	Unit        xtime.Unit
	Annotation  ts.Annotation
}

// Options are options for cross block iteration.
type Options interface {
	// Validate validates the options.
	Validate() error

	// SetFixedBufferCount sets the fixed buffer count.
	SetFixedBufferCount(value int) Options

	// FixedBufferCount returns the fixed buffer count.
	FixedBufferCount() int

	// SetFixedBufferCapacity sets the fixed buffer capacity.
	SetFixedBufferCapacity(value int) Options

	// FixedBufferCapacity returns the fixed buffer capacity.
	FixedBufferCapacity() int

	// SetFixedBufferTimeout sets the fixed buffer timeout, 0 implies no timeout.
	SetFixedBufferTimeout(value time.Duration) Options

	// FixedBufferTimeout returns the fixed buffer timeout, 0 implies no timeout.
	FixedBufferTimeout() time.Duration

	// SetReaderIteratorPool sets the reader iterator pool.
	SetReaderIteratorPool(value encoding.ReaderIteratorPool) Options

	// ReaderIteratorPool returns the reader iterator pool.
	ReaderIteratorPool() encoding.ReaderIteratorPool

	// SetSchemaDescr sets the schema description.
	SetSchemaDescr(value namespace.SchemaDescr) Options

	// SetSchemaDescr returns the schema description.
	SchemaDescr() namespace.SchemaDescr
}
