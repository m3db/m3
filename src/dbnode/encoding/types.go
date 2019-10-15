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

package encoding

import (
	"io"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"
)

// StreamOptions is an options struct that can be passed to Encoder.Stream()
// to modify its behavior.
type StreamOptions struct {
	// Optional bytes into which the stream should be copied. If left
	// nil then the checked bytes pool on the encoder will be used instead.
	// TODO(rartoul): Actually use this field in the Stream API() in a subsequent
	// P.R.
	Bytes checked.Bytes
}

// Encoder is the generic interface for different types of encoders.
type Encoder interface {
	// SetSchema sets up the schema needed by schema-aware encoder to encode the stream.
	// SetSchema can be called multiple times between reset for mid-stream schema changes.
	SetSchema(descr namespace.SchemaDescr)

	// Encode encodes a datapoint and optionally an annotation.
	// Schema must be set prior to Encode for schema-aware encoder. A schema can be set
	// via Reset/DiscardReset/SetSchema.
	Encode(dp ts.Datapoint, unit xtime.Unit, annotation ts.Annotation) error

	// Stream is the streaming interface for reading encoded bytes in the encoder.
	// A boolean is returned indicating whether the returned xio.SegmentReader contains
	// any data (true) or is empty (false) to encourage callers to remember to handle
	// the special case where there is an empty stream.
	Stream(opts StreamOptions) (xio.SegmentReader, bool)

	// NumEncoded returns the number of encoded datapoints.
	NumEncoded() int

	// LastEncoded returns the last encoded datapoint, useful for
	// de-duplicating encoded values. If there are no previously encoded values
	// an error is returned.
	LastEncoded() (ts.Datapoint, error)

	// Len returns the length of the encoded stream as returned by a call to Stream().
	Len() int

	// Reset resets the start time of the encoder and the internal state.
	// Reset sets up the schema for schema-aware encoders such as proto encoders.
	Reset(t time.Time, capacity int, schema namespace.SchemaDescr)

	// Close closes the encoder and if pooled will return to the pool.
	Close()

	// Discard will take ownership of the encoder data and if pooled will return to the pool.
	Discard() ts.Segment

	// DiscardReset will take ownership of the encoder data and reset the encoder for use.
	// DiscardReset sets up the schema for schema-aware encoders such as proto encoders.
	DiscardReset(t time.Time, capacity int, schema namespace.SchemaDescr) ts.Segment
}

// NewEncoderFn creates a new encoder
type NewEncoderFn func(start time.Time, bytes []byte) Encoder

// Options represents different options for encoding time as well as markers.
type Options interface {
	// SetDefaultTimeUnit sets the default time unit for the encoder.
	SetDefaultTimeUnit(tu xtime.Unit) Options

	// DefaultTimeUnit returns the default time unit for the encoder.
	DefaultTimeUnit() xtime.Unit

	// SetTimeEncodingSchemes sets the time encoding schemes for different time units.
	SetTimeEncodingSchemes(value TimeEncodingSchemes) Options

	// TimeEncodingSchemes returns the time encoding schemes for different time units.
	TimeEncodingSchemes() TimeEncodingSchemes

	// SetMarkerEncodingScheme sets the marker encoding scheme.
	SetMarkerEncodingScheme(value MarkerEncodingScheme) Options

	// MarkerEncodingScheme returns the marker encoding scheme.
	MarkerEncodingScheme() MarkerEncodingScheme

	// SetEncoderPool sets the encoder pool.
	SetEncoderPool(value EncoderPool) Options

	// EncoderPool returns the encoder pool.
	EncoderPool() EncoderPool

	// SetReaderIteratorPool sets the ReaderIteratorPool.
	SetReaderIteratorPool(value ReaderIteratorPool) Options

	// ReaderIteratorPool returns the ReaderIteratorPool.
	ReaderIteratorPool() ReaderIteratorPool

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.CheckedBytesPool) Options

	// BytesPool returns the bytes pool.
	BytesPool() pool.CheckedBytesPool

	// SetSegmentReaderPool sets the segment reader pool.
	SetSegmentReaderPool(value xio.SegmentReaderPool) Options

	// SegmentReaderPool returns the segment reader pool.
	SegmentReaderPool() xio.SegmentReaderPool

	// SetByteFieldDictionaryLRUSize sets theByteFieldDictionaryLRUSize which controls
	// how many recently seen byte field values will be maintained in the compression
	// dictionaries LRU when compressing / decompressing byte fields in ProtoBuf messages.
	// Increasing this value can potentially lead to better compression at the cost of
	// using more memory for storing metadata when compressing / decompressing.
	SetByteFieldDictionaryLRUSize(value int) Options

	// ByteFieldDictionaryLRUSize returns the ByteFieldDictionaryLRUSize.
	ByteFieldDictionaryLRUSize() int

	// SetIStreamReaderSizeM3TSZ sets the istream bufio reader size for m3tsz encoding iteration.
	SetIStreamReaderSizeM3TSZ(value int) Options

	// IStreamReaderSizeM3TSZ returns the istream bufio reader size for m3tsz encoding iteration.
	IStreamReaderSizeM3TSZ() int

	// SetIStreamReaderSizeProto sets the istream bufio reader size for proto encoding iteration.
	SetIStreamReaderSizeProto(value int) Options

	// SetIStreamReaderSizeProto returns the istream bufio reader size for proto encoding iteration.
	IStreamReaderSizeProto() int
}

// Iterator is the generic interface for iterating over encoded data.
type Iterator interface {
	// Next moves to the next item
	Next() bool

	// Current returns the value as well as the annotation associated with the current datapoint.
	// Users should not hold on to the returned Annotation object as it may get invalidated when
	// the iterator calls Next().
	Current() (ts.Datapoint, xtime.Unit, ts.Annotation)

	// Err returns the error encountered
	Err() error

	// Close closes the iterator and if pooled will return to the pool.
	Close()
}

// ReaderIterator is the interface for a single-reader iterator.
type ReaderIterator interface {
	Iterator

	// Reset resets the iterator to read from a new reader with a new schema (for schema aware iterators).
	Reset(reader io.Reader, schema namespace.SchemaDescr)
}

// MultiReaderIterator is an iterator that iterates in order over a list of sets of
// internally ordered but not collectively in order readers, it also deduplicates datapoints.
type MultiReaderIterator interface {
	Iterator

	// Reset resets the iterator to read from a slice of readers with a new schema (for schema aware iterators).
	Reset(readers []xio.SegmentReader, start time.Time, blockSize time.Duration, schema namespace.SchemaDescr)

	// Reset resets the iterator to read from a slice of slice readers with a new schema (for schema aware iterators).
	ResetSliceOfSlices(readers xio.ReaderSliceOfSlicesIterator, schema namespace.SchemaDescr)

	// Readers exposes the underlying ReaderSliceOfSlicesIterator for this MultiReaderIterator
	Readers() xio.ReaderSliceOfSlicesIterator
}

// SeriesIterator is an iterator that iterates over a set of iterators from different replicas
// and de-dupes & merges results from the replicas for a given series while also applying a time
// filter on top of the values in case replicas returned values out of range on either end
type SeriesIterator interface {
	Iterator

	// ID gets the ID of the series
	ID() ident.ID

	// Namespace gets the namespace of the series
	Namespace() ident.ID

	// Tags returns an iterator over the tags associated with the ID.
	Tags() ident.TagIterator

	// Start returns the start time filter specified for the iterator
	Start() time.Time

	// End returns the end time filter specified for the iterator
	End() time.Time

	// Reset resets the iterator to read from a set of iterators from different replicas, one
	// must note that this can be an array with nil entries if some replicas did not return successfully.
	// NB: the SeriesIterator assumes ownership of the provided ids, this includes calling `id.Finalize()` upon
	// iter.Close().
	Reset(opts SeriesIteratorOptions)

	// SetIterateEqualTimestampStrategy sets the equal timestamp strategy of how
	// to select a value when the timestamp matches differing values with the same
	// timestamp from different replicas.
	// It can be set at any time and will apply to the current value returned
	// from the iterator immediately.
	SetIterateEqualTimestampStrategy(strategy IterateEqualTimestampStrategy)

	// Replicas exposes the underlying MultiReaderIterator slice for this SeriesIterator
	Replicas() []MultiReaderIterator
}

// SeriesIteratorOptions is a set of options for using a series iterator.
type SeriesIteratorOptions struct {
	ID                            ident.ID
	Namespace                     ident.ID
	Tags                          ident.TagIterator
	Replicas                      []MultiReaderIterator
	StartInclusive                time.Time
	EndExclusive                  time.Time
	IterateEqualTimestampStrategy IterateEqualTimestampStrategy
}

// SeriesIterators is a collection of SeriesIterator that can close all iterators
type SeriesIterators interface {
	// Iters returns the array of series iterators
	Iters() []SeriesIterator

	// Len returns the length of the iters
	Len() int

	// Close closes all iterators contained
	Close()
}

// MutableSeriesIterators is a mutable SeriesIterators
type MutableSeriesIterators interface {
	SeriesIterators

	// Reset the iters collection to a size for reuse
	Reset(size int)

	// Cap returns the capacity of the iters
	Cap() int

	// SetAt an index a SeriesIterator
	SetAt(idx int, iter SeriesIterator)
}

// Decoder is the generic interface for different types of decoders.
type Decoder interface {
	// Decode decodes the encoded data in the reader.
	Decode(reader io.Reader) ReaderIterator
}

// NewDecoderFn creates a new decoder
type NewDecoderFn func() Decoder

// EncoderAllocate allocates an encoder for a pool.
type EncoderAllocate func() Encoder

// ReaderIteratorAllocate allocates a ReaderIterator for a pool.
type ReaderIteratorAllocate func(reader io.Reader, descr namespace.SchemaDescr) ReaderIterator

// IStream encapsulates a readable stream.
type IStream interface {
	Read([]byte) (int, error)
	ReadBit() (Bit, error)
	ReadByte() (byte, error)
	ReadBits(numBits int) (uint64, error)
	PeekBits(numBits int) (uint64, error)
	RemainingBitsInCurrentByte() int
	Reset(r io.Reader)
}

// OStream encapsulates a writable stream.
type OStream interface {
	Len() int
	Empty() bool
	WriteBit(v Bit)
	WriteBits(v uint64, numBits int)
	WriteByte(v byte)
	WriteBytes(bytes []byte)
	Write(bytes []byte) (int, error)
	Reset(buffer checked.Bytes)
	Discard() checked.Bytes
	Rawbytes() ([]byte, int)
}

// EncoderPool provides a pool for encoders
type EncoderPool interface {
	// Init initializes the pool.
	Init(alloc EncoderAllocate)

	// Get provides an encoder from the pool
	Get() Encoder

	// Put returns an encoder to the pool
	Put(e Encoder)
}

// ReaderIteratorPool provides a pool for ReaderIterators
type ReaderIteratorPool interface {
	// Init initializes the pool.
	Init(alloc ReaderIteratorAllocate)

	// Get provides a ReaderIterator from the pool
	Get() ReaderIterator

	// Put returns a ReaderIterator to the pool
	Put(iter ReaderIterator)
}

// MultiReaderIteratorPool provides a pool for MultiReaderIterators
type MultiReaderIteratorPool interface {
	// Init initializes the pool.
	Init(alloc ReaderIteratorAllocate)

	// Get provides a MultiReaderIterator from the pool
	Get() MultiReaderIterator

	// Put returns a MultiReaderIterator to the pool
	Put(iter MultiReaderIterator)
}

// SeriesIteratorPool provides a pool for SeriesIterator
type SeriesIteratorPool interface {
	// Init initializes the pool
	Init()

	// Get provides a SeriesIterator from the pool
	Get() SeriesIterator

	// Put returns a SeriesIterator to the pool
	Put(iter SeriesIterator)
}

// MutableSeriesIteratorsPool provides a pool for MutableSeriesIterators
type MutableSeriesIteratorsPool interface {
	// Init initializes the pool
	Init()

	// Get provides a MutableSeriesIterators from the pool
	Get(size int) MutableSeriesIterators

	// Put returns a MutableSeriesIterators to the pool
	Put(iters MutableSeriesIterators)
}

// MultiReaderIteratorArrayPool provides a pool for MultiReaderIterator arrays
type MultiReaderIteratorArrayPool interface {
	// Init initializes the pool
	Init()

	// Get provides a Iterator array from the pool
	Get(size int) []MultiReaderIterator

	// Put returns a Iterator array to the pool
	Put(iters []MultiReaderIterator)
}

// IteratorPools exposes a small subset of iterator pools that are sufficient for clients
// to rebuild SeriesIterator
type IteratorPools interface {
	// MultiReaderIteratorArray exposes the session's MultiReaderIteratorArrayPool
	MultiReaderIteratorArray() MultiReaderIteratorArrayPool
	// MultiReaderIterator exposes the session's MultiReaderIteratorPool
	MultiReaderIterator() MultiReaderIteratorPool
	// MutableSeriesIterators exposes the session's MutableSeriesIteratorsPool
	MutableSeriesIterators() MutableSeriesIteratorsPool
	// SeriesIterator exposes the session's SeriesIteratorPool
	SeriesIterator() SeriesIteratorPool
	// CheckedBytesWrapper exposes the session's CheckedBytesWrapperPool
	CheckedBytesWrapper() xpool.CheckedBytesWrapperPool
	// ID exposes the session's identity pool
	ID() ident.Pool
	// TagEncoder exposes the session's tag encoder pool
	TagEncoder() serialize.TagEncoderPool
	// TagDecoder exposes the session's tag decoder pool
	TagDecoder() serialize.TagDecoderPool
}
