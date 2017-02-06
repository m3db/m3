// Copyright (c) 2016 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package fs

import (
	"io"
	"os"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"
)

// FileSetWriter provides an unsynchronized writer for a TSDB file set
type FileSetWriter interface {
	io.Closer

	// Open opens the files for writing data to the given shard in the given namespace
	Open(namespace ts.ID, shard uint32, start time.Time) error

	// Write will write the id and data pair and returns an error on a write error
	Write(id ts.ID, data checked.Bytes, checksum uint32) error

	// WriteAll will write the id and all byte slices and returns an error on a write error
	WriteAll(id ts.ID, data []checked.Bytes, checksum uint32) error
}

// FileSetReader provides an unsynchronized reader for a TSDB file set
type FileSetReader interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(namespace ts.ID, shard uint32, start time.Time) error

	// Read returns the next id, data, checksum tuple or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	Read() (id ts.ID, data checked.Bytes, checksum uint32, err error)

	// ReadMetadata returns the next id and metadata or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	ReadMetadata() (id ts.ID, length int, checksum uint32, err error)

	// Validate validates the data and returns an error if the data are corrupted
	Validate() error

	// Range returns the time range associated with data in the volume
	Range() xtime.Range

	// Entries returns the count of entries in the volume
	Entries() int

	// EntriesRead returns the position read into the volume
	EntriesRead() int
}

// FileSetSeeker provides an out of order reader for a TSDB file set
type FileSetSeeker interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(namespace ts.ID, shard uint32, start time.Time) error

	// Seek returns the data for specified ID provided the index was loaded upon open. An
	// error will be returned if the index was not loaded or ID cannot be found.
	Seek(id ts.ID) (data checked.Bytes, err error)

	// SeekOffset returns the offset for specified ID provided the index was loaded upon open. If
	// the index was not loaded or ID cannot be found the value -1 will be returned.
	// This can be helpful ahead of issuing a number of seek requests so that the seek
	// requests can be made in order.
	SeekOffset(id ts.ID) int

	// Range returns the time range associated with data in the volume
	Range() xtime.Range

	// Entries returns the count of entries in the volume
	Entries() int

	// IDs retrieves all the identifiers present in the file set
	IDs() []ts.ID
}

// FileSetSeekerManager provides management of seekers for a TSDB namespace.
type FileSetSeekerManager interface {
	io.Closer

	// Open opens the seekers for a given namespace.
	Open(namespace ts.ID) error

	// CacheShardIndices will pre-parse the indexes for given shards
	// to improve times when seeking to a block.
	CacheShardIndices(shards []uint32) error

	// Seeker returns an open seeker for a given shard and block start time.
	Seeker(shard uint32, start time.Time) (FileSetSeeker, error)
}

// BlockRetriever provides a block retriever for TSDB file sets
type BlockRetriever interface {
	io.Closer
	block.DatabaseBlockRetriever

	// Open the block retriever to retrieve from a namespace
	Open(namespace ts.ID) error
}

// RetrievableBlockSegmentReader is a retrievable block reader
type RetrievableBlockSegmentReader interface {
	xio.SegmentReader
}

// Options represents the options for filesystem persistence
type Options interface {
	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetRetentionOptions sets the retention options
	SetRetentionOptions(value retention.Options) Options

	// RetentionOptions returns the retention options
	RetentionOptions() retention.Options

	// SetRateLimitOptions sets the rate limit options
	SetRateLimitOptions(value ratelimit.Options) Options

	// RateLimitOptions returns the rate limit options
	RateLimitOptions() ratelimit.Options

	// SetDecodingOptions sets the decoding options
	SetDecodingOptions(value msgpack.DecodingOptions) Options

	// DecodingOptions returns the decoding options
	DecodingOptions() msgpack.DecodingOptions

	// SetFilePathPrefix sets the file path prefix for sharded TSDB files
	SetFilePathPrefix(value string) Options

	// FilePathPrefix returns the file path prefix for sharded TSDB files
	FilePathPrefix() string

	// SetNewFileMode sets the new file mode
	SetNewFileMode(value os.FileMode) Options

	// NewFileMode returns the new file mode
	NewFileMode() os.FileMode

	// SetNewDirectoryMode sets the new directory mode
	SetNewDirectoryMode(value os.FileMode) Options

	// NewDirectoryMode returns the new directory mode
	NewDirectoryMode() os.FileMode

	// SetWriterBufferSize sets the buffer size for writing TSDB files
	SetWriterBufferSize(value int) Options

	// WriterBufferSize returns the buffer size for writing TSDB files
	WriterBufferSize() int

	// SetReaderBufferSize sets the buffer size for reading TSDB files
	SetReaderBufferSize(value int) Options

	// ReaderBufferSize returns the buffer size for reading TSDB files
	ReaderBufferSize() int
}

// BlockRetrieverOptions represents the options for block retrieval
type BlockRetrieverOptions interface {
	// SetRequestPoolOptions sets the request pool options
	SetRequestPoolOptions(value pool.ObjectPoolOptions) BlockRetrieverOptions

	// RequestPoolOptions returns the request pool options
	RequestPoolOptions() pool.ObjectPoolOptions

	// SetBytesPool sets the bytes pool
	SetBytesPool(value pool.CheckedBytesPool) BlockRetrieverOptions

	// BytesPool returns the bytes pool
	BytesPool() pool.CheckedBytesPool

	// SetSegmentReaderPool sets the segment reader pool
	SetSegmentReaderPool(value xio.SegmentReaderPool) BlockRetrieverOptions

	// SegmentReaderPool returns the segment reader pool
	SegmentReaderPool() xio.SegmentReaderPool

	// SetFetchConcurrency sets the fetch concurrency
	SetFetchConcurrency(value int) BlockRetrieverOptions

	// FetchConcurrency returns the fetch concurrency
	FetchConcurrency() int
}
