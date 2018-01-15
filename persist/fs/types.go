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
	"github.com/m3db/m3db/persist/fs/msgpack"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

// FileSetWriter provides an unsynchronized writer for a TSDB file set
type FileSetWriter interface {
	io.Closer

	// Open opens the files for writing data to the given shard in the given namespace
	Open(namespace ts.ID, blockSize time.Duration, shard uint32, start time.Time) error

	// Write will write the id and data pair and returns an error on a write error
	Write(id ts.ID, data checked.Bytes, checksum uint32) error

	// WriteAll will write the id and all byte slices and returns an error on a write error
	WriteAll(id ts.ID, data []checked.Bytes, checksum uint32) error
}

// FileSetReaderStatus describes the status of a file set reader
type FileSetReaderStatus struct {
	Open       bool
	Namespace  ts.ID
	Shard      uint32
	BlockStart time.Time
}

// FileSetReader provides an unsynchronized reader for a TSDB file set
type FileSetReader interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(namespace ts.ID, shard uint32, start time.Time) error

	// Status returns the status of the reader
	Status() FileSetReaderStatus

	// Read returns the next id, data, checksum tuple or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	Read() (id ts.ID, data checked.Bytes, checksum uint32, err error)

	// ReadMetadata returns the next id and metadata or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	ReadMetadata() (id ts.ID, length int, checksum uint32, err error)

	// ReadBloomFilter returns the bloom filter stored on disk in a container object that is safe
	// for concurrent use and has a Close() method for releasing resources when done.
	ReadBloomFilter() (*ManagedConcurrentBloomFilter, error)

	// Validate validates both the metadata and data and returns an error if either is corrupted
	Validate() error

	// ValidateMetadata validates the data and returns an error if the data is corrupted
	ValidateMetadata() error

	// ValidateData validates the data and returns an error if the data is corrupted
	ValidateData() error

	// Range returns the time range associated with data in the volume
	Range() xtime.Range

	// Entries returns the count of entries in the volume
	Entries() int

	// EntriesRead returns the position read into the volume
	EntriesRead() int

	// MetadataRead returns the position of metadata read into the volume
	MetadataRead() int
}

// FileSetSeeker provides an out of order reader for a TSDB file set
type FileSetSeeker interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(namespace ts.ID, shard uint32, start time.Time) error

	// SeekByID returns the data for specified ID provided the index was loaded upon open. An
	// error will be returned if the index was not loaded or ID cannot be found.
	SeekByID(id ts.ID) (data checked.Bytes, err error)

	// SeekByIndexEntry is similar to Seek, but uses an IndexEntry instead of
	// looking it up on its own. Useful in cases where you've already obtained an
	// entry and don't want to waste resources looking it up again.
	SeekByIndexEntry(entry IndexEntry) (checked.Bytes, error)

	// SeekIndexEntry returns the IndexEntry for the specified ID. This can be useful
	// ahead of issuing a number of seek requests so that the seek requests can be
	// made in order. The returned IndexEntry can also be passed to SeekUsingIndexEntry
	// to prevent duplicate index lookups.
	SeekIndexEntry(id ts.ID) (IndexEntry, error)

	// Range returns the time range associated with data in the volume
	Range() xtime.Range

	// Entries returns the count of entries in the volume
	Entries() int

	// ConcurrentIDBloomFilter returns a concurrency-safe bloom filter that can
	// be used to quickly disqualify ID's that definitely do not exist. I.E if the
	// Test() method returns true, the ID may exist on disk, but if it returns
	// false, it definitely does not.
	ConcurrentIDBloomFilter() *ManagedConcurrentBloomFilter

	// ConcurrentClone clones a seeker, creating a copy that uses the same underlying resources
	// (mmaps), but that is capable of seeking independently. The original can continue
	// to be used after the clones are closed, but the clones cannot be used after the
	// original is closed.
	ConcurrentClone() (ConcurrentFileSetSeeker, error)
}

// ConcurrentFileSetSeeker is a limited interface that is returned when ConcurrentClone() is called on FileSetSeeker.
// The clones can be used together concurrently and share underlying resources. Clones are no
// longer usable once the original has been closed.
type ConcurrentFileSetSeeker interface {
	io.Closer

	// SeekByID is the same as in FileSetSeeker
	SeekByID(id ts.ID) (data checked.Bytes, err error)

	// SeekByIndexEntry is the same as in FileSetSeeker
	SeekByIndexEntry(entry IndexEntry) (checked.Bytes, error)

	// SeekIndexEntry is the same as in FileSetSeeker
	SeekIndexEntry(id ts.ID) (IndexEntry, error)

	// ConcurrentIDBloomFilter is the same as in FileSetSeeker
	ConcurrentIDBloomFilter() *ManagedConcurrentBloomFilter
}

// FileSetSeekerManager provides management of seekers for a TSDB namespace.
type FileSetSeekerManager interface {
	io.Closer

	// Open opens the seekers for a given namespace.
	Open(md namespace.Metadata) error

	// CacheShardIndices will pre-parse the indexes for given shards
	// to improve times when seeking to a block.
	CacheShardIndices(shards []uint32) error

	// Borrow returns an open seeker for a given shard and block start time.
	Borrow(shard uint32, start time.Time) (ConcurrentFileSetSeeker, error)

	// Return returns an open seeker for a given shard and block start time.
	Return(shard uint32, start time.Time, seeker ConcurrentFileSetSeeker) error

	// ConcurrentIDBloomFilter returns a concurrent ID bloom filter for a given
	// shard and block start time
	ConcurrentIDBloomFilter(shard uint32, start time.Time) (*ManagedConcurrentBloomFilter, error)
}

// BlockRetriever provides a block retriever for TSDB file sets
type BlockRetriever interface {
	io.Closer
	block.DatabaseBlockRetriever

	// Open the block retriever to retrieve from a namespace
	Open(md namespace.Metadata) error
}

// RetrievableBlockSegmentReader is a retrievable block reader
type RetrievableBlockSegmentReader interface {
	xio.SegmentReader
}

// Options represents the options for filesystem persistence
type Options interface {
	// Validate will validate the options and return an error if not valid
	Validate() error

	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetRuntimeOptionsManager sets the runtime options manager
	SetRuntimeOptionsManager(value runtime.OptionsManager) Options

	// RuntimeOptionsManager returns the runtime options manager
	RuntimeOptionsManager() runtime.OptionsManager

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

	// SetIndexSummariesPercent size sets the percent of index summaries to write
	SetIndexSummariesPercent(value float64) Options

	// IndexSummariesPercent size returns the percent of index summaries to write
	IndexSummariesPercent() float64

	// SetIndexBloomFilterFalsePositivePercent size sets the percent of false positive
	// rate to use for the index bloom filter size and k hashes estimation
	SetIndexBloomFilterFalsePositivePercent(value float64) Options

	// IndexBloomFilterFalsePositivePercent size returns the percent of false positive
	// rate to use for the index bloom filter size and k hashes estimation
	IndexBloomFilterFalsePositivePercent() float64

	// SetWriterBufferSize sets the buffer size for writing TSDB files
	SetWriterBufferSize(value int) Options

	// WriterBufferSize returns the buffer size for writing TSDB files
	WriterBufferSize() int

	// SetInfoReaderBufferSize sets the buffer size for reading TSDB info, digest and checkpoint files
	SetInfoReaderBufferSize(value int) Options

	// InfoReaderBufferSize returns the buffer size for reading TSDB info, digest and checkpoint files
	InfoReaderBufferSize() int

	// SetDataReaderBufferSize sets the buffer size for reading TSDB data and index files
	SetDataReaderBufferSize(value int) Options

	// DataReaderBufferSize returns the buffer size for reading TSDB data and index files
	DataReaderBufferSize() int

	// SetSeekReaderBufferSize size sets the buffer size for seeking TSDB files
	SetSeekReaderBufferSize(value int) Options

	// SeekReaderBufferSize size returns the buffer size for seeking TSDB files
	SeekReaderBufferSize() int

	// SetMmapEnableHugeTLB sets whether mmap huge pages are enabled when running on linux
	SetMmapEnableHugeTLB(value bool) Options

	// MmapEnableHugeTLB returns whether mmap huge pages are enabled when running on linux
	MmapEnableHugeTLB() bool

	// SetMmapHugeTLBThreshold sets the threshold when to use mmap huge pages for mmap'd files on linux
	SetMmapHugeTLBThreshold(value int64) Options

	// MmapHugeTLBThreshold returns the threshold when to use mmap huge pages for mmap'd files on linux
	MmapHugeTLBThreshold() int64
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
