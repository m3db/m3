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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

// FileSetFileIdentifier contains all the information required to identify a FileSetFile
type FileSetFileIdentifier struct {
	FileSetContentType persist.FileSetContentType
	Namespace          ident.ID
	BlockStart         time.Time
	// Only required for data content files
	Shard uint32
	// Required for snapshot files (index yes, data yes) and flush files (index yes, data no)
	VolumeIndex int
}

// DataWriterOpenOptions is the options struct for the Open method on the DataFileSetWriter
type DataWriterOpenOptions struct {
	FileSetType        persist.FileSetType
	FileSetContentType persist.FileSetContentType
	Identifier         FileSetFileIdentifier
	BlockSize          time.Duration
	// Only used when writing snapshot files
	Snapshot DataWriterSnapshotOptions
}

// DataWriterSnapshotOptions is the options struct for Open method on the DataFileSetWriter
// that contains information specific to writing snapshot files
type DataWriterSnapshotOptions struct {
	SnapshotTime time.Time
	SnapshotID   []byte
}

// DataFileSetWriter provides an unsynchronized writer for a TSDB file set
type DataFileSetWriter interface {
	io.Closer

	// Open opens the files for writing data to the given shard in the given namespace.
	// This method is not thread-safe, so its the callers responsibilities that they never
	// try and write two snapshot files for the same block start at the same time or their
	// will be a race in determining the snapshot file's index.
	Open(opts DataWriterOpenOptions) error

	// Write will write the id and data pair and returns an error on a write error. Callers
	// must not call this method with a given ID more than once.
	Write(id ident.ID, tags ident.Tags, data checked.Bytes, checksum uint32) error

	// WriteAll will write the id and all byte slices and returns an error on a write error.
	// Callers must not call this method with a given ID more than once.
	WriteAll(id ident.ID, tags ident.Tags, data []checked.Bytes, checksum uint32) error
}

// DataFileSetReaderStatus describes the status of a file set reader
type DataFileSetReaderStatus struct {
	Namespace  ident.ID
	BlockStart time.Time

	Shard uint32
	Open  bool
}

// DataReaderOpenOptions is options struct for the reader open method.
type DataReaderOpenOptions struct {
	Identifier  FileSetFileIdentifier
	FileSetType persist.FileSetType
}

// DataFileSetReader provides an unsynchronized reader for a TSDB file set
type DataFileSetReader interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(opts DataReaderOpenOptions) error

	// Status returns the status of the reader
	Status() DataFileSetReaderStatus

	// Read returns the next id, data, checksum tuple or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	// Note: make sure to finalize the ID, close the Tags and finalize the Data when done with
	// them so they can be returned to their respective pools.
	Read() (id ident.ID, tags ident.TagIterator, data checked.Bytes, checksum uint32, err error)

	// ReadMetadata returns the next id and metadata or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	// Note: make sure to finalize the ID, and close the Tags when done with them so they can
	// be returned to their respective pools.
	ReadMetadata() (id ident.ID, tags ident.TagIterator, length int, checksum uint32, err error)

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

// DataFileSetSeeker provides an out of order reader for a TSDB file set
type DataFileSetSeeker interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(namespace ident.ID, shard uint32, start time.Time) error

	// SeekByID returns the data for specified ID provided the index was loaded upon open. An
	// error will be returned if the index was not loaded or ID cannot be found.
	SeekByID(id ident.ID) (data checked.Bytes, err error)

	// SeekByIndexEntry is similar to Seek, but uses an IndexEntry instead of
	// looking it up on its own. Useful in cases where you've already obtained an
	// entry and don't want to waste resources looking it up again.
	SeekByIndexEntry(entry IndexEntry) (checked.Bytes, error)

	// SeekIndexEntry returns the IndexEntry for the specified ID. This can be useful
	// ahead of issuing a number of seek requests so that the seek requests can be
	// made in order. The returned IndexEntry can also be passed to SeekUsingIndexEntry
	// to prevent duplicate index lookups.
	SeekIndexEntry(id ident.ID) (IndexEntry, error)

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
	ConcurrentClone() (ConcurrentDataFileSetSeeker, error)
}

// ConcurrentDataFileSetSeeker is a limited interface that is returned when ConcurrentClone() is called on DataFileSetSeeker.
// The clones can be used together concurrently and share underlying resources. Clones are no
// longer usable once the original has been closed.
type ConcurrentDataFileSetSeeker interface {
	io.Closer

	// SeekByID is the same as in DataFileSetSeeker
	SeekByID(id ident.ID) (data checked.Bytes, err error)

	// SeekByIndexEntry is the same as in DataFileSetSeeker
	SeekByIndexEntry(entry IndexEntry) (checked.Bytes, error)

	// SeekIndexEntry is the same as in DataFileSetSeeker
	SeekIndexEntry(id ident.ID) (IndexEntry, error)

	// ConcurrentIDBloomFilter is the same as in DataFileSetSeeker
	ConcurrentIDBloomFilter() *ManagedConcurrentBloomFilter
}

// DataFileSetSeekerManager provides management of seekers for a TSDB namespace.
type DataFileSetSeekerManager interface {
	io.Closer

	// Open opens the seekers for a given namespace.
	Open(md namespace.Metadata) error

	// CacheShardIndices will pre-parse the indexes for given shards
	// to improve times when seeking to a block.
	CacheShardIndices(shards []uint32) error

	// Borrow returns an open seeker for a given shard and block start time.
	Borrow(shard uint32, start time.Time) (ConcurrentDataFileSetSeeker, error)

	// Return returns an open seeker for a given shard and block start time.
	Return(shard uint32, start time.Time, seeker ConcurrentDataFileSetSeeker) error

	// ConcurrentIDBloomFilter returns a concurrent ID bloom filter for a given
	// shard and block start time
	ConcurrentIDBloomFilter(shard uint32, start time.Time) (*ManagedConcurrentBloomFilter, error)
}

// DataBlockRetriever provides a block retriever for TSDB file sets
type DataBlockRetriever interface {
	io.Closer
	block.DatabaseBlockRetriever

	// Open the block retriever to retrieve from a namespace
	Open(md namespace.Metadata) error
}

// RetrievableDataBlockSegmentReader is a retrievable block reader
type RetrievableDataBlockSegmentReader interface {
	xio.SegmentReader
}

// IndexWriterSnapshotOptions is a set of options for writing an index file set snapshot.
type IndexWriterSnapshotOptions struct {
	SnapshotTime time.Time
}

// IndexWriterOpenOptions is a set of options when opening an index file set writer.
type IndexWriterOpenOptions struct {
	Identifier  FileSetFileIdentifier
	BlockSize   time.Duration
	FileSetType persist.FileSetType
	Shards      map[uint32]struct{}
	// Only used when writing snapshot files
	Snapshot IndexWriterSnapshotOptions
}

// IndexFileSetWriter is a index file set writer.
type IndexFileSetWriter interface {
	idxpersist.IndexFileSetWriter
	io.Closer

	// Open the index file set writer.
	Open(opts IndexWriterOpenOptions) error
}

// IndexSegmentFileSetWriter is an index segment file set writer.
type IndexSegmentFileSetWriter interface {
	idxpersist.IndexSegmentFileSetWriter
}

// IndexSegmentFileSet is an index segment file set.
type IndexSegmentFileSet interface {
	idxpersist.IndexSegmentFileSet
}

// IndexSegmentFile is a file in an index segment file set.
type IndexSegmentFile interface {
	idxpersist.IndexSegmentFileSet
}

// IndexReaderOpenOptions is the index file set reader open options.
type IndexReaderOpenOptions struct {
	Identifier  FileSetFileIdentifier
	FileSetType persist.FileSetType
}

// IndexReaderOpenResult describes the results of opening a
// index file set volume.
type IndexReaderOpenResult struct {
	Shards map[uint32]struct{}
}

// IndexFileSetReader is an index file set reader.
type IndexFileSetReader interface {
	idxpersist.IndexFileSetReader
	io.Closer

	// Open the index file set reader.
	Open(opts IndexReaderOpenOptions) (IndexReaderOpenResult, error)

	// Validate returns whether all checksums were matched as expected,
	// it must be called after reading all the segment file sets otherwise
	// it returns an error.
	Validate() error
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

	// SetTagEncoderPool sets the tag encoder pool
	SetTagEncoderPool(value serialize.TagEncoderPool) Options

	// TagEncoderPool returns the tag encoder pool
	TagEncoderPool() serialize.TagEncoderPool

	// SetTagDecoderPool sets the tag decoder pool
	SetTagDecoderPool(value serialize.TagDecoderPool) Options

	// TagDecoderPool returns the tag decoder pool
	TagDecoderPool() serialize.TagDecoderPool

	// SetFStOptions sets the fst options
	SetFSTOptions(value fst.Options) Options

	// FSTOptions returns the fst options
	FSTOptions() fst.Options
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

	// SetIdentifierPool sets the identifierPool
	SetIdentifierPool(value ident.Pool) BlockRetrieverOptions

	// IdentifierPool returns the identifierPool
	IdentifierPool() ident.Pool
}
