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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"
)

// FileSetFileIdentifier contains all the information required to identify a FileSetFile.
type FileSetFileIdentifier struct {
	FileSetContentType persist.FileSetContentType
	Namespace          ident.ID
	BlockStart         time.Time
	// Only required for data content files
	Shard uint32
	// Required for snapshot files (index yes, data yes) and flush files (index yes, data yes)
	VolumeIndex int
}

// DataWriterOpenOptions is the options struct for the Open method on the DataFileSetWriter.
type DataWriterOpenOptions struct {
	FileSetType        persist.FileSetType
	FileSetContentType persist.FileSetContentType
	Identifier         FileSetFileIdentifier
	BlockSize          time.Duration
	// Only used when writing snapshot files
	Snapshot DataWriterSnapshotOptions
}

// DataWriterSnapshotOptions is the options struct for Open method on the DataFileSetWriter
// that contains information specific to writing snapshot files.
type DataWriterSnapshotOptions struct {
	SnapshotTime time.Time
	SnapshotID   []byte
}

// DataFileSetWriter provides an unsynchronized writer for a TSDB file set.
type DataFileSetWriter interface {
	io.Closer

	// Open opens the files for writing data to the given shard in the given namespace.
	// This method is not thread-safe, so its the callers responsibilities that they never
	// try and write two snapshot files for the same block start at the same time or their
	// will be a race in determining the snapshot file's index.
	Open(opts DataWriterOpenOptions) error

	// Write will write the id and data pair and returns an error on a write error. Callers
	// must not call this method with a given ID more than once.
	Write(metadata persist.Metadata, data checked.Bytes, checksum uint32) error

	// WriteAll will write the id and all byte slices and returns an error on a write error.
	// Callers must not call this method with a given ID more than once.
	WriteAll(metadata persist.Metadata, data []checked.Bytes, checksum uint32) error

	// DeferClose returns a DataCloser that defers writing of a checkpoint file.
	DeferClose() (persist.DataCloser, error)
}

// SnapshotMetadataFileWriter writes out snapshot metadata files.
type SnapshotMetadataFileWriter interface {
	Write(args SnapshotMetadataWriteArgs) error
}

// SnapshotMetadataFileReader reads snapshot metadata files.
type SnapshotMetadataFileReader interface {
	Read(id SnapshotMetadataIdentifier) (SnapshotMetadata, error)
}

// DataFileSetReaderStatus describes the status of a file set reader.
type DataFileSetReaderStatus struct {
	Namespace  ident.ID
	BlockStart time.Time
	Shard      uint32
	Volume     int
	Open       bool
	BlockSize  time.Duration
}

// DataReaderOpenOptions is options struct for the reader open method.
type DataReaderOpenOptions struct {
	// Identifier allows to identify a FileSetFile.
	Identifier FileSetFileIdentifier
	// FileSetType is the file set type.
	FileSetType persist.FileSetType
	// StreamingEnabled enables using streaming methods, such as DataFileSetReader.StreamingRead.
	StreamingEnabled bool
	// NB(bodu): This option can inform the reader to optimize for reading
	// only metadata by not sorting index entries. Setting this option will
	// throw an error if a regular `Read()` is attempted.
	OptimizedReadMetadataOnly bool
}

// DataFileSetReader provides an unsynchronized reader for a TSDB file set.
type DataFileSetReader interface {
	io.Closer

	// Open opens the files for the given shard and version for reading
	Open(opts DataReaderOpenOptions) error

	// Status returns the status of the reader
	Status() DataFileSetReaderStatus

	// Read returns the next id, tags, data, checksum tuple or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	// Note: make sure to finalize the ID, close the Tags and finalize the Data when done with
	// them so they can be returned to their respective pools.
	Read() (id ident.ID, tags ident.TagIterator, data checked.Bytes, checksum uint32, err error)

	// StreamingRead returns the next unpooled id, encodedTags, data, checksum
	// values ordered by id, or error, will return io.EOF at end of volume.
	// Can only by used when DataReaderOpenOptions.StreamingEnabled is true.
	// Note: the returned data gets invalidated on the next call to StreamingRead.
	StreamingRead() (StreamedDataEntry, error)

	// ReadMetadata returns the next id and metadata or error, will return io.EOF at end of volume.
	// Use either Read or ReadMetadata to progress through a volume, but not both.
	// Note: make sure to finalize the ID, and close the Tags when done with them so they can
	// be returned to their respective pools.
	ReadMetadata() (id ident.ID, tags ident.TagIterator, length int, checksum uint32, err error)

	// ReadBloomFilter returns the bloom filter stored on disk in a container object that is safe
	// for concurrent use and has a Close() method for releasing resources when done.
	ReadBloomFilter() (*ManagedConcurrentBloomFilter, error)

	// Validate validates both the metadata and data and returns an error if either is corrupted.
	Validate() error

	// ValidateMetadata validates the data and returns an error if the data is corrupted.
	ValidateMetadata() error

	// ValidateData validates the data and returns an error if the data is corrupted.
	ValidateData() error

	// Range returns the time range associated with data in the volume.
	Range() xtime.Range

	// Entries returns the count of entries in the volume.
	Entries() int

	// EntriesRead returns the position read into the volume.
	EntriesRead() int

	// MetadataRead returns the position of metadata read into the volume.
	MetadataRead() int

	// StreamingEnabled returns true if the reader is opened in streaming mode.
	StreamingEnabled() bool
}

// DataFileSetSeeker provides an out of order reader for a TSDB file set.
type DataFileSetSeeker interface {
	io.Closer

	// Open opens the files for the given shard and version for reading.
	Open(
		namespace ident.ID,
		shard uint32,
		start time.Time,
		volume int,
		resources ReusableSeekerResources,
	) error

	// SeekByID returns the data for specified ID provided the index was loaded upon open. An
	// error will be returned if the index was not loaded or ID cannot be found.
	SeekByID(id ident.ID, resources ReusableSeekerResources) (data checked.Bytes, err error)

	// SeekByIndexEntry is similar to Seek, but uses an IndexEntry instead of
	// looking it up on its own. Useful in cases where you've already obtained an
	// entry and don't want to waste resources looking it up again.
	SeekByIndexEntry(entry IndexEntry, resources ReusableSeekerResources) (checked.Bytes, error)

	// SeekIndexEntry returns the IndexEntry for the specified ID. This can be useful
	// ahead of issuing a number of seek requests so that the seek requests can be
	// made in order. The returned IndexEntry can also be passed to SeekByIndexEntry
	// to prevent duplicate index lookups.
	SeekIndexEntry(id ident.ID, resources ReusableSeekerResources) (IndexEntry, error)

	// SeekWideEntry seeks in a manner similar to SeekIndexEntry, but
	// instead yields a wide entry checksum of the series.
	SeekWideEntry(
		id ident.ID,
		filter schema.WideEntryFilter,
		resources ReusableSeekerResources,
	) (xio.WideEntry, error)

	// Range returns the time range associated with data in the volume
	Range() xtime.Range

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

// ConcurrentDataFileSetSeeker is a limited interface that is returned when ConcurrentClone() is called
// on DataFileSetSeeker. A seeker is essentially  a wrapper around file
// descriptors around a set of files, allowing for interaction with them.
// We can ask a seeker for a specific time series, which will then be streamed
// out from the according data file.
// The clones can be used together concurrently and share underlying resources.
// Clones are no longer usable once the original has been closed.
type ConcurrentDataFileSetSeeker interface {
	io.Closer

	// SeekByID is the same as in DataFileSetSeeker.
	SeekByID(id ident.ID, resources ReusableSeekerResources) (data checked.Bytes, err error)

	// SeekByIndexEntry is the same as in DataFileSetSeeker.
	SeekByIndexEntry(entry IndexEntry, resources ReusableSeekerResources) (checked.Bytes, error)

	// SeekIndexEntry is the same as in DataFileSetSeeker.
	SeekIndexEntry(id ident.ID, resources ReusableSeekerResources) (IndexEntry, error)

	// SeekWideEntry is the same as in DataFileSetSeeker.
	SeekWideEntry(
		id ident.ID,
		filter schema.WideEntryFilter,
		resources ReusableSeekerResources,
	) (xio.WideEntry, error)

	// ConcurrentIDBloomFilter is the same as in DataFileSetSeeker.
	ConcurrentIDBloomFilter() *ManagedConcurrentBloomFilter
}

// DataFileSetSeekerManager provides management of seekers for a TSDB namespace.
type DataFileSetSeekerManager interface {
	io.Closer

	// Open opens the seekers for a given namespace.
	Open(
		md namespace.Metadata,
		shardSet sharding.ShardSet,
	) error

	// CacheShardIndices will pre-parse the indexes for given shards
	// to improve times when seeking to a block.
	CacheShardIndices(shards []uint32) error

	// AssignShardSet assigns current per ns shardset.
	AssignShardSet(shardSet sharding.ShardSet)

	// Borrow returns an open seeker for a given shard, block start time, and
	// volume.
	Borrow(shard uint32, start time.Time) (ConcurrentDataFileSetSeeker, error)

	// Return returns (closes) an open seeker for a given shard, block start
	// time, and volume.
	Return(shard uint32, start time.Time, seeker ConcurrentDataFileSetSeeker) error

	// Test checks if an ID exists in a concurrent ID bloom filter for a
	// given shard, block, start time and volume.
	Test(id ident.ID, shard uint32, start time.Time) (bool, error)
}

// DataBlockRetriever provides a block retriever for TSDB file sets.
type DataBlockRetriever interface {
	io.Closer
	block.DatabaseBlockRetriever

	// Open the block retriever to retrieve from a namespace.
	Open(
		md namespace.Metadata,
		shardSet sharding.ShardSet,
	) error
}

// RetrievableDataBlockSegmentReader is a retrievable block reader.
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
	Snapshot        IndexWriterSnapshotOptions
	IndexVolumeType idxpersist.IndexVolumeType
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

// Options represents the options for filesystem persistence.
type Options interface {
	// Validate will validate the options and return an error if not valid.
	Validate() error

	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() instrument.Options

	// SetRuntimeOptionsManager sets the runtime options manager.
	SetRuntimeOptionsManager(value runtime.OptionsManager) Options

	// RuntimeOptionsManager returns the runtime options manager.
	RuntimeOptionsManager() runtime.OptionsManager

	// SetDecodingOptions sets the decoding options.
	SetDecodingOptions(value msgpack.DecodingOptions) Options

	// DecodingOptions returns the decoding options.
	DecodingOptions() msgpack.DecodingOptions

	// SetFilePathPrefix sets the file path prefix for sharded TSDB files.
	SetFilePathPrefix(value string) Options

	// FilePathPrefix returns the file path prefix for sharded TSDB files.
	FilePathPrefix() string

	// SetNewFileMode sets the new file mode.
	SetNewFileMode(value os.FileMode) Options

	// NewFileMode returns the new file mode.
	NewFileMode() os.FileMode

	// SetNewDirectoryMode sets the new directory mode.
	SetNewDirectoryMode(value os.FileMode) Options

	// NewDirectoryMode returns the new directory mode.
	NewDirectoryMode() os.FileMode

	// SetIndexSummariesPercent size sets the percent of index summaries to write.
	SetIndexSummariesPercent(value float64) Options

	// IndexSummariesPercent size returns the percent of index summaries to write.
	IndexSummariesPercent() float64

	// SetIndexBloomFilterFalsePositivePercent size sets the percent of false positive
	// rate to use for the index bloom filter size and k hashes estimation.
	SetIndexBloomFilterFalsePositivePercent(value float64) Options

	// IndexBloomFilterFalsePositivePercent size returns the percent of false positive
	// rate to use for the index bloom filter size and k hashes estimation.
	IndexBloomFilterFalsePositivePercent() float64

	// SetForceIndexSummariesMmapMemory sets whether the summaries files will be mmap'd
	// as an anonymous region, or as a file.
	SetForceIndexSummariesMmapMemory(value bool) Options

	// ForceIndexSummariesMmapMemory returns whether the summaries files will be mmap'd
	// as an anonymous region, or as a file.
	ForceIndexSummariesMmapMemory() bool

	// SetForceBloomFilterMmapMemory sets whether the bloom filters will be mmap'd.
	// as an anonymous region, or as a file.
	SetForceBloomFilterMmapMemory(value bool) Options

	// ForceBloomFilterMmapMemory returns whether the bloom filters will be mmap'd.
	// as an anonymous region, or as a file.
	ForceBloomFilterMmapMemory() bool

	// SetWriterBufferSize sets the buffer size for writing TSDB files.
	SetWriterBufferSize(value int) Options

	// WriterBufferSize returns the buffer size for writing TSDB files.
	WriterBufferSize() int

	// SetInfoReaderBufferSize sets the buffer size for reading TSDB info,
	// digest and checkpoint files.
	SetInfoReaderBufferSize(value int) Options

	// InfoReaderBufferSize returns the buffer size for reading TSDB info,
	// digest and checkpoint files.
	InfoReaderBufferSize() int

	// SetDataReaderBufferSize sets the buffer size for reading TSDB data and index files.
	SetDataReaderBufferSize(value int) Options

	// DataReaderBufferSize returns the buffer size for reading TSDB data and index files.
	DataReaderBufferSize() int

	// SetSeekReaderBufferSize size sets the buffer size for seeking TSDB files.
	SetSeekReaderBufferSize(value int) Options

	// SeekReaderBufferSize size returns the buffer size for seeking TSDB files.
	SeekReaderBufferSize() int

	// SetMmapEnableHugeTLB sets whether mmap huge pages are enabled when running on linux.
	SetMmapEnableHugeTLB(value bool) Options

	// MmapEnableHugeTLB returns whether mmap huge pages are enabled when running on linux.
	MmapEnableHugeTLB() bool

	// SetMmapHugeTLBThreshold sets the threshold when to use mmap huge pages for mmap'd files on linux.
	SetMmapHugeTLBThreshold(value int64) Options

	// MmapHugeTLBThreshold returns the threshold when to use mmap huge pages for mmap'd files on linux.
	MmapHugeTLBThreshold() int64

	// SetTagEncoderPool sets the tag encoder pool.
	SetTagEncoderPool(value serialize.TagEncoderPool) Options

	// TagEncoderPool returns the tag encoder pool.
	TagEncoderPool() serialize.TagEncoderPool

	// SetTagDecoderPool sets the tag decoder pool.
	SetTagDecoderPool(value serialize.TagDecoderPool) Options

	// TagDecoderPool returns the tag decoder pool.
	TagDecoderPool() serialize.TagDecoderPool

	// SetFSTOptions sets the fst options.
	SetFSTOptions(value fst.Options) Options

	// FSTOptions returns the fst options.
	FSTOptions() fst.Options

	// SetFStWriterOptions sets the fst writer options.
	SetFSTWriterOptions(value fst.WriterOptions) Options

	// FSTWriterOptions returns the fst writer options.
	FSTWriterOptions() fst.WriterOptions

	// SetMmapReporter sets the mmap reporter.
	SetMmapReporter(value mmap.Reporter) Options

	// MmapReporter returns the mmap reporter.
	MmapReporter() mmap.Reporter

	// SetIndexReaderAutovalidateIndexSegments sets the index reader to
	// autovalidate index segments data integrity on file open.
	SetIndexReaderAutovalidateIndexSegments(value bool) Options

	// IndexReaderAutovalidateIndexSegments returns the index reader to
	// autovalidate index segments data integrity on file open.
	IndexReaderAutovalidateIndexSegments() bool

	// SetEncodingOptions sets the encoder options used by the encoder.
	SetEncodingOptions(value msgpack.LegacyEncodingOptions) Options

	// EncodingOptions returns the encoder options used by the encoder.
	EncodingOptions() msgpack.LegacyEncodingOptions
}

// BlockRetrieverOptions represents the options for block retrieval.
type BlockRetrieverOptions interface {
	// Validate validates the options.
	Validate() error

	// SetRetrieveRequestPool sets the retrieve request pool.
	SetRetrieveRequestPool(value RetrieveRequestPool) BlockRetrieverOptions

	// RetrieveRequestPool returns the retrieve request pool.
	RetrieveRequestPool() RetrieveRequestPool

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.CheckedBytesPool) BlockRetrieverOptions

	// BytesPool returns the bytes pool.
	BytesPool() pool.CheckedBytesPool

	// SetFetchConcurrency sets the fetch concurrency.
	SetFetchConcurrency(value int) BlockRetrieverOptions

	// FetchConcurrency returns the fetch concurrency.
	FetchConcurrency() int

	// SetCacheBlocksOnRetrieve sets whether to cache blocks after retrieval at a global level.
	SetCacheBlocksOnRetrieve(value bool) BlockRetrieverOptions

	// CacheBlocksOnRetrieve returns whether to cache blocks after retrieval at a global level.
	CacheBlocksOnRetrieve() bool

	// SetIdentifierPool sets the identifierPool.
	SetIdentifierPool(value ident.Pool) BlockRetrieverOptions

	// IdentifierPool returns the identifierPool.
	IdentifierPool() ident.Pool

	// SetBlockLeaseManager sets the block leaser.
	SetBlockLeaseManager(leaseMgr block.LeaseManager) BlockRetrieverOptions

	// BlockLeaseManager returns the block leaser.
	BlockLeaseManager() block.LeaseManager

	// SetQueryLimits sets query limits.
	SetQueryLimits(value limits.QueryLimits) BlockRetrieverOptions

	// QueryLimits returns the query limits.
	QueryLimits() limits.QueryLimits
}

// ForEachRemainingFn is the function that is run on each of the remaining
// series of the merge target that did not intersect with the fileset.
type ForEachRemainingFn func(seriesMetadata doc.Metadata, data block.FetchBlockResult) error

// MergeWith is an interface that the fs merger uses to merge data with.
type MergeWith interface {
	// Read returns the data for the given block start and series ID, whether
	// any data was found, and the error encountered (if any).
	Read(
		ctx context.Context,
		seriesID ident.ID,
		blockStart xtime.UnixNano,
		nsCtx namespace.Context,
	) ([]xio.BlockReader, bool, error)

	// ForEachRemaining loops through each seriesID/blockStart combination that
	// was not already handled by a call to Read().
	ForEachRemaining(
		ctx context.Context,
		blockStart xtime.UnixNano,
		fn ForEachRemainingFn,
		nsCtx namespace.Context,
	) error
}

// Merger is in charge of merging filesets with some target MergeWith interface.
type Merger interface {
	// Merge merges the specified fileset file with a merge target.
	Merge(
		fileID FileSetFileIdentifier,
		mergeWith MergeWith,
		nextVolumeIndex int,
		flushPreparer persist.FlushPreparer,
		nsCtx namespace.Context,
		onFlush persist.OnFlushSeries,
	) (persist.DataCloser, error)

	// MergeAndCleanup merges the specified fileset file with a merge target and
	// removes the previous version of the fileset. This should only be called
	// within the bootstrapper. Any other file deletions outside of the
	// bootstrapper should be handled by the CleanupManager.
	MergeAndCleanup(
		fileID FileSetFileIdentifier,
		mergeWith MergeWith,
		nextVolumeIndex int,
		flushPreparer persist.FlushPreparer,
		nsCtx namespace.Context,
		onFlush persist.OnFlushSeries,
		isBootstrapped bool,
	) error
}

// NewMergerFn is the function to call to get a new Merger.
type NewMergerFn func(
	reader DataFileSetReader,
	blockAllocSize int,
	srPool xio.SegmentReaderPool,
	multiIterPool encoding.MultiReaderIteratorPool,
	identPool ident.Pool,
	encoderPool encoding.EncoderPool,
	contextPool context.Pool,
	filePathPrefix string,
	nsOpts namespace.Options,
) Merger

// Segments represents on index segments on disk for an index volume.
type Segments interface {
	ShardTimeRanges() result.ShardTimeRanges
	VolumeType() idxpersist.IndexVolumeType
	VolumeIndex() int
	AbsoluteFilePaths() []string
	BlockStart() time.Time
}

// IndexClaimsManager manages concurrent claims to volume indices per ns and block start.
// This allows multiple threads to safely increment the volume index.
type IndexClaimsManager interface {
	ClaimNextIndexFileSetVolumeIndex(
		md namespace.Metadata,
		blockStart time.Time,
	) (int, error)
}

// StreamedDataEntry contains the data of single entry returned by streaming method(s).
// The underlying data slices are reused and invalidated on every read.
type StreamedDataEntry struct {
	ID           ident.BytesID
	EncodedTags  ts.EncodedTags
	Data         []byte
	DataChecksum uint32
}

// NewReaderFn creates a new DataFileSetReader.
type NewReaderFn func(bytesPool pool.CheckedBytesPool, opts Options) (DataFileSetReader, error)

// DataEntryProcessor processes StreamedDataEntries.
type DataEntryProcessor interface {
	// SetEntriesCount sets the number of entries to be processed.
	SetEntriesCount(int)
	// ProcessEntry processes a single StreamedDataEntry.
	ProcessEntry(StreamedDataEntry) error
}
