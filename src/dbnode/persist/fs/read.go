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

package fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

var (
	// ErrCheckpointFileNotFound returned when the checkpoint file doesn't exist
	ErrCheckpointFileNotFound = errors.New("checkpoint file does not exist")

	// errReadNotExpectedSize returned when the size of the next read does not match size specified by the index
	errReadNotExpectedSize = errors.New("next read not expected size")

	errUnexpectedSortByOffset = errors.New("should not sort index by offsets when doing reads sorted by id")

	// errReadMetadataOptimizedForRead returned when we optimized for only reading metadata but are attempting a regular read
	errReadMetadataOptimizedForRead = errors.New("read metadata optimized for regular read")

	errStreamingRequired    = errors.New("streaming must be enabled for streaming read methods")
	errStreamingUnsupported = errors.New("streaming mode be disabled for non streaming read methods")
)

const (
	mmapPersistFsDataName      = "mmap.persist.fs.data"
	mmapPersistFsDataIndexName = "mmap.persist.fs.dataindex"
)

type reader struct {
	opts          Options
	hugePagesOpts mmap.HugeTLBOptions

	filePathPrefix string
	namespace      ident.ID

	start     time.Time
	blockSize time.Duration

	infoFdWithDigest           digest.FdWithDigestReader
	bloomFilterWithDigest      digest.FdWithDigestReader
	digestFdWithDigestContents digest.FdWithDigestContentsReader

	indexFd                 *os.File
	indexMmap               mmap.Descriptor
	indexDecoderStream      dataFileSetReaderDecoderStream
	indexEntriesByOffsetAsc []schema.IndexEntry

	dataFd     *os.File
	dataMmap   mmap.Descriptor
	dataReader digest.ReaderWithDigest

	bloomFilterFd *os.File

	entries         int
	bloomFilterInfo schema.IndexBloomFilterInfo
	entriesRead     int
	metadataRead    int
	decoder         *msgpack.Decoder
	digestBuf       digest.Buffer
	bytesPool       pool.CheckedBytesPool
	tagDecoderPool  serialize.TagDecoderPool

	streamingID   ident.BytesID
	streamingTags []byte
	streamingData []byte

	expectedInfoDigest        uint32
	expectedIndexDigest       uint32
	expectedDataDigest        uint32
	expectedDigestOfDigest    uint32
	expectedBloomFilterDigest uint32
	shard                     uint32
	volume                    int
	open                      bool
	streamingEnabled          bool
	// NB(bodu): Informs whether or not we optimize for only reading
	// metadata. We don't need to sort for reading metadata but sorting is
	// required if we are performing regulars reads.
	optimizedReadMetadataOnly bool
}

// NewReader returns a new reader and expects all files to exist. Will read the
// index info in full on call to Open. The bytesPool can be passed as nil if callers
// would prefer just dynamically allocated IDs and data.
func NewReader(
	bytesPool pool.CheckedBytesPool,
	opts Options,
) (DataFileSetReader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &reader{
		// When initializing new fields that should be static, be sure to save
		// and reset them after Close() resets the fields to all default values.
		opts:           opts,
		filePathPrefix: opts.FilePathPrefix(),
		hugePagesOpts: mmap.HugeTLBOptions{
			Enabled:   opts.MmapEnableHugeTLB(),
			Threshold: opts.MmapHugeTLBThreshold(),
		},
		infoFdWithDigest:           digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsReader(opts.InfoReaderBufferSize()),
		bloomFilterWithDigest:      digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
		indexDecoderStream:         newReaderDecoderStream(),
		dataReader:                 digest.NewReaderWithDigest(nil),
		decoder:                    msgpack.NewDecoder(opts.DecodingOptions()),
		digestBuf:                  digest.NewBuffer(),
		bytesPool:                  bytesPool,
		tagDecoderPool:             opts.TagDecoderPool(),
	}, nil
}

func (r *reader) Open(opts DataReaderOpenOptions) error {
	var (
		namespace   = opts.Identifier.Namespace
		shard       = opts.Identifier.Shard
		blockStart  = opts.Identifier.BlockStart
		volumeIndex = opts.Identifier.VolumeIndex
		err         error
	)

	var (
		shardDir            string
		checkpointFilepath  string
		infoFilepath        string
		digestFilepath      string
		bloomFilterFilepath string
		indexFilepath       string
		dataFilepath        string
	)

	r.streamingEnabled = opts.StreamingEnabled

	switch opts.FileSetType {
	case persist.FileSetSnapshotType:
		shardDir = ShardSnapshotsDirPath(r.filePathPrefix, namespace, shard)
		checkpointFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, checkpointFileSuffix)
		infoFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, infoFileSuffix)
		digestFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, digestFileSuffix)
		bloomFilterFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, bloomFilterFileSuffix)
		indexFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, indexFileSuffix)
		dataFilepath = filesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, dataFileSuffix)
	case persist.FileSetFlushType:
		shardDir = ShardDataDirPath(r.filePathPrefix, namespace, shard)

		isLegacy := false
		if volumeIndex == 0 {
			isLegacy, err = isFirstVolumeLegacy(shardDir, blockStart, checkpointFileSuffix)
			if err != nil {
				return err
			}
		}

		checkpointFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, checkpointFileSuffix, isLegacy)
		infoFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, infoFileSuffix, isLegacy)
		digestFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, digestFileSuffix, isLegacy)
		bloomFilterFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, bloomFilterFileSuffix, isLegacy)
		indexFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, indexFileSuffix, isLegacy)
		dataFilepath = dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, dataFileSuffix, isLegacy)
	default:
		return fmt.Errorf("unable to open reader with fileset type: %s", opts.FileSetType)
	}

	// If there is no checkpoint file, don't read the data files.
	digest, err := readCheckpointFile(checkpointFilepath, r.digestBuf)
	if err != nil {
		return err
	}
	r.expectedDigestOfDigest = digest

	var infoFd, digestFd *os.File
	err = openFiles(os.Open, map[string]**os.File{
		infoFilepath:        &infoFd,
		digestFilepath:      &digestFd,
		bloomFilterFilepath: &r.bloomFilterFd,
	})
	if err != nil {
		return err
	}

	r.infoFdWithDigest.Reset(infoFd)
	r.digestFdWithDigestContents.Reset(digestFd)

	defer func() {
		// NB(r): We don't need to keep these FDs open as we use these up front
		r.infoFdWithDigest.Close()
		r.digestFdWithDigestContents.Close()
	}()

	result, err := mmap.Files(os.Open, map[string]mmap.FileDesc{
		indexFilepath: mmap.FileDesc{
			File:       &r.indexFd,
			Descriptor: &r.indexMmap,
			Options: mmap.Options{
				Read:    true,
				HugeTLB: r.hugePagesOpts,
				ReporterOptions: mmap.ReporterOptions{
					Context: mmap.Context{
						Name: mmapPersistFsDataIndexName,
					},
					Reporter: r.opts.MmapReporter(),
				},
			},
		},
		dataFilepath: mmap.FileDesc{
			File:       &r.dataFd,
			Descriptor: &r.dataMmap,
			Options: mmap.Options{
				Read:    true,
				HugeTLB: r.hugePagesOpts,
				ReporterOptions: mmap.ReporterOptions{
					Context: mmap.Context{
						Name: mmapPersistFsDataName,
					},
					Reporter: r.opts.MmapReporter(),
				},
			},
		},
	})
	if err != nil {
		return err
	}

	if warning := result.Warning; warning != nil {
		logger := r.opts.InstrumentOptions().Logger()
		logger.Warn("warning while mmapping files in reader", zap.Error(warning))
	}

	r.indexDecoderStream.Reset(r.indexMmap.Bytes)
	r.dataReader.Reset(bytes.NewReader(r.dataMmap.Bytes))

	if err := r.readDigest(); err != nil {
		// Try to close if failed to read
		r.Close()
		return err
	}
	infoStat, err := infoFd.Stat()
	if err != nil {
		r.Close()
		return err
	}
	if err := r.readInfo(int(infoStat.Size())); err != nil {
		r.Close()
		return err
	}
	if opts.StreamingEnabled {
		r.decoder.Reset(r.indexDecoderStream)
	} else if err := r.readIndexAndSortByOffsetAsc(); err != nil {
		r.Close()
		return err
	}

	r.open = true
	r.namespace = namespace
	r.shard = shard
	r.optimizedReadMetadataOnly = opts.OptimizedReadMetadataOnly

	return nil
}

func (r *reader) Status() DataFileSetReaderStatus {
	return DataFileSetReaderStatus{
		Open:       r.open,
		Namespace:  r.namespace,
		Shard:      r.shard,
		Volume:     r.volume,
		BlockStart: r.start,
		BlockSize:  r.blockSize,
	}
}

func (r *reader) readDigest() error {
	fsDigests, err := readFileSetDigests(r.digestFdWithDigestContents)
	if err != nil {
		return err
	}

	err = r.digestFdWithDigestContents.Validate(r.expectedDigestOfDigest)
	if err != nil {
		return err
	}

	// Note that we skip over the summaries file digest here which is available,
	// but we don't need
	r.expectedInfoDigest = fsDigests.infoDigest
	r.expectedIndexDigest = fsDigests.indexDigest
	r.expectedBloomFilterDigest = fsDigests.bloomFilterDigest
	r.expectedDataDigest = fsDigests.dataDigest

	return nil
}

func (r *reader) readInfo(size int) error {
	buf := make([]byte, size)
	n, err := r.infoFdWithDigest.ReadAllAndValidate(buf, r.expectedInfoDigest)
	if err != nil {
		return err
	}
	r.decoder.Reset(msgpack.NewByteDecoderStream(buf[:n]))
	info, err := r.decoder.DecodeIndexInfo()
	if err != nil {
		return err
	}
	r.start = xtime.FromNanoseconds(info.BlockStart)
	r.volume = info.VolumeIndex
	r.blockSize = time.Duration(info.BlockSize)
	r.entries = int(info.Entries)
	r.entriesRead = 0
	r.metadataRead = 0
	r.bloomFilterInfo = info.BloomFilter
	return nil
}

func (r *reader) readIndexAndSortByOffsetAsc() error {
	if r.streamingEnabled {
		return errUnexpectedSortByOffset
	}

	r.decoder.Reset(r.indexDecoderStream)
	for i := 0; i < r.entries; i++ {
		entry, err := r.decoder.DecodeIndexEntry(nil)
		if err != nil {
			return err
		}
		r.indexEntriesByOffsetAsc = append(r.indexEntriesByOffsetAsc, entry)
	}
	// This is false by default so we always sort unless otherwise specified.
	if !r.optimizedReadMetadataOnly {
		// NB(r): As we decode each block we need access to each index entry
		// in the order we decode the data. This is only required for regular reads.
		sort.Sort(indexEntriesByOffsetAsc(r.indexEntriesByOffsetAsc))
	}
	return nil
}

func (r *reader) StreamingRead() (StreamedDataEntry, error) {
	if !r.streamingEnabled {
		return StreamedDataEntry{}, errStreamingRequired
	}

	if r.entriesRead >= r.entries {
		return StreamedDataEntry{}, io.EOF
	}

	entry, err := r.decoder.DecodeIndexEntry(nil)
	if err != nil {
		return StreamedDataEntry{}, err
	}

	if entry.Offset+entry.Size > int64(len(r.dataMmap.Bytes)) {
		return StreamedDataEntry{}, fmt.Errorf(
			"attempt to read beyond data file size (offset=%d, size=%d, file size=%d)",
			entry.Offset, entry.Size, len(r.dataMmap.Bytes))
	}
	data := r.dataMmap.Bytes[entry.Offset : entry.Offset+entry.Size]

	// NB(r): _must_ check the checksum against known checksum as the data
	// file might not have been verified if we haven't read through the file yet.
	if entry.DataChecksum != int64(digest.Checksum(data)) {
		return StreamedDataEntry{}, errSeekChecksumMismatch
	}

	r.streamingData = append(r.streamingData[:0], data...)
	r.streamingID = append(r.streamingID[:0], entry.ID...)
	r.streamingTags = append(r.streamingTags[:0], entry.EncodedTags...)

	r.entriesRead++

	dataEntry := StreamedDataEntry{
		ID:           r.streamingID,
		EncodedTags:  r.streamingTags,
		Data:         r.streamingData,
		DataChecksum: uint32(entry.DataChecksum),
	}

	return dataEntry, nil
}

func (r *reader) Read() (ident.ID, ident.TagIterator, checked.Bytes, uint32, error) {
	if r.streamingEnabled {
		return nil, nil, nil, 0, errStreamingUnsupported
	}

	// NB(bodu): We cannot perform regular reads if we're optimizing for only reading metadata.
	if r.optimizedReadMetadataOnly {
		return nil, nil, nil, 0, errReadMetadataOptimizedForRead
	}
	if r.entries > 0 && len(r.indexEntriesByOffsetAsc) < r.entries {
		// Have not read the index yet, this is required when reading
		// data as we need each index entry in order by by the offset ascending
		if err := r.readIndexAndSortByOffsetAsc(); err != nil {
			return nil, nil, nil, 0, err
		}
	}

	if r.entriesRead >= r.entries {
		return nil, nil, nil, 0, io.EOF
	}

	entry := r.indexEntriesByOffsetAsc[r.entriesRead]

	var data checked.Bytes
	if r.bytesPool != nil {
		data = r.bytesPool.Get(int(entry.Size))
		data.IncRef()
		defer data.DecRef()
		data.Resize(int(entry.Size))
	} else {
		data = checked.NewBytes(make([]byte, entry.Size), nil)
		data.IncRef()
		defer data.DecRef()
	}

	n, err := r.dataReader.Read(data.Bytes())
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if n != int(entry.Size) {
		return nil, nil, nil, 0, errReadNotExpectedSize
	}

	id := r.entryClonedID(entry.ID)
	tags := r.entryClonedEncodedTagsIter(entry.EncodedTags)

	r.entriesRead++
	return id, tags, data, uint32(entry.DataChecksum), nil
}

func (r *reader) ReadMetadata() (ident.ID, ident.TagIterator, int, uint32, error) {
	if r.streamingEnabled {
		return nil, nil, 0, 0, errStreamingUnsupported
	}

	if r.metadataRead >= r.entries {
		return nil, nil, 0, 0, io.EOF
	}

	entry := r.indexEntriesByOffsetAsc[r.metadataRead]
	id := r.entryClonedID(entry.ID)
	tags := r.entryClonedEncodedTagsIter(entry.EncodedTags)
	length := int(entry.Size)
	checksum := uint32(entry.DataChecksum)

	r.metadataRead++
	return id, tags, length, checksum, nil
}

func (r *reader) ReadBloomFilter() (*ManagedConcurrentBloomFilter, error) {
	return newManagedConcurrentBloomFilterFromFile(
		r.bloomFilterFd,
		r.bloomFilterWithDigest,
		r.expectedBloomFilterDigest,
		uint(r.bloomFilterInfo.NumElementsM),
		uint(r.bloomFilterInfo.NumHashesK),
		r.opts.ForceBloomFilterMmapMemory(),
		mmap.ReporterOptions{
			Reporter: r.opts.MmapReporter(),
		},
	)
}

func (r *reader) entryClonedBytes(bytes []byte) checked.Bytes {
	var bytesClone checked.Bytes
	if r.bytesPool != nil {
		bytesClone = r.bytesPool.Get(len(bytes))
	} else {
		bytesClone = checked.NewBytes(make([]byte, 0, len(bytes)), nil)
	}
	bytesClone.IncRef()
	bytesClone.AppendAll(bytes)
	bytesClone.DecRef()
	return bytesClone
}

func (r *reader) entryClonedID(id []byte) ident.ID {
	return ident.BinaryID(r.entryClonedBytes(id))
}

func (r *reader) entryClonedEncodedTagsIter(encodedTags []byte) ident.TagIterator {
	if len(encodedTags) == 0 {
		// No tags set for this entry, return an empty tag iterator
		return ident.EmptyTagIterator
	}
	decoder := r.tagDecoderPool.Get()
	decoder.Reset(r.entryClonedBytes(encodedTags))
	return decoder
}

// NB(xichen): Validate should be called after all data is read because
// the digest is calculated for the entire data file.
func (r *reader) Validate() error {
	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(r.ValidateMetadata())
	multiErr = multiErr.Add(r.ValidateData())
	return multiErr.FinalError()
}

// NB(r): ValidateMetadata can be called immediately after Open(...) since
// the metadata is read upfront.
func (r *reader) ValidateMetadata() error {
	err := r.indexDecoderStream.reader().Validate(r.expectedIndexDigest)
	if err != nil {
		return fmt.Errorf("could not validate index file: %v", err)
	}
	return nil
}

// NB(xichen): ValidateData should be called after all data is read because
// the digest is calculated for the entire data file.
func (r *reader) ValidateData() error {
	err := r.dataReader.Validate(r.expectedDataDigest)
	if err != nil {
		return fmt.Errorf("could not validate data file: %v", err)
	}
	return nil
}

func (r *reader) Range() xtime.Range {
	return xtime.Range{Start: r.start, End: r.start.Add(r.blockSize)}
}

func (r *reader) Entries() int {
	return r.entries
}

func (r *reader) EntriesRead() int {
	return r.entriesRead
}

func (r *reader) MetadataRead() int {
	return r.metadataRead
}

func (r *reader) StreamingEnabled() bool {
	return r.streamingEnabled
}

func (r *reader) Close() error {
	// Close and prepare resources that are to be reused
	multiErr := xerrors.NewMultiError()
	multiErr = multiErr.Add(mmap.Munmap(r.indexMmap))
	multiErr = multiErr.Add(mmap.Munmap(r.dataMmap))
	multiErr = multiErr.Add(r.indexFd.Close())
	multiErr = multiErr.Add(r.dataFd.Close())
	multiErr = multiErr.Add(r.bloomFilterFd.Close())
	r.indexDecoderStream.Reset(nil)
	r.dataReader.Reset(nil)
	for i := 0; i < len(r.indexEntriesByOffsetAsc); i++ {
		r.indexEntriesByOffsetAsc[i].ID = nil
	}
	r.indexEntriesByOffsetAsc = r.indexEntriesByOffsetAsc[:0]

	// Save fields we want to reassign after resetting struct
	opts := r.opts
	filePathPrefix := r.filePathPrefix
	hugePagesOpts := r.hugePagesOpts
	infoFdWithDigest := r.infoFdWithDigest
	digestFdWithDigestContents := r.digestFdWithDigestContents
	bloomFilterWithDigest := r.bloomFilterWithDigest
	indexDecoderStream := r.indexDecoderStream
	dataReader := r.dataReader
	decoder := r.decoder
	digestBuf := r.digestBuf
	bytesPool := r.bytesPool
	tagDecoderPool := r.tagDecoderPool
	indexEntriesByOffsetAsc := r.indexEntriesByOffsetAsc

	// Reset struct
	*r = reader{}

	// Reset the saved fields
	r.opts = opts
	r.filePathPrefix = filePathPrefix
	r.hugePagesOpts = hugePagesOpts
	r.infoFdWithDigest = infoFdWithDigest
	r.digestFdWithDigestContents = digestFdWithDigestContents
	r.bloomFilterWithDigest = bloomFilterWithDigest
	r.indexDecoderStream = indexDecoderStream
	r.dataReader = dataReader
	r.decoder = decoder
	r.digestBuf = digestBuf
	r.bytesPool = bytesPool
	r.tagDecoderPool = tagDecoderPool
	r.indexEntriesByOffsetAsc = indexEntriesByOffsetAsc

	return multiErr.FinalError()
}

// indexEntriesByOffsetAsc implements sort.Sort
type indexEntriesByOffsetAsc []schema.IndexEntry

func (e indexEntriesByOffsetAsc) Len() int {
	return len(e)
}

func (e indexEntriesByOffsetAsc) Less(i, j int) bool {
	return e[i].Offset < e[j].Offset
}

func (e indexEntriesByOffsetAsc) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
