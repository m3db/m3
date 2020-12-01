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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	xmsgpack "github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	// errSeekIDNotFound returned when ID cannot be found in the shard
	errSeekIDNotFound = errors.New("id not found in shard")

	// errSeekChecksumMismatch returned when data checksum does not match the expected checksum
	errSeekChecksumMismatch = errors.New("checksum does not match expected checksum")

	// errSeekNotCompleted returned when no error but seek did not complete.
	errSeekNotCompleted = errors.New("seek not completed")

	// errClonesShouldNotBeOpened returned when Open() is called on a clone
	errClonesShouldNotBeOpened = errors.New("clone should not be opened")
)

const (
	maxSimpleBytesPoolSliceSize = 4096
	// One for the ID and one for the tags.
	maxSimpleBytesPoolSize = 2
)

type seeker struct {
	opts seekerOpts

	// Data read from the indexInfo file. Note that we use xtime.UnixNano
	// instead of time.Time to avoid keeping an extra pointer around.
	start          xtime.UnixNano
	blockSize      time.Duration
	versionChecker schema.VersionChecker

	dataFd        *os.File
	indexFd       *os.File
	indexFileSize int64

	unreadBuf []byte

	// Bloom filter associated with the shard / block the seeker is responsible
	// for. Needs to be closed when done.
	bloomFilter *ManagedConcurrentBloomFilter
	indexLookup *nearestIndexOffsetLookup

	isClone bool
}

// IndexEntry is an entry from the index file which can be passed to
// SeekUsingIndexEntry to seek to the data for that entry.
type IndexEntry struct {
	Size         uint32
	DataChecksum uint32
	Offset       int64
	EncodedTags  checked.Bytes
}

// NewSeeker returns a new seeker.
func NewSeeker(
	filePathPrefix string,
	dataBufferSize int,
	infoBufferSize int,
	bytesPool pool.CheckedBytesPool,
	keepUnreadBuf bool,
	opts Options,
) DataFileSetSeeker {
	return newSeeker(seekerOpts{
		filePathPrefix: filePathPrefix,
		dataBufferSize: dataBufferSize,
		infoBufferSize: infoBufferSize,
		bytesPool:      bytesPool,
		keepUnreadBuf:  keepUnreadBuf,
		opts:           opts,
	})
}

type seekerOpts struct {
	filePathPrefix string
	infoBufferSize int
	dataBufferSize int
	bytesPool      pool.CheckedBytesPool
	keepUnreadBuf  bool
	opts           Options
}

// fileSetSeeker adds package level access to further methods
// on the seeker for use by the seeker manager for efficient
// multi-seeker use.
type fileSetSeeker interface {
	DataFileSetSeeker

	// unreadBuffer returns the unread buffer
	unreadBuffer() []byte

	// setUnreadBuffer sets the unread buffer
	setUnreadBuffer(buf []byte)
}

func newSeeker(opts seekerOpts) fileSetSeeker {
	return &seeker{
		opts: opts,
	}
}

func (s *seeker) ConcurrentIDBloomFilter() *ManagedConcurrentBloomFilter {
	return s.bloomFilter
}

func (s *seeker) Open(
	namespace ident.ID,
	shard uint32,
	blockStart time.Time,
	volumeIndex int,
	resources ReusableSeekerResources,
) error {
	if s.isClone {
		return errClonesShouldNotBeOpened
	}

	shardDir := ShardDataDirPath(s.opts.filePathPrefix, namespace, shard)
	var (
		infoFd, digestFd, bloomFilterFd, summariesFd *os.File
		err                                          error
		isLegacy                                     bool
	)

	if volumeIndex == 0 {
		isLegacy, err = isFirstVolumeLegacy(shardDir, blockStart, checkpointFileSuffix)
		if err != nil {
			return err
		}
	}

	// Open necessary files
	if err := openFiles(os.Open, map[string]**os.File{
		dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, infoFileSuffix, isLegacy):        &infoFd,
		dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, indexFileSuffix, isLegacy):       &s.indexFd,
		dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, dataFileSuffix, isLegacy):        &s.dataFd,
		dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, digestFileSuffix, isLegacy):      &digestFd,
		dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, bloomFilterFileSuffix, isLegacy): &bloomFilterFd,
		dataFilesetPathFromTimeAndIndex(shardDir, blockStart, volumeIndex, summariesFileSuffix, isLegacy):   &summariesFd,
	}); err != nil {
		return err
	}

	var (
		infoFdWithDigest           = resources.seekerOpenResources.infoFDDigestReader
		indexFdWithDigest          = resources.seekerOpenResources.indexFDDigestReader
		bloomFilterFdWithDigest    = resources.seekerOpenResources.bloomFilterFDDigestReader
		summariesFdWithDigest      = resources.seekerOpenResources.summariesFDDigestReader
		digestFdWithDigestContents = resources.seekerOpenResources.digestFDDigestContentsReader
	)
	defer func() {
		// NB(rartoul): We don't need to keep these FDs open as we use them up front.
		infoFdWithDigest.Close()
		bloomFilterFdWithDigest.Close()
		summariesFdWithDigest.Close()
		digestFdWithDigestContents.Close()
	}()

	infoFdWithDigest.Reset(infoFd)
	indexFdWithDigest.Reset(s.indexFd)
	summariesFdWithDigest.Reset(summariesFd)
	digestFdWithDigestContents.Reset(digestFd)

	expectedDigests, err := readFileSetDigests(digestFdWithDigestContents)
	if err != nil {
		// Try to close if failed to read
		s.Close()
		return err
	}

	infoStat, err := infoFd.Stat()
	if err != nil {
		s.Close()
		return err
	}

	info, err := s.readInfo(
		int(infoStat.Size()),
		infoFdWithDigest,
		expectedDigests.infoDigest,
		resources,
	)
	if err != nil {
		s.Close()
		return err
	}
	s.start = xtime.UnixNano(info.BlockStart)
	s.blockSize = time.Duration(info.BlockSize)
	s.versionChecker = schema.NewVersionChecker(int(info.MajorVersion), int(info.MinorVersion))

	err = s.validateIndexFileDigest(
		indexFdWithDigest, expectedDigests.indexDigest)
	if err != nil {
		s.Close()
		return fmt.Errorf(
			"index file digest for file: %s does not match the expected digest: %c",
			filesetPathFromTimeLegacy(shardDir, blockStart, indexFileSuffix), err,
		)
	}

	indexFdStat, err := s.indexFd.Stat()
	if err != nil {
		s.Close()
		return err
	}
	s.indexFileSize = indexFdStat.Size()

	s.bloomFilter, err = newManagedConcurrentBloomFilterFromFile(
		bloomFilterFd,
		bloomFilterFdWithDigest,
		expectedDigests.bloomFilterDigest,
		uint(info.BloomFilter.NumElementsM),
		uint(info.BloomFilter.NumHashesK),
		s.opts.opts.ForceBloomFilterMmapMemory(),
		mmap.ReporterOptions{
			Reporter: s.opts.opts.MmapReporter(),
		},
	)
	if err != nil {
		s.Close()
		return err
	}

	summariesFdWithDigest.Reset(summariesFd)
	s.indexLookup, err = newNearestIndexOffsetLookupFromSummariesFile(
		summariesFdWithDigest,
		expectedDigests.summariesDigest,
		resources.xmsgpackDecoder,
		resources.byteDecoderStream,
		int(info.Summaries.Summaries),
		s.opts.opts.ForceIndexSummariesMmapMemory(),
		mmap.ReporterOptions{
			Reporter: s.opts.opts.MmapReporter(),
		},
	)
	if err != nil {
		s.Close()
		return err
	}

	if !s.opts.keepUnreadBuf {
		// NB(r): Free the unread buffer and reset the decoder as unless
		// using this seeker in the seeker manager we never use this buffer again.
		s.unreadBuf = nil
	}

	return err
}

func (s *seeker) prepareUnreadBuf(size int) {
	if len(s.unreadBuf) < size {
		// NB(r): Make a little larger so unlikely to occur multiple times
		s.unreadBuf = make([]byte, int(1.5*float64(size)))
	}
}

func (s *seeker) unreadBuffer() []byte {
	return s.unreadBuf
}

func (s *seeker) setUnreadBuffer(buf []byte) {
	s.unreadBuf = buf
}

func (s *seeker) readInfo(
	size int,
	infoDigestReader digest.FdWithDigestReader,
	expectedInfoDigest uint32,
	resources ReusableSeekerResources,
) (schema.IndexInfo, error) {
	s.prepareUnreadBuf(size)
	n, err := infoDigestReader.ReadAllAndValidate(s.unreadBuf[:size], expectedInfoDigest)
	if err != nil {
		return schema.IndexInfo{}, err
	}

	resources.xmsgpackDecoder.Reset(xmsgpack.NewByteDecoderStream(s.unreadBuf[:n]))
	return resources.xmsgpackDecoder.DecodeIndexInfo()
}

// SeekByID returns the data for the specified ID. An error will be returned if the
// ID cannot be found.
func (s *seeker) SeekByID(id ident.ID, resources ReusableSeekerResources) (checked.Bytes, error) {
	entry, err := s.SeekIndexEntry(id, resources)
	if err != nil {
		return nil, err
	}

	return s.SeekByIndexEntry(entry, resources)
}

// SeekByIndexEntry is similar to Seek, but uses the provided IndexEntry
// instead of looking it up on its own. Useful in cases where you've already
// obtained an entry and don't want to waste resources looking it up again.
func (s *seeker) SeekByIndexEntry(
	entry IndexEntry,
	resources ReusableSeekerResources,
) (checked.Bytes, error) {
	resources.offsetFileReader.reset(s.dataFd, entry.Offset)

	// Obtain an appropriately sized buffer.
	var buffer checked.Bytes
	if s.opts.bytesPool != nil {
		buffer = s.opts.bytesPool.Get(int(entry.Size))
		buffer.IncRef()
		defer buffer.DecRef()
		buffer.Resize(int(entry.Size))
	} else {
		buffer = checked.NewBytes(make([]byte, entry.Size), nil)
		buffer.IncRef()
		defer buffer.DecRef()
	}

	// Copy the actual data into the underlying buffer.
	underlyingBuf := buffer.Bytes()
	n, err := io.ReadFull(resources.offsetFileReader, underlyingBuf)
	if err != nil {
		return nil, err
	}
	if n != int(entry.Size) {
		// This check is redundant because io.ReadFull will return an error if
		// its not able to read the specified number of bytes, but we keep it
		// in for posterity.
		return nil, fmt.Errorf("tried to read: %d bytes but read: %d", entry.Size, n)
	}

	// NB(r): _must_ check the checksum against known checksum as the data
	// file might not have been verified if we haven't read through the file yet.
	if entry.DataChecksum != digest.Checksum(underlyingBuf) {
		return nil, errSeekChecksumMismatch
	}

	return buffer, nil
}

// SeekIndexEntry performs the following steps:
//
//     1. Go to the indexLookup and it will give us an offset that is a good starting
//        point for scanning the index file.
//     2. Reset an offsetFileReader with the index fd and an offset (so that calls to Read() will
//        begin at the offset provided by the offset lookup).
//     3. Reset a decoder with fileDecoderStream (offsetFileReader wrapped in a bufio.Reader).
//     4. Call DecodeIndexEntry in a tight loop (which will advance our position in the
//        offsetFileReader internally) until we've either found the entry we're looking for or gone so
//        far we know it does not exist.
func (s *seeker) SeekIndexEntry(
	id ident.ID,
	resources ReusableSeekerResources,
) (IndexEntry, error) {
	offset, err := s.indexLookup.getNearestIndexFileOffset(id, resources)
	// Should never happen, either something is really wrong with the code or
	// the file on disk was corrupted.
	if err != nil {
		return IndexEntry{}, err
	}

	resources.offsetFileReader.reset(s.indexFd, offset)
	resources.fileDecoderStream.Reset(resources.offsetFileReader)
	resources.xmsgpackDecoder.Reset(resources.fileDecoderStream)

	idBytes := id.Bytes()
	for {
		// Use the bytesPool on resources here because its designed for this express purpose
		// and is much faster / cheaper than the checked bytes pool which has a lot of
		// synchronization and is prone to allocation (due to being shared). Basically because
		// this is a tight loop (scanning linearly through the index file) we want to use a
		// very cheap pool until we find what we're looking for, and then we can perform a single
		// copy into checked.Bytes from the more expensive pool.
		entry, err := resources.xmsgpackDecoder.DecodeIndexEntry(resources.decodeIndexEntryBytesPool)
		if err == io.EOF {
			// We reached the end of the file without finding it.
			return IndexEntry{}, errSeekIDNotFound
		}
		if err != nil {
			// Should never happen, either something is really wrong with the code or
			// the file on disk was corrupted.
			return IndexEntry{}, instrument.InvariantErrorf(err.Error())
		}
		if entry.ID == nil {
			// Should never happen, either something is really wrong with the code or
			// the file on disk was corrupted.
			return IndexEntry{},
				instrument.InvariantErrorf("decoded index entry had no ID for: %s", id.String())
		}

		comparison := bytes.Compare(entry.ID, idBytes)
		if comparison == 0 {
			// If it's a match, we need to copy the tags into a checked bytes
			// so they can be passed along. We use the "real" bytes pool here
			// because we're passing ownership of the bytes to the entry / caller.
			var checkedEncodedTags checked.Bytes
			if len(entry.EncodedTags) > 0 {
				checkedEncodedTags = s.opts.bytesPool.Get(len(entry.EncodedTags))
				checkedEncodedTags.IncRef()
				checkedEncodedTags.AppendAll(entry.EncodedTags)
			}

			indexEntry := IndexEntry{
				Size:         uint32(entry.Size),
				DataChecksum: uint32(entry.DataChecksum),
				Offset:       entry.Offset,
				EncodedTags:  checkedEncodedTags,
			}

			// Safe to return resources to the pool because ID will not be
			// passed along and tags have been copied.
			resources.decodeIndexEntryBytesPool.Put(entry.ID)
			resources.decodeIndexEntryBytesPool.Put(entry.EncodedTags)

			return indexEntry, nil
		}

		// No longer being used so we can return to the pool.
		resources.decodeIndexEntryBytesPool.Put(entry.ID)
		resources.decodeIndexEntryBytesPool.Put(entry.EncodedTags)

		// We've scanned far enough through the index file to be sure that the ID
		// we're looking for doesn't exist (because the index is sorted by ID)
		if comparison == 1 {
			return IndexEntry{}, errSeekIDNotFound
		}
	}
}

// SeekWideEntry performs the following steps:
//
//     1. Go to the indexLookup and it will give us an offset that is a good starting
//        point for scanning the index file.
//     2. Reset an offsetFileReader with the index fd and an offset (so that calls to Read() will
//        begin at the offset provided by the offset lookup).
//     3. Reset a decoder with fileDecoderStream (offsetFileReader wrapped in a bufio.Reader).
//     4. Call DecodeToWideEntry in a tight loop (which will advance our position in the
//        offsetFileReader internally) until we've either found the entry we're looking for or gone so
//        far we know it does not exist.
func (s *seeker) SeekWideEntry(
	id ident.ID,
	filter schema.WideEntryFilter,
	resources ReusableSeekerResources,
) (xio.WideEntry, error) {
	offset, err := s.indexLookup.getNearestIndexFileOffset(id, resources)
	// Should never happen, either something is really wrong with the code or
	// the file on disk was corrupted.
	if err != nil {
		return xio.WideEntry{}, err
	}

	resources.offsetFileReader.reset(s.indexFd, offset)
	resources.fileDecoderStream.Reset(resources.offsetFileReader)
	resources.xmsgpackDecoder.Reset(resources.fileDecoderStream)

	idBytes := id.Bytes()
	for {
		entry, status, err := resources.xmsgpackDecoder.
			DecodeToWideEntry(idBytes, resources.decodeIndexEntryBytesPool)
		if err != nil {
			// No longer being used so we can return to the pool.
			resources.decodeIndexEntryBytesPool.Put(entry.ID)
			resources.decodeIndexEntryBytesPool.Put(entry.EncodedTags)

			if err == io.EOF {
				// Reached the end of the file without finding the ID.
				return xio.WideEntry{}, errSeekIDNotFound
			}
			// Should never happen, either something is really wrong with the code or
			// the file on disk was corrupted.
			return xio.WideEntry{}, instrument.InvariantErrorf(err.Error())
		}

		if filter != nil {
			filtered, err := filter(entry)
			if err != nil || filtered {
				// NB: this entry is not being taken, can free memory.
				resources.decodeIndexEntryBytesPool.Put(entry.ID)
				resources.decodeIndexEntryBytesPool.Put(entry.EncodedTags)
				return xio.WideEntry{}, err
			}
		}

		if status != xmsgpack.MatchedLookupStatus {
			// No longer being used so we can return to the pool.
			resources.decodeIndexEntryBytesPool.Put(entry.ID)
			resources.decodeIndexEntryBytesPool.Put(entry.EncodedTags)

			if status == xmsgpack.NotFoundLookupStatus {
				// a `NotFound` status for the wide entry decode indicates that the
				// current seek has passed the point in the file where this ID could have
				// appeared; short-circuit here as the ID does not exist in the file.
				return xio.WideEntry{}, errSeekIDNotFound
			} else if status == xmsgpack.MismatchLookupStatus {
				// a `Mismatch` status for the wide entry decode indicates that the
				// current seek does not match the ID, but that it may still appear in
				// the file.
				continue
			} else if status == xmsgpack.ErrorLookupStatus {
				return xio.WideEntry{}, errors.New("unknown index lookup error")
			}
		}

		// If it's a match, we need to copy the tags into a checked bytes
		// so they can be passed along. We use the "real" bytes pool here
		// because we're passing ownership of the bytes to the entry / caller.
		var checkedEncodedTags checked.Bytes
		if tags := entry.EncodedTags; len(tags) > 0 {
			checkedEncodedTags = s.opts.bytesPool.Get(len(tags))
			checkedEncodedTags.IncRef()
			checkedEncodedTags.AppendAll(tags)
		}

		// No longer being used so we can return to the pool.
		resources.decodeIndexEntryBytesPool.Put(entry.ID)
		resources.decodeIndexEntryBytesPool.Put(entry.EncodedTags)

		return xio.WideEntry{
			ID:               id,
			Size:             entry.Size,
			Offset:           entry.Offset,
			DataChecksum:     entry.DataChecksum,
			EncodedTags:      checkedEncodedTags,
			MetadataChecksum: entry.MetadataChecksum,
		}, nil
	}
}

func (s *seeker) Range() xtime.Range {
	return xtime.Range{Start: s.start.ToTime(), End: s.start.ToTime().Add(s.blockSize)}
}

func (s *seeker) Close() error {
	// Parent should handle cleaning up shared resources
	if s.isClone {
		return nil
	}

	multiErr := xerrors.NewMultiError()
	if s.bloomFilter != nil {
		multiErr = multiErr.Add(s.bloomFilter.Close())
		s.bloomFilter = nil
	}
	if s.indexLookup != nil {
		multiErr = multiErr.Add(s.indexLookup.close())
		s.indexLookup = nil
	}
	if s.indexFd != nil {
		multiErr = multiErr.Add(s.indexFd.Close())
		s.indexFd = nil
	}
	if s.dataFd != nil {
		multiErr = multiErr.Add(s.dataFd.Close())
		s.dataFd = nil
	}
	return multiErr.FinalError()
}

func (s *seeker) ConcurrentClone() (ConcurrentDataFileSetSeeker, error) {
	// IndexLookup is not concurrency safe, but a parent and its clone can be used
	// concurrently safely.
	indexLookupClone, err := s.indexLookup.concurrentClone()
	if err != nil {
		return nil, err
	}

	seeker := &seeker{
		opts:          s.opts,
		indexFileSize: s.indexFileSize,
		// BloomFilter is concurrency safe.
		bloomFilter: s.bloomFilter,
		indexLookup: indexLookupClone,
		isClone:     true,

		// Index and data fd's are always accessed via the ReadAt() / pread APIs so
		// they are concurrency safe and can be shared among clones.
		indexFd: s.indexFd,
		dataFd:  s.dataFd,

		versionChecker: s.versionChecker,
	}

	return seeker, nil
}

func (s *seeker) validateIndexFileDigest(
	indexFdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
) error {
	// If piecemeal checksumming validation enabled for index entries, do not attempt to validate the
	// checksum of the entire file
	if s.versionChecker.IndexEntryValidationEnabled() {
		return nil
	}

	buf := make([]byte, s.opts.dataBufferSize)
	for {
		n, err := indexFdWithDigest.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error reading index file: %v", err)
		}
		if n == 0 || err == io.EOF {
			break
		}
	}
	return indexFdWithDigest.Validate(expectedDigest)
}

// ReusableSeekerResources is a collection of reusable resources
// that the seeker requires for seeking. It can be pooled by callers
// using the seeker so that expensive resources don't need to be
// maintained for each seeker, especially when only a few are generally
// being used at a time due to the FetchConcurrency.
type ReusableSeekerResources struct {
	msgpackDecoder    *msgpack.Decoder
	xmsgpackDecoder   *xmsgpack.Decoder
	fileDecoderStream *bufio.Reader
	byteDecoderStream xmsgpack.ByteDecoderStream
	offsetFileReader  *offsetFileReader
	// This pool should only be used for calling DecodeIndexEntry. We use a
	// special pool here to avoid the overhead of channel synchronization, as
	// well as ref counting that comes with the checked bytes pool. In addition,
	// since the ReusableSeekerResources is only ever used by a single seeker at
	// a time, we can size this pool such that it almost never has to allocate.
	decodeIndexEntryBytesPool pool.BytesPool

	seekerOpenResources reusableSeekerOpenResources
}

// reusableSeekerOpenResources contains resources used for the Open() method of the seeker.
type reusableSeekerOpenResources struct {
	infoFDDigestReader           digest.FdWithDigestReader
	indexFDDigestReader          digest.FdWithDigestReader
	bloomFilterFDDigestReader    digest.FdWithDigestReader
	summariesFDDigestReader      digest.FdWithDigestReader
	digestFDDigestContentsReader digest.FdWithDigestContentsReader
}

func newReusableSeekerOpenResources(opts Options) reusableSeekerOpenResources {
	return reusableSeekerOpenResources{
		infoFDDigestReader:           digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
		indexFDDigestReader:          digest.NewFdWithDigestReader(opts.DataReaderBufferSize()),
		bloomFilterFDDigestReader:    digest.NewFdWithDigestReader(opts.DataReaderBufferSize()),
		summariesFDDigestReader:      digest.NewFdWithDigestReader(opts.DataReaderBufferSize()),
		digestFDDigestContentsReader: digest.NewFdWithDigestContentsReader(opts.InfoReaderBufferSize()),
	}
}

// NewReusableSeekerResources creates a new ReusableSeekerResources.
func NewReusableSeekerResources(opts Options) ReusableSeekerResources {
	seekReaderSize := opts.SeekReaderBufferSize()
	return ReusableSeekerResources{
		msgpackDecoder:            msgpack.NewDecoder(nil),
		xmsgpackDecoder:           xmsgpack.NewDecoder(opts.DecodingOptions()),
		fileDecoderStream:         bufio.NewReaderSize(nil, seekReaderSize),
		byteDecoderStream:         xmsgpack.NewByteDecoderStream(nil),
		offsetFileReader:          newOffsetFileReader(),
		decodeIndexEntryBytesPool: newSimpleBytesPool(),
		seekerOpenResources:       newReusableSeekerOpenResources(opts),
	}
}

type simpleBytesPool struct {
	pool             [][]byte
	maxByteSliceSize int
	maxPoolSize      int
}

func newSimpleBytesPool() pool.BytesPool {
	s := &simpleBytesPool{
		maxByteSliceSize: maxSimpleBytesPoolSliceSize,
		maxPoolSize:      maxSimpleBytesPoolSize,
	}
	s.Init()
	return s
}

func (s *simpleBytesPool) Init() {
	for i := 0; i < s.maxPoolSize; i++ {
		s.pool = append(s.pool, make([]byte, 0, s.maxByteSliceSize))
	}
}

func (s *simpleBytesPool) Get(capacity int) []byte {
	if len(s.pool) == 0 {
		return make([]byte, 0, capacity)
	}

	lastIdx := len(s.pool) - 1
	b := s.pool[lastIdx]

	if cap(b) >= capacity {
		// If the slice has enough capacity, remove it from the
		// pool and return it to the caller.
		s.pool = s.pool[:lastIdx]
		return b
	}

	return make([]byte, 0, capacity)
}

func (s *simpleBytesPool) Put(b []byte) {
	if b == nil ||
		len(s.pool) >= s.maxPoolSize ||
		cap(b) > s.maxByteSliceSize {
		return
	}

	s.pool = append(s.pool, b[:])
}

var _ io.Reader = &offsetFileReader{}

// offsetFileReader implements io.Reader() and allows an *os.File to be wrapped
// such that any calls to Read() are issued at the provided offset. This is used
// to issue reads to specific portions of the index and data files without having
// to first call Seek(). This reduces the number of syscalls that need to be made
// and also allows the fds to be shared among concurrent goroutines since the
// internal F.D offset managed by the kernel is not being used.
type offsetFileReader struct {
	fd     *os.File
	offset int64
}

func newOffsetFileReader() *offsetFileReader {
	return &offsetFileReader{}
}

func (p *offsetFileReader) Read(b []byte) (n int, err error) {
	n, err = p.fd.ReadAt(b, p.offset)
	p.offset += int64(n)
	return n, err
}

func (p *offsetFileReader) reset(fd *os.File, offset int64) {
	p.fd = fd
	p.offset = offset
}
