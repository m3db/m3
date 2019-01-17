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
	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	// errSeekIDNotFound returned when ID cannot be found in the shard
	errSeekIDNotFound = errors.New("id not found in shard")

	// errSeekChecksumMismatch returned when data checksum does not match the expected checksum
	errSeekChecksumMismatch = errors.New("checksum does not match expected checksum")

	// errInvalidDataFileOffset returned when the provided offset into the data file is not valid
	errInvalidDataFileOffset = errors.New("invalid data file offset")

	// errNotEnoughBytes returned when the data file doesn't have enough bytes to satisfy a read
	errNotEnoughBytes = errors.New("invalid data file, not enough bytes to satisfy read")

	// errClonesShouldNotBeOpened returned when Open() is called on a clone
	errClonesShouldNotBeOpened = errors.New("clone should not be opened")

	// errDecodedIndexEntryHadNoID is returned when the decoded index entry has no idea.
	errDecodedIndexEntryHadNoID = errors.New("decoded index entry had no ID")
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
	start     xtime.UnixNano
	blockSize time.Duration

	dataFd        *os.File
	indexFd       *os.File
	indexFileSize int64

	shardDir string

	unreadBuf []byte

	// Bloom filter associated with the shard / block the seeker is responsible
	// for. Needs to be closed when done.
	bloomFilter *ManagedConcurrentBloomFilter
	indexLookup *nearestIndexOffsetLookup

	isClone bool
}

// IndexEntry is an entry from the index file which can be passed to
// SeekUsingIndexEntry to seek to the data for that entry
type IndexEntry struct {
	Size        uint32
	Checksum    uint32
	Offset      int64
	EncodedTags checked.Bytes
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
	resources ReusableSeekerResources,
) error {
	if s.isClone {
		return errClonesShouldNotBeOpened
	}

	shardDir := ShardDataDirPath(s.opts.filePathPrefix, namespace, shard)
	s.shardDir = shardDir
	var infoFd, digestFd, bloomFilterFd, summariesFd *os.File

	// Open necessary files
	if err := openFiles(os.Open, map[string]**os.File{
		filesetPathFromTime(shardDir, blockStart, infoFileSuffix):        &infoFd,
		filesetPathFromTime(shardDir, blockStart, indexFileSuffix):       &s.indexFd,
		filesetPathFromTime(shardDir, blockStart, dataFileSuffix):        &s.dataFd,
		filesetPathFromTime(shardDir, blockStart, digestFileSuffix):      &digestFd,
		filesetPathFromTime(shardDir, blockStart, bloomFilterFileSuffix): &bloomFilterFd,
		filesetPathFromTime(shardDir, blockStart, summariesFileSuffix):   &summariesFd,
	}); err != nil {
		return err
	}

	// Setup digest readers
	var (
		infoFdWithDigest           = digest.NewFdWithDigestReader(s.opts.infoBufferSize)
		indexFdWithDigest          = digest.NewFdWithDigestReader(s.opts.dataBufferSize)
		bloomFilterFdWithDigest    = digest.NewFdWithDigestReader(s.opts.dataBufferSize)
		summariesFdWithDigest      = digest.NewFdWithDigestReader(s.opts.dataBufferSize)
		digestFdWithDigestContents = digest.NewFdWithDigestContentsReader(s.opts.infoBufferSize)
	)
	defer func() {
		// NB(rartoul): We don't need to keep these FDs open as we use these up front
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

	err = s.validateIndexFileDigest(
		indexFdWithDigest, expectedDigests.indexDigest)
	if err != nil {
		s.Close()
		return fmt.Errorf(
			"index file digest for file: %s does not match the expected digest: %c",
			filesetPathFromTime(shardDir, blockStart, indexFileSuffix), err,
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
		int(info.Summaries.Summaries),
		s.opts.opts.ForceIndexSummariesMmapMemory(),
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
	newOffset, err := s.dataFd.Seek(entry.Offset, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	if newOffset != entry.Offset {
		return nil, fmt.Errorf("tried to seek to: %d, but seeked to: %d", entry.Offset, newOffset)
	}

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
	n, err := io.ReadFull(s.dataFd, underlyingBuf)
	if err != nil {
		return nil, err
	}
	if n != int(entry.Size) {
		return nil, fmt.Errorf("tried to read: %d bytes but read: %d", entry.Size, n)
	}

	// NB(r): _must_ check the checksum against known checksum as the data
	// file might not have been verified if we haven't read through the file yet.
	if entry.Checksum != digest.Checksum(underlyingBuf) {
		return nil, errSeekChecksumMismatch
	}

	return buffer, nil
}

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

	seekedOffset, err := s.indexFd.Seek(offset, os.SEEK_SET)
	if err != nil {
		return IndexEntry{}, err
	}
	if seekedOffset != offset {
		return IndexEntry{}, fmt.Errorf("tried to seek to offset: %d, but seeked to: %d", seekedOffset, offset)
	}

	resources.fileDecoderStream.Reset(s.indexFd)
	resources.xmsgpackDecoder.Reset(resources.fileDecoderStream)

	idBytes := id.Bytes()
	for {
		// Prevent panics when we're scanning to the end of the buffer.
		currOffset, err := s.indexFd.Seek(0, os.SEEK_CUR)
		if err != nil {
			return IndexEntry{}, err
		}
		if currOffset >= s.indexFileSize && resources.fileDecoderStream.Buffered() <= 0 {
			return IndexEntry{}, errSeekIDNotFound
		}

		entry, err := resources.xmsgpackDecoder.DecodeIndexEntry(resources.bytesPool)
		if err != nil {
			// Should never happen, either something is really wrong with the code or
			// the file on disk was corrupted.
			return IndexEntry{}, err
		}
		if entry.ID == nil {
			// Should never happen, either something is really wrong with the code or
			// the file on disk was corrupted.
			return IndexEntry{}, errDecodedIndexEntryHadNoID
		}

		comparison := bytes.Compare(entry.ID, idBytes)
		if comparison == 0 {
			// If it's a match, we need to copy the tags into a checked bytes
			// so they can be passed along.
			checkedBytes := s.opts.bytesPool.Get(len(entry.EncodedTags))
			checkedBytes.IncRef()
			checkedBytes.AppendAll(entry.EncodedTags)

			indexEntry := IndexEntry{
				Size:        uint32(entry.Size),
				Checksum:    uint32(entry.Checksum),
				Offset:      entry.Offset,
				EncodedTags: checkedBytes,
			}

			// Safe to return resources to the pool because ID will not be
			// passed along and tags have been copied.
			resources.bytesPool.Put(entry.ID)
			resources.bytesPool.Put(entry.EncodedTags)

			return indexEntry, nil
		}

		// No longer being used so we can return to the pool.
		resources.bytesPool.Put(entry.ID)
		resources.bytesPool.Put(entry.EncodedTags)

		// We've scanned far enough through the index file to be sure that the ID
		// we're looking for doesn't exist (because the index is sorted by ID)
		if comparison == 1 {
			return IndexEntry{}, errSeekIDNotFound
		}
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
	}

	// File descriptors are not concurrency safe since they have an internal
	// seek position.
	if err := openFiles(os.Open, map[string]**os.File{
		filesetPathFromTime(s.shardDir, s.start.ToTime(), indexFileSuffix): &seeker.indexFd,
		filesetPathFromTime(s.shardDir, s.start.ToTime(), dataFileSuffix):  &seeker.dataFd,
	}); err != nil {
		return nil, err
	}

	return seeker, nil
}

func (s *seeker) validateIndexFileDigest(
	indexFdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
) error {
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
	bytesPool         pool.BytesPool
}

// NewReusableSeekerResources creates a new ReusableSeekerResources.
func NewReusableSeekerResources(opts Options) ReusableSeekerResources {
	seekReaderSize := opts.SeekReaderBufferSize()
	return ReusableSeekerResources{
		msgpackDecoder:    msgpack.NewDecoder(nil),
		xmsgpackDecoder:   xmsgpack.NewDecoder(opts.DecodingOptions()),
		fileDecoderStream: bufio.NewReaderSize(nil, seekReaderSize),
		byteDecoderStream: xmsgpack.NewByteDecoderStream(nil),
		bytesPool:         newSimpleBytesPool(),
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
		// One for the ID and one for the tags.
		maxPoolSize: maxSimpleBytesPoolSize,
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
