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
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
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
)

type seeker struct {
	opts           seekerOpts
	filePathPrefix string

	// Data read from the indexInfo file
	start           time.Time
	blockSize       time.Duration
	entries         int
	bloomFilterInfo schema.IndexBloomFilterInfo
	summariesInfo   schema.IndexSummariesInfo

	dataFd        *os.File
	indexFd       *os.File
	indexFileSize int64
	buffReader    *buffReaderWithSkip

	dataFilePath  string
	indexFilePath string

	unreadBuf []byte

	decoder      *msgpack.Decoder
	decodingOpts msgpack.DecodingOptions
	bytesPool    pool.CheckedBytesPool

	// Bloom filter associated with the shard / block the seeker is responsible
	// for. Needs to be closed when done.
	bloomFilter *ManagedConcurrentBloomFilter
	indexLookup *nearestIndexOffsetLookup

	keepUnreadBuf bool

	isClone bool
}

// IndexEntry is an entry from the index file which can be passed to
// SeekUsingIndexEntry to seek to the data for that entry
type IndexEntry struct {
	Size        uint32
	Checksum    uint32
	Offset      int64
	EncodedTags []byte
}

// NewSeeker returns a new seeker.
func NewSeeker(
	filePathPrefix string,
	dataBufferSize int,
	infoBufferSize int,
	seekBufferSize int,
	bytesPool pool.CheckedBytesPool,
	keepUnreadBuf bool,
	decodingOpts msgpack.DecodingOptions,
	opts Options,
) DataFileSetSeeker {
	return newSeeker(seekerOpts{
		filePathPrefix: filePathPrefix,
		dataBufferSize: dataBufferSize,
		infoBufferSize: infoBufferSize,
		seekBufferSize: seekBufferSize,
		bytesPool:      bytesPool,
		keepUnreadBuf:  keepUnreadBuf,
		decodingOpts:   decodingOpts,
		opts:           opts,
	})
}

type seekerOpts struct {
	filePathPrefix string
	infoBufferSize int
	dataBufferSize int
	seekBufferSize int
	bytesPool      pool.CheckedBytesPool
	keepUnreadBuf  bool
	decodingOpts   msgpack.DecodingOptions
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
	if opts.decodingOpts == nil {
		opts.decodingOpts = msgpack.NewDecodingOptions()
	}
	decodingOpts := opts.decodingOpts.
		SetCheckedBytesPool(opts.bytesPool)

	return &seeker{
		filePathPrefix: opts.filePathPrefix,
		keepUnreadBuf:  opts.keepUnreadBuf,
		bytesPool:      opts.bytesPool,
		decoder:        msgpack.NewDecoder(decodingOpts),
		buffReader:     &buffReaderWithSkip{bufio.NewReader(nil), nil},
		decodingOpts:   opts.decodingOpts,
		opts:           opts,
	}
}

func (s *seeker) ConcurrentIDBloomFilter() *ManagedConcurrentBloomFilter {
	return s.bloomFilter
}

func (s *seeker) Open(namespace ident.ID, shard uint32, blockStart time.Time) error {
	if s.isClone {
		return errClonesShouldNotBeOpened
	}

	shardDir := ShardDataDirPath(s.filePathPrefix, namespace, shard)
	var infoFd, digestFd, bloomFilterFd, summariesFd *os.File

	s.indexFilePath = filesetPathFromTime(shardDir, blockStart, indexFileSuffix)
	s.dataFilePath = filesetPathFromTime(shardDir, blockStart, dataFileSuffix)
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
		// indexFdWithDigest.Close()
		bloomFilterFdWithDigest.Close()
		summariesFdWithDigest.Close()
		digestFdWithDigestContents.Close()
		// dataFd.Close()
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
	if err := s.readInfo(int(infoStat.Size()), infoFdWithDigest, expectedDigests.infoDigest); err != nil {
		s.Close()
		return err
	}

	indexFdStat, err := s.indexFd.Stat()
	if err != nil {
		s.Close()
		return err
	}
	s.indexFileSize = indexFdStat.Size()
	s.buffReader.file = s.indexFd

	buf := make([]byte, 4096)
	for {
		n, err := indexFdWithDigest.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error reading index file: %v", err)
		}
		if n == 0 || err == io.EOF {
			break
		}
	}
	err = indexFdWithDigest.Validate(expectedDigests.indexDigest)
	if err != nil {
		s.Close()
		return fmt.Errorf(
			"index file digest for file: %s does not match the expected digest: %c",
			filesetPathFromTime(shardDir, blockStart, indexFileSuffix), err,
		)
	}

	s.bloomFilter, err = newManagedConcurrentBloomFilterFromFile(
		bloomFilterFd,
		bloomFilterFdWithDigest,
		expectedDigests.bloomFilterDigest,
		uint(s.bloomFilterInfo.NumElementsM),
		uint(s.bloomFilterInfo.NumHashesK),
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
		s.decoder,
		int(s.summariesInfo.Summaries),
		s.opts.opts.ForceIndexSummariesMmapMemory(),
	)
	if err != nil {
		s.Close()
		return err
	}

	if !s.keepUnreadBuf {
		// NB(r): Free the unread buffer and reset the decoder as unless
		// using this seeker in the seeker manager we never use this buffer again
		s.unreadBuf = nil
		s.decoder.Reset(nil)
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

func (s *seeker) readInfo(size int, infoDigestReader digest.FdWithDigestReader, expectedInfoDigest uint32) error {
	s.prepareUnreadBuf(size)
	n, err := infoDigestReader.ReadAllAndValidate(s.unreadBuf[:size], expectedInfoDigest)
	if err != nil {
		return err
	}

	s.decoder.Reset(msgpack.NewDecoderStream(s.unreadBuf[:n]))
	info, err := s.decoder.DecodeIndexInfo()
	if err != nil {
		return err
	}

	s.start = xtime.FromNanoseconds(info.BlockStart)
	s.blockSize = time.Duration(info.BlockSize)
	s.entries = int(info.Entries)
	s.bloomFilterInfo = info.BloomFilter
	s.summariesInfo = info.Summaries

	return nil
}

// SeekByID returns the data for the specified ID. An error will be returned if the
// ID cannot be found.
func (s *seeker) SeekByID(id ident.ID) (checked.Bytes, error) {
	entry, err := s.SeekIndexEntry(id)
	if err != nil {
		return nil, err
	}

	return s.SeekByIndexEntry(entry)
}

// SeekByIndexEntry is similar to Seek, but uses the provided IndexEntry
// instead of looking it up on its own. Useful in cases where you've already
// obtained an entry and don't want to waste resources looking it up again.
func (s *seeker) SeekByIndexEntry(entry IndexEntry) (checked.Bytes, error) {
	newOffset, err := s.dataFd.Seek(entry.Offset, 0)
	if err != nil {
		return nil, err
	}
	if newOffset != entry.Offset {
		return nil, fmt.Errorf("tried to seek to: %d, but seeked to: %d", entry.Offset, newOffset)
	}

	// Obtain an appropriately sized buffer
	var buffer checked.Bytes
	if s.bytesPool != nil {
		buffer = s.bytesPool.Get(int(entry.Size))
		buffer.IncRef()
		defer buffer.DecRef()
		buffer.Resize(int(entry.Size))
	} else {
		buffer = checked.NewBytes(make([]byte, entry.Size), nil)
		buffer.IncRef()
		defer buffer.DecRef()
	}

	// Copy the actual data into the underlying buffer
	underlyingBuf := buffer.Bytes()
	n, err := s.dataFd.Read(underlyingBuf)
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

func (s *seeker) SeekIndexEntry(id ident.ID) (IndexEntry, error) {
	offset, err := s.indexLookup.getNearestIndexFileOffset(id)
	// Should never happen, either something is really wrong with the code or
	// the file on disk was corrupted
	if err != nil {
		return IndexEntry{}, err
	}

	seekedOffset, err := s.indexFd.Seek(offset, 0)
	if err != nil {
		return IndexEntry{}, err
	}
	if seekedOffset != offset {
		return IndexEntry{}, fmt.Errorf("tried to seek to offset: %d, but seeked to: %d", seekedOffset, offset)
	}

	s.buffReader.Reset(s.indexFd)
	s.decoder.Reset(s.buffReader)

	idBytes := id.Bytes()
	for {
		// Prevent panic's when we're scanning to the end of the buffer
		currOffset, err := s.indexFd.Seek(0, 1)
		if err != nil {
			return IndexEntry{}, err
		}
		if currOffset >= s.indexFileSize && s.buffReader.Buffered() <= 0 {
			return IndexEntry{}, errSeekIDNotFound
		}
		entry, err := s.decoder.DecodeIndexEntry()
		// Should never happen, either something is really wrong with the code or
		// the file on disk was corrupted
		if err != nil {
			return IndexEntry{}, err
		}
		comparison := bytes.Compare(entry.CheckedID.Bytes(), idBytes)
		entry.CheckedID.DecRef()
		entry.CheckedID.Finalize()
		if comparison == 0 {
			return IndexEntry{
				Size:        uint32(entry.Size),
				Checksum:    uint32(entry.Checksum),
				Offset:      entry.Offset,
				EncodedTags: entry.EncodedTags,
			}, nil
		}

		// We've scanned far enough through the index file to be sure that the ID
		// we're looking for doesn't exist (because the index is sorted by ID)
		if comparison == 1 {
			return IndexEntry{}, errSeekIDNotFound
		}
	}

	// Similar to the case above where comparison == 1, except in this case we're
	// sure that the ID we're looking for doesn't exist because we reached the end
	// of the index file.
	return IndexEntry{}, errSeekIDNotFound
}

func (s *seeker) Range() xtime.Range {
	return xtime.Range{Start: s.start, End: s.start.Add(s.blockSize)}
}

func (s *seeker) Entries() int {
	return s.entries
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
	// indexLookup is not concurrency safe, but a parent and its clone can be used
	// concurrently safely.
	indexLookupClone, err := s.indexLookup.concurrentClone()
	if err != nil {
		return nil, err
	}

	decodingOpts := s.decodingOpts.
		SetCheckedBytesPool(s.bytesPool)
	seeker := &seeker{
		// Bare-minimum required fields for a clone to function properly
		bytesPool:     s.bytesPool,
		decoder:       msgpack.NewDecoder(decodingOpts),
		opts:          s.opts,
		indexFileSize: s.indexFileSize,
		// bloomFilter is concurrency safe
		bloomFilter: s.bloomFilter,
		indexLookup: indexLookupClone,
		isClone:     true,
	}

	// Open necessary files
	if err := openFiles(os.Open, map[string]**os.File{
		s.indexFilePath: &seeker.indexFd,
		s.dataFilePath:  &seeker.dataFd,
	}); err != nil {
		return nil, err
	}

	seeker.buffReader = &buffReaderWithSkip{
		bufio.NewReader(nil),
		seeker.indexFd,
	}

	return seeker, nil
}

type buffReaderWithSkip struct {
	*bufio.Reader
	file *os.File
}

func (y *buffReaderWithSkip) Skip(n int64) error {
	_, err := y.Discard(int(n))
	if err != nil {
		return err
	}
	return nil
}

func (y *buffReaderWithSkip) Offset() (int, error) {
	offset, err := y.file.Seek(0, 1)
	if err != nil {
		return -1, err
	}

	return int(offset - int64(y.Buffered())), nil
}
