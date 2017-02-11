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
	"io"
	"os"
	"time"

	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/m3db/m3db/digest"
)

var (
	// errSeekIDNotFound returned when ID cannot be found in the shard
	errSeekIDNotFound = errors.New("id not found in shard")
)

type seeker struct {
	filePathPrefix string
	start          time.Time
	blockSize      time.Duration

	infoFdWithDigest           digest.FdWithDigestReader
	indexFdWithDigest          digest.FdWithDigestReader
	dataReader                 *bufio.Reader
	digestFdWithDigestContents digest.FdWithDigestContentsReader
	expectedInfoDigest         uint32
	expectedIndexDigest        uint32

	keepIndexIDs  bool
	keepUnreadBuf bool

	unreadBuf []byte
	prologue  []byte
	entries   int
	dataFd    *os.File
	// NB(r): specifically use a non pointer type for
	// key and value in this map to avoid the GC scanning
	// this large map.
	indexMap  map[ts.Hash]indexMapEntry
	indexIDs  []ts.ID
	decoder   encoding.Decoder
	bytesPool pool.CheckedBytesPool
}

type indexMapEntry struct {
	size   int64
	offset int64
}

// NewSeeker returns a new seeker.
func NewSeeker(
	filePathPrefix string,
	bufferSize int,
	bytesPool pool.CheckedBytesPool,
	decodingOpts msgpack.DecodingOptions,
) FileSetSeeker {
	return newSeeker(seekerOpts{
		filePathPrefix: filePathPrefix,
		bufferSize:     bufferSize,
		bytesPool:      bytesPool,
		keepIndexIDs:   true,
		keepUnreadBuf:  false,
		decodingOpts:   decodingOpts,
	})
}

type seekerOpts struct {
	filePathPrefix string
	bufferSize     int
	bytesPool      pool.CheckedBytesPool
	keepIndexIDs   bool
	keepUnreadBuf  bool
	decodingOpts   msgpack.DecodingOptions
}

// fileSetSeeker adds package level access to further methods
// on the seeker for use by the seeker manager for efficient
// multi-seeker use.
type fileSetSeeker interface {
	FileSetSeeker

	// unreadBuffer returns the unread buffer
	unreadBuffer() []byte

	// setUnreadBuffer sets the unread buffer
	setUnreadBuffer(buf []byte)
}

func newSeeker(opts seekerOpts) fileSetSeeker {
	return &seeker{
		filePathPrefix:             opts.filePathPrefix,
		infoFdWithDigest:           digest.NewFdWithDigestReader(opts.bufferSize),
		indexFdWithDigest:          digest.NewFdWithDigestReader(opts.bufferSize),
		dataReader:                 bufio.NewReaderSize(nil, opts.bufferSize),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsReader(opts.bufferSize),
		keepIndexIDs:               opts.keepIndexIDs,
		keepUnreadBuf:              opts.keepUnreadBuf,
		prologue:                   make([]byte, markerLen+idxLen),
		bytesPool:                  opts.bytesPool,
		decoder:                    msgpack.NewDecoder(opts.decodingOpts),
	}
}

func (s *seeker) IDs() []ts.ID {
	return s.indexIDs
}

func (s *seeker) Open(namespace ts.ID, shard uint32, blockStart time.Time) error {
	shardDir := ShardDirPath(s.filePathPrefix, namespace, shard)
	var infoFd, indexFd, dataFd, digestFd *os.File
	if err := openFiles(os.Open, map[string]**os.File{
		filesetPathFromTime(shardDir, blockStart, infoFileSuffix):   &infoFd,
		filesetPathFromTime(shardDir, blockStart, indexFileSuffix):  &indexFd,
		filesetPathFromTime(shardDir, blockStart, dataFileSuffix):   &dataFd,
		filesetPathFromTime(shardDir, blockStart, digestFileSuffix): &digestFd,
	}); err != nil {
		return err
	}

	s.infoFdWithDigest.Reset(infoFd)
	s.indexFdWithDigest.Reset(indexFd)
	s.digestFdWithDigestContents.Reset(digestFd)

	defer func() {
		// NB(r): We don't need to keep these FDs open as we use these up front
		s.infoFdWithDigest.Close()
		s.indexFdWithDigest.Close()
		s.digestFdWithDigestContents.Close()
	}()

	if err := s.readDigest(); err != nil {
		// Try to close if failed to read
		s.Close()
		return err
	}
	infoStat, err := infoFd.Stat()
	if err != nil {
		s.Close()
		return err
	}
	indexStat, err := indexFd.Stat()
	if err != nil {
		s.Close()
		return err
	}
	if err := s.readInfo(int(infoStat.Size())); err != nil {
		s.Close()
		return err
	}
	if err := s.readIndex(int(indexStat.Size())); err != nil {
		s.Close()
		return err
	}

	if !s.keepUnreadBuf {
		// NB(r): Free the unread buffer and reset the decoder as unless
		// using this seeker in the seeker manager we never use this buffer again
		s.unreadBuf = nil
		s.decoder.Reset(nil)
	}

	s.dataFd = dataFd
	return nil
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

func (s *seeker) readDigest() error {
	var err error
	if s.expectedInfoDigest, err = s.digestFdWithDigestContents.ReadDigest(); err != nil {
		return err
	}
	if s.expectedIndexDigest, err = s.digestFdWithDigestContents.ReadDigest(); err != nil {
		return err
	}
	return nil
}

func (s *seeker) readInfo(size int) error {
	s.prepareUnreadBuf(size)
	n, err := s.infoFdWithDigest.ReadAllAndValidate(s.unreadBuf[:size], s.expectedInfoDigest)
	if err != nil {
		return err
	}
	s.decoder.Reset(s.unreadBuf[:n])
	info, err := s.decoder.DecodeIndexInfo()
	if err != nil {
		return err
	}
	s.start = xtime.FromNanoseconds(info.Start)
	s.blockSize = time.Duration(info.BlockSize)
	s.entries = int(info.Entries)
	return nil
}

func (s *seeker) readIndex(size int) error {
	s.prepareUnreadBuf(size)
	indexBytes := s.unreadBuf[:size]
	n, err := s.indexFdWithDigest.ReadAllAndValidate(indexBytes, s.expectedIndexDigest)
	if err != nil {
		return err
	}

	s.decoder.Reset(s.unreadBuf[:n][:])
	if s.indexMap == nil {
		s.indexMap = make(map[ts.Hash]indexMapEntry, s.entries)
	}
	// Read all entries of index
	for read := 0; read < s.entries; read++ {
		entry, err := s.decoder.DecodeIndexEntry()
		if err != nil {
			return err
		}
		s.indexMap[ts.HashFn(entry.ID)] = indexMapEntry{
			size:   entry.Size,
			offset: entry.Offset,
		}

		if s.keepIndexIDs {
			entryID := append([]byte(nil), entry.ID...)
			id := ts.BinaryID(checked.NewBytes(entryID, nil))
			s.indexIDs = append(s.indexIDs, id)
		}
	}

	return nil
}

func (s *seeker) Seek(id ts.ID) (checked.Bytes, error) {
	entry, exists := s.indexMap[id.Hash()]
	if !exists {
		return nil, errSeekIDNotFound
	}

	_, err := s.dataFd.Seek(entry.offset, 0)
	if err != nil {
		return nil, err
	}
	s.dataReader.Reset(s.dataFd)

	n, err := s.dataReader.Read(s.prologue)
	if err != nil {
		return nil, err
	} else if n != cap(s.prologue) {
		return nil, errReadNotExpectedSize
	} else if !bytes.Equal(s.prologue[:markerLen], marker) {
		return nil, errReadMarkerNotFound
	}

	var data checked.Bytes
	if s.bytesPool != nil {
		data = s.bytesPool.Get(int(entry.size))
		data.IncRef()
		defer data.DecRef()
		data.Resize(int(entry.size))
	} else {
		data = checked.NewBytes(make([]byte, entry.size), nil)
		data.IncRef()
		defer data.DecRef()
	}

	n, err = s.dataReader.Read(data.Get())
	if err != nil {
		return nil, err
	}

	// In case the buffered reader only returns what's remaining in
	// the buffer, repeatedly read what's left in the underlying reader.
	for n < int(entry.size) {
		b := data.Get()[n:]
		remainder, err := s.dataReader.Read(b)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		n += remainder
	}

	if n != int(entry.size) {
		return nil, errReadNotExpectedSize
	}

	return data, nil
}

func (s *seeker) SeekOffset(id ts.ID) int {
	entry, exists := s.indexMap[id.Hash()]
	if !exists {
		return -1
	}
	return int(entry.offset)
}

func (s *seeker) Range() xtime.Range {
	return xtime.Range{Start: s.start, End: s.start.Add(s.blockSize)}
}

func (s *seeker) Entries() int {
	return s.entries
}

func (s *seeker) Close() error {
	// Prepare for reuse
	for key := range s.indexMap {
		delete(s.indexMap, key)
	}
	if s.dataFd == nil {
		return nil
	}
	err := s.dataFd.Close()
	s.dataFd = nil
	return err
}
