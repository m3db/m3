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

	"github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3db/digest"
)

var (
	// errSeekIDNotFound returned when id cannot be found in the shard
	errSeekIDNotFound = errors.New("id was not found in shard")
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

	unreadBuf  []byte
	prologue   []byte
	entries    int
	reusableID ts.ID
	dataFd     *os.File
	indexMap   map[ts.Hash]*schema.IndexEntry
	bytesPool  pool.BytesPool
}

// NewSeeker returns a new seeker for a filePathPrefix and expects all files to exist.  Will
// read the index info.
func NewSeeker(filePathPrefix string, bufferSize int, bytesPool pool.BytesPool) FileSetSeeker {
	return &seeker{
		filePathPrefix:             filePathPrefix,
		infoFdWithDigest:           digest.NewFdWithDigestReader(bufferSize),
		indexFdWithDigest:          digest.NewFdWithDigestReader(bufferSize),
		dataReader:                 bufio.NewReaderSize(nil, bufferSize),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsReader(bufferSize),
		prologue:                   make([]byte, markerLen+idxLen),
		bytesPool:                  bytesPool,
		reusableID:                 ts.StringID(""),
	}
}

// GetFileIds retrieves all the identifiers present in the configured file
func (s *seeker) GetFileIds() []string {
	fileIds := make([]string, len(s.indexMap))
	idx := 0
	for _, indexEntry := range s.indexMap {
		fileIds[idx] = string(indexEntry.Id)
		idx++
	}
	return fileIds
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

	s.dataFd = dataFd
	return nil
}

func (s *seeker) prepareUnreadBuf(size int) {
	if len(s.unreadBuf) < size {
		s.unreadBuf = make([]byte, size)
	}
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
	info, err := readInfo(s.unreadBuf[:n])
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

	indexUnread := s.unreadBuf[:n][:]
	s.indexMap = make(map[ts.Hash]*schema.IndexEntry, s.entries)
	// Read all entries of index
	for read := 0; read < s.entries; read++ {
		entry := &schema.IndexEntry{}
		size, consumed := proto.DecodeVarint(indexUnread)
		indexUnread = indexUnread[consumed:]
		if consumed < 1 {
			return errReadIndexEntryZeroSize
		}

		indexEntryData := indexUnread[:size]
		if err := proto.Unmarshal(indexEntryData, entry); err != nil {
			return err
		}

		indexUnread = indexUnread[size:]
		s.reusableID.Reset(entry.Id)
		s.indexMap[s.reusableID.Hash()] = entry
	}

	return nil
}

func (s *seeker) Seek(id ts.ID) ([]byte, error) {
	entry, exists := s.indexMap[id.Hash()]
	if !exists {
		return nil, errSeekIDNotFound
	}

	_, err := s.dataFd.Seek(entry.Offset, 0)
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

	var data []byte
	if s.bytesPool != nil {
		data = s.bytesPool.Get(int(entry.Size))[:entry.Size]
	} else {
		data = make([]byte, entry.Size)
	}

	n, err = s.dataReader.Read(data)
	if err != nil {
		return nil, err
	}

	// In case the buffered reader only returns what's remaining in
	// the buffer, repeatedly read what's left in the underlying reader.
	for n < len(data) {
		b := data[n:]
		remainder, err := s.dataReader.Read(b)

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		n += remainder
	}

	if n != int(entry.Size) {
		return nil, errReadNotExpectedSize
	}

	return data, nil
}

func (s *seeker) Range() xtime.Range {
	return xtime.Range{Start: s.start, End: s.start.Add(s.blockSize)}
}

func (s *seeker) Entries() int {
	return s.entries
}

func (s *seeker) Close() error {
	return closeAll(
		s.infoFdWithDigest,
		s.indexFdWithDigest,
	)
}
