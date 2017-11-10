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

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/schema"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	// errCheckpointFileNotFound returned when the checkpoint file doesn't exist
	errCheckpointFileNotFound = errors.New("checkpoint file does not exist")

	// errReadNotExpectedSize returned when the size of the next read does not match size specified by the index
	errReadNotExpectedSize = errors.New("next read not expected size")

	// errReadMarkerNotFound returned when the marker is not found at the beginning of a data record
	errReadMarkerNotFound = errors.New("expected marker not found")
)

// ErrReadWrongIdx returned when the wrong idx is read in the data file
type ErrReadWrongIdx struct {
	ExpectedIdx int64
	ActualIdx   int64
}

func (e ErrReadWrongIdx) Error() string {
	return fmt.Sprintf("expected idx %d but found idx %d", e.ExpectedIdx, e.ActualIdx)
}

type reader struct {
	filePathPrefix string
	start          time.Time
	blockSize      time.Duration

	infoFdWithDigest           digest.FdWithDigestReader
	digestFdWithDigestContents digest.FdWithDigestContentsReader

	indexFd     *os.File
	indexMmap   []byte
	indexReader *decoderStream

	dataFd     *os.File
	dataMmap   []byte
	dataReader digest.ReaderWithDigest

	expectedInfoDigest     uint32
	expectedIndexDigest    uint32
	expectedDataDigest     uint32
	expectedDigestOfDigest uint32
	entries                int
	entriesRead            int
	metadataRead           int
	prologue               []byte
	decoder                encoding.Decoder
	digestBuf              digest.Buffer
	bytesPool              pool.CheckedBytesPool

	indexEntriesByOffset []schema.IndexEntry
}

type indexEntriesByOffset []schema.IndexEntry

func (e indexEntriesByOffset) Len() int {
	return len(e)
}

func (e indexEntriesByOffset) Less(i, j int) bool {
	return e[i].Offset < e[j].Offset
}

func (e indexEntriesByOffset) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

type decoderStream struct {
	bytesReader      *bytes.Reader
	readerWithDigest digest.ReaderWithDigest
	backingBytes     []byte
	buf              [8]byte
	lastReadByte     int
	unreadByte       int
}

func newDecoderStream() *decoderStream {
	return &decoderStream{
		readerWithDigest: digest.NewReaderWithDigest(nil),
		bytesReader:      bytes.NewReader(nil),
	}
}

func (s *decoderStream) Reset(d []byte) {
	s.bytesReader.Reset(d)
	s.readerWithDigest.Reset(s.bytesReader)
	s.backingBytes = d
	s.lastReadByte = -1
	s.unreadByte = -1
}

func (s *decoderStream) Read(p []byte) (int, error) {
	var numUnreadByte int
	if s.unreadByte >= 0 {
		p[0] = byte(s.unreadByte)
		p = p[1:]
		s.unreadByte = -1
		numUnreadByte = 1
	}
	n, err := s.readerWithDigest.Read(p)
	n += numUnreadByte
	if n > 0 {
		s.lastReadByte = int(p[n-1])
	}
	return n, err
}

func (s *decoderStream) ReadByte() (byte, error) {
	if s.unreadByte >= 0 {
		r := byte(s.unreadByte)
		s.unreadByte = -1
		return r, nil
	}
	n, err := s.readerWithDigest.Read(s.buf[:1])
	if n > 0 {
		s.lastReadByte = int(s.buf[0])
	}
	return s.buf[0], err
}

func (s *decoderStream) UnreadByte() error {
	if s.lastReadByte < 0 {
		return fmt.Errorf("no previous read byte or already unread byte")
	}
	s.unreadByte = s.lastReadByte
	s.lastReadByte = -1
	return nil
}

func (s *decoderStream) Bytes() []byte {
	return s.backingBytes
}

func (s *decoderStream) Skip(length int64) error {
	// NB(r): This ensures the reader with digest is always read
	// from start to end, i.e. to calculate digest properly.
	remaining := length
	for {
		readEnd := int64(len(s.buf))
		if remaining < readEnd {
			readEnd = remaining
		}
		n, err := s.Read(s.buf[:readEnd])
		if err != nil {
			return err
		}
		remaining -= int64(n)
		if remaining < 0 {
			return fmt.Errorf("skipped too far, remaining is: %d", remaining)
		}
		if remaining == 0 {
			return nil
		}
	}
}

func (s *decoderStream) Remaining() int64 {
	return int64(s.bytesReader.Len())
}

// NewReader returns a new reader and expects all files to exist. Will read the
// index info in full on call to Open. The bytesPool can be passed as nil if callers
// would prefer just dynamically allocated IDs and data.
func NewReader(
	bytesPool pool.CheckedBytesPool,
	opts Options,
) (FileSetReader, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &reader{
		filePathPrefix:             opts.FilePathPrefix(),
		infoFdWithDigest:           digest.NewFdWithDigestReader(opts.InfoReaderBufferSize()),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsReader(opts.InfoReaderBufferSize()),
		indexReader:                newDecoderStream(),
		dataReader:                 digest.NewReaderWithDigest(nil),
		prologue:                   make([]byte, markerLen+idxLen),
		decoder:                    msgpack.NewDecoder(opts.DecodingOptions()),
		digestBuf:                  digest.NewBuffer(),
		bytesPool:                  bytesPool,
	}, nil
}

func (r *reader) Open(namespace ts.ID, shard uint32, blockStart time.Time) error {
	// If there is no checkpoint file, don't read the data files.
	shardDir := ShardDirPath(r.filePathPrefix, namespace, shard)
	if err := r.readCheckpointFile(shardDir, blockStart); err != nil {
		return err
	}

	var infoFd, digestFd *os.File
	if err := openFiles(os.Open, map[string]**os.File{
		filesetPathFromTime(shardDir, blockStart, infoFileSuffix):   &infoFd,
		filesetPathFromTime(shardDir, blockStart, digestFileSuffix): &digestFd,
	}); err != nil {
		return err
	}

	r.infoFdWithDigest.Reset(infoFd)
	r.digestFdWithDigestContents.Reset(digestFd)

	defer func() {
		// NB(r): We don't need to keep these FDs open as we use these up front
		r.infoFdWithDigest.Close()
		r.digestFdWithDigestContents.Close()
	}()

	if err := mmapFiles(os.Open, map[string]mmapFileDesc{
		filesetPathFromTime(shardDir, blockStart, indexFileSuffix): mmapFileDesc{
			file:    &r.indexFd,
			bytes:   &r.indexMmap,
			options: mmapOptions{read: true},
		},
		filesetPathFromTime(shardDir, blockStart, dataFileSuffix): mmapFileDesc{
			file:    &r.dataFd,
			bytes:   &r.dataMmap,
			options: mmapOptions{read: true},
		},
	}); err != nil {
		return err
	}

	r.indexReader.Reset(r.indexMmap)
	r.dataReader.Reset(bytes.NewReader(r.dataMmap))

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
	if err := r.readIndex(); err != nil {
		r.Close()
		return err
	}
	return nil
}

func (r *reader) readCheckpointFile(shardDir string, blockStart time.Time) error {
	filePath := filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
	if !FileExists(filePath) {
		return errCheckpointFileNotFound
	}
	fd, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fd.Close()
	digest, err := r.digestBuf.ReadDigestFromFile(fd)
	if err != nil {
		return err
	}
	r.expectedDigestOfDigest = digest
	return nil
}

func (r *reader) readDigest() error {
	var err error
	if r.expectedInfoDigest, err = r.digestFdWithDigestContents.ReadDigest(); err != nil {
		return err
	}
	if r.expectedIndexDigest, err = r.digestFdWithDigestContents.ReadDigest(); err != nil {
		return err
	}
	if _, err := r.digestFdWithDigestContents.ReadDigest(); err != nil {
		// Skip the summaries digest
		return err
	}
	if _, err := r.digestFdWithDigestContents.ReadDigest(); err != nil {
		// Skip the bloom filter digest
		return err
	}
	if r.expectedDataDigest, err = r.digestFdWithDigestContents.ReadDigest(); err != nil {
		return err
	}
	return r.digestFdWithDigestContents.Validate(r.expectedDigestOfDigest)
}

func (r *reader) readInfo(size int) error {
	buf := make([]byte, size)
	n, err := r.infoFdWithDigest.ReadAllAndValidate(buf, r.expectedInfoDigest)
	if err != nil {
		return err
	}
	r.decoder.Reset(encoding.NewDecoderStream(buf[:n]))
	info, err := r.decoder.DecodeIndexInfo()
	if err != nil {
		return err
	}
	r.start = xtime.FromNanoseconds(info.Start)
	r.blockSize = time.Duration(info.BlockSize)
	r.entries = int(info.Entries)
	r.entriesRead = 0
	return nil
}

func (r *reader) readIndex() error {
	r.decoder.Reset(r.indexReader)
	for i := 0; i < r.entries; i++ {
		entry, err := r.decoder.DecodeIndexEntry()
		if err != nil {
			return err
		}
		r.indexEntriesByOffset = append(r.indexEntriesByOffset, entry)
	}
	// NB(r): As we decode each block we need access to each index entry
	// in the order we decode the data
	sort.Sort(indexEntriesByOffset(r.indexEntriesByOffset))
	return nil
}

func (r *reader) Read() (ts.ID, checked.Bytes, uint32, error) {
	var none ts.ID
	if r.entriesRead >= r.entries {
		return none, nil, 0, io.EOF
	}

	entry := r.indexEntriesByOffset[r.entriesRead]

	n, err := r.dataReader.Read(r.prologue)
	if err != nil {
		return none, nil, 0, err
	}
	if n != cap(r.prologue) {
		return none, nil, 0, errReadNotExpectedSize
	}
	if !bytes.Equal(r.prologue[:markerLen], marker) {
		return none, nil, 0, errReadMarkerNotFound
	}
	idx := int64(endianness.Uint64(r.prologue[markerLen : markerLen+idxLen]))
	if idx != entry.Index {
		return none, nil, 0, ErrReadWrongIdx{ExpectedIdx: entry.Index, ActualIdx: idx}
	}

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

	n, err = r.dataReader.Read(data.Get())
	if err != nil {
		return none, nil, 0, err
	}
	if n != int(entry.Size) {
		return none, nil, 0, errReadNotExpectedSize
	}

	r.entriesRead++

	return r.entryID(entry.ID), data, uint32(entry.Checksum), nil
}

func (r *reader) ReadMetadata() (id ts.ID, length int, checksum uint32, err error) {
	var none ts.ID
	if r.metadataRead >= r.entries {
		return none, 0, 0, io.EOF
	}

	entry, err := r.decoder.DecodeIndexEntry()
	if err != nil {
		return none, 0, 0, err
	}

	r.metadataRead++

	return r.entryID(entry.ID), int(entry.Size), uint32(entry.Checksum), nil
}

func (r *reader) entryID(id []byte) ts.ID {
	var idClone checked.Bytes
	if r.bytesPool != nil {
		idClone = r.bytesPool.Get(len(id))
		idClone.IncRef()
		defer idClone.DecRef()
	} else {
		idClone = checked.NewBytes(make([]byte, 0, len(id)), nil)
		idClone.IncRef()
		defer idClone.DecRef()
	}

	idClone.AppendAll(id)

	return ts.BinaryID(idClone)
}

// NB(xichen): Validate should be called after all data are read because the
// digest is calculated for the entire data file.
func (r *reader) Validate() error {
	err := r.indexReader.readerWithDigest.Validate(r.expectedIndexDigest)
	if err != nil {
		return fmt.Errorf("could not validate index file: %v", err)
	}
	err = r.dataReader.Validate(r.expectedDataDigest)
	if err != nil {
		return fmt.Errorf("could not validate data file: %v", err)
	}
	return err
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

func (r *reader) Close() error {
	for i := 0; i < len(r.indexEntriesByOffset); i++ {
		r.indexEntriesByOffset[i].ID = nil
	}
	r.indexEntriesByOffset = r.indexEntriesByOffset[:0]

	if err := munmap(r.indexMmap); err != nil {
		return err
	}
	r.indexMmap = nil

	if err := munmap(r.dataMmap); err != nil {
		return err
	}
	r.dataMmap = nil

	if err := r.indexFd.Close(); err != nil {
		return err
	}

	return r.dataFd.Close()
}
