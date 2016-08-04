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
	"os"
	"time"

	schema "github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
)

type fileReader func(fd *os.File, buf []byte) (int, error)

var (
	// errReadIndexEntryZeroSize returned when size of next index entry is zero
	errReadIndexEntryZeroSize = errors.New("next index entry is encoded as zero size")

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

	infoFdWithDigest       fdWithDigest
	indexFdWithDigest      fdWithDigest
	dataFdWithDigest       fdWithDigest
	digestFdWithDigest     fdWithDigest
	expectedInfoDigest     uint32
	expectedIndexDigest    uint32
	expectedDataDigest     uint32
	expectedDigestOfDigest uint32

	entries     int
	entriesRead int
	indexUnread []byte
	currEntry   schema.IndexEntry
	buf         []byte
}

// NewReader returns a new reader for a filePathPrefix, expects all files to exist.  Will
// read the index info.
func NewReader(filePathPrefix string) m3db.FileSetReader {
	return &reader{
		filePathPrefix:     filePathPrefix,
		infoFdWithDigest:   newFdWithDigest(),
		indexFdWithDigest:  newFdWithDigest(),
		dataFdWithDigest:   newFdWithDigest(),
		digestFdWithDigest: newFdWithDigest(),
		buf:                make([]byte, digestLen),
	}
}

func (r *reader) Open(shard uint32, blockStart time.Time) error {
	// If there is no checkpoint file, don't read the data files.
	shardDir := shardDirPath(r.filePathPrefix, shard)
	if err := r.readCheckpointFile(shardDir, blockStart); err != nil {
		return err
	}
	if err := openFiles(os.Open, map[string]**os.File{
		filepathFromTime(shardDir, blockStart, infoFileSuffix):   &r.infoFdWithDigest.fd,
		filepathFromTime(shardDir, blockStart, indexFileSuffix):  &r.indexFdWithDigest.fd,
		filepathFromTime(shardDir, blockStart, dataFileSuffix):   &r.dataFdWithDigest.fd,
		filepathFromTime(shardDir, blockStart, digestFileSuffix): &r.digestFdWithDigest.fd,
	}); err != nil {
		closeFiles(validFiles(r.infoFdWithDigest.fd, r.indexFdWithDigest.fd, r.dataFdWithDigest.fd, r.digestFdWithDigest.fd)...)
		return err
	}
	r.infoFdWithDigest.resetDigest()
	r.indexFdWithDigest.resetDigest()
	r.dataFdWithDigest.resetDigest()
	r.digestFdWithDigest.resetDigest()
	if err := r.readDigest(); err != nil {
		// Try to close if failed to read digest
		r.Close()
		return err
	}
	if err := r.readInfo(); err != nil {
		// Try to close if failed to read info
		r.Close()
		return err
	}
	if err := r.readIndex(); err != nil {
		// Try to close if failed to read index
		r.Close()
		return err
	}
	return nil
}

func (r *reader) readCheckpointFile(shardDir string, blockStart time.Time) error {
	digest, err := readCheckpointFile(shardDir, blockStart, r.buf)
	if err != nil {
		return err
	}
	r.expectedDigestOfDigest = digest
	return nil
}

func (r *reader) readDigest() error {
	var err error
	if r.expectedInfoDigest, err = r.digestFdWithDigest.readDigest(r.buf); err != nil {
		return err
	}
	if r.expectedIndexDigest, err = r.digestFdWithDigest.readDigest(r.buf); err != nil {
		return err
	}
	if r.expectedDataDigest, err = r.digestFdWithDigest.readDigest(r.buf); err != nil {
		return err
	}
	return validateDigest(r.expectedDigestOfDigest, r.digestFdWithDigest.digest)
}

func (r *reader) readInfo() error {
	data, err := r.infoFdWithDigest.readAllAndValidate(r.expectedInfoDigest)
	if err != nil {
		return err
	}
	info, err := readInfo(data)
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
	// NB(r): use a bytes.NewReader if/when protobuf library supports buffered reading
	data, err := r.indexFdWithDigest.readAllAndValidate(r.expectedIndexDigest)
	if err != nil {
		return err
	}
	r.indexUnread = data

	return nil
}

func (r *reader) Read() (string, []byte, error) {
	var none string
	entry := &r.currEntry
	entry.Reset()

	size, consumed := proto.DecodeVarint(r.indexUnread)
	r.indexUnread = r.indexUnread[consumed:]
	if consumed < 1 {
		return none, nil, errReadIndexEntryZeroSize
	}
	indexEntryData := r.indexUnread[:size]
	if err := proto.Unmarshal(indexEntryData, entry); err != nil {
		return none, nil, err
	}
	r.indexUnread = r.indexUnread[size:]

	expectedSize := markerLen + idxLen + int(entry.Size)
	data := make([]byte, expectedSize)
	n, err := r.dataFdWithDigest.readBytes(data)
	if err != nil {
		return none, nil, err
	}
	if n != expectedSize {
		return none, nil, errReadNotExpectedSize
	}

	if !bytes.Equal(data[:markerLen], marker) {
		return none, nil, errReadMarkerNotFound
	}

	idx := int64(endianness.Uint64(data[markerLen : markerLen+idxLen]))
	if idx != entry.Idx {
		return none, nil, ErrReadWrongIdx{ExpectedIdx: entry.Idx, ActualIdx: idx}
	}

	r.entriesRead++

	return entry.Key, data[markerLen+idxLen:], nil
}

// NB(xichen): Validate should be called after all data are read because the
// digest is calculated for the entire data file.
func (r *reader) Validate() error {
	return validateDigest(r.expectedDataDigest, r.dataFdWithDigest.digest)
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
	return closeFiles(
		r.infoFdWithDigest.fd,
		r.indexFdWithDigest.fd,
		r.dataFdWithDigest.fd,
		r.digestFdWithDigest.fd,
	)
}
