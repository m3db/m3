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
	"io/ioutil"
	"os"
	"time"

	"github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
)

type fileReader func(fd *os.File, buf []byte) (int, error)

var (
	// errCheckpointFileNotFound returned when the checkpoint file doesn't exist
	errCheckpointFileNotFound = errors.New("checkpoint file does not exist")

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

	infoFd  *os.File
	indexFd *os.File
	dataFd  *os.File

	read        fileReader
	entries     int
	entriesRead int
	indexUnread []byte
	currEntry   schema.IndexEntry
}

// NewReader returns a new reader for a filePathPrefix, expects all files to exist.  Will
// read the index info.
func NewReader(filePathPrefix string) m3db.FileSetReader {
	return &reader{
		filePathPrefix: filePathPrefix,
		read:           readFile,
	}
}

func (r *reader) Open(shard uint32, blockStart time.Time) error {
	// If there is no checkpoint file, don't read the data files.
	shardDir := ShardDirPath(r.filePathPrefix, shard)
	checkpointFilePath := filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
	if !FileExists(checkpointFilePath) {
		return errCheckpointFileNotFound
	}

	if err := openFiles(os.Open, map[string]**os.File{
		filesetPathFromTime(shardDir, blockStart, infoFileSuffix):  &r.infoFd,
		filesetPathFromTime(shardDir, blockStart, indexFileSuffix): &r.indexFd,
		filesetPathFromTime(shardDir, blockStart, dataFileSuffix):  &r.dataFd,
	}); err != nil {
		closeFiles(validFiles(r.infoFd, r.indexFd, r.dataFd)...)
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

func (r *reader) readInfo() error {
	info, err := ReadInfo(r.infoFd)
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
	data, err := ioutil.ReadAll(r.indexFd)
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
	n, err := r.read(r.dataFd, data)
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
	return closeFiles(r.infoFd, r.indexFd, r.dataFd)
}

func readFile(fd *os.File, buf []byte) (int, error) {
	return fd.Read(buf)
}
