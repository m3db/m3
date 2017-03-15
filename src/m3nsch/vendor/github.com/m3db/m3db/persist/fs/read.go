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

	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/m3db/m3db/digest"
)

type fileReader func(fd *os.File, buf []byte) (int, error)

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
	indexFdWithDigest          digest.FdWithDigestReader
	dataFdWithDigest           digest.FdWithDigestReader
	digestFdWithDigestContents digest.FdWithDigestContentsReader
	expectedInfoDigest         uint32
	expectedIndexDigest        uint32
	expectedDataDigest         uint32
	expectedDigestOfDigest     uint32

	unreadBuf   []byte
	entries     int
	entriesRead int
	prologue    []byte
	decoder     encoding.Decoder
	digestBuf   digest.Buffer
	bytesPool   pool.CheckedBytesPool
}

// NewReader returns a new reader for a filePathPrefix, expects all files to exist.  Will
// read the index info.
func NewReader(
	filePathPrefix string,
	bufferSize int,
	bytesPool pool.CheckedBytesPool,
	decodingOpts msgpack.DecodingOptions,
) FileSetReader {
	return &reader{
		filePathPrefix:             filePathPrefix,
		infoFdWithDigest:           digest.NewFdWithDigestReader(bufferSize),
		indexFdWithDigest:          digest.NewFdWithDigestReader(bufferSize),
		dataFdWithDigest:           digest.NewFdWithDigestReader(bufferSize),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsReader(bufferSize),
		prologue:                   make([]byte, markerLen+idxLen),
		decoder:                    msgpack.NewDecoder(decodingOpts),
		digestBuf:                  digest.NewBuffer(),
		bytesPool:                  bytesPool,
	}
}

func (r *reader) Open(namespace ts.ID, shard uint32, blockStart time.Time) error {
	// If there is no checkpoint file, don't read the data files.
	shardDir := ShardDirPath(r.filePathPrefix, namespace, shard)
	if err := r.readCheckpointFile(shardDir, blockStart); err != nil {
		return err
	}
	var infoFd, indexFd, dataFd, digestFd *os.File
	if err := openFiles(os.Open, map[string]**os.File{
		filesetPathFromTime(shardDir, blockStart, infoFileSuffix):   &infoFd,
		filesetPathFromTime(shardDir, blockStart, indexFileSuffix):  &indexFd,
		filesetPathFromTime(shardDir, blockStart, dataFileSuffix):   &dataFd,
		filesetPathFromTime(shardDir, blockStart, digestFileSuffix): &digestFd,
	}); err != nil {
		return err
	}

	r.infoFdWithDigest.Reset(infoFd)
	r.indexFdWithDigest.Reset(indexFd)
	r.dataFdWithDigest.Reset(dataFd)
	r.digestFdWithDigestContents.Reset(digestFd)

	defer func() {
		// NB(r): We don't need to keep these FDs open as we use these up front
		r.infoFdWithDigest.Close()
		r.indexFdWithDigest.Close()
		r.digestFdWithDigestContents.Close()
	}()

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
	indexStat, err := indexFd.Stat()
	if err != nil {
		r.Close()
		return err
	}
	if err := r.readInfo(int(infoStat.Size())); err != nil {
		r.Close()
		return err
	}
	if err := r.readIndex(int(indexStat.Size())); err != nil {
		r.Close()
		return err
	}
	return nil
}

func (r *reader) prepareUnreadBuf(size int) {
	if len(r.unreadBuf) < size {
		r.unreadBuf = make([]byte, size)
	}
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
	if r.expectedDataDigest, err = r.digestFdWithDigestContents.ReadDigest(); err != nil {
		return err
	}
	return r.digestFdWithDigestContents.Validate(r.expectedDigestOfDigest)
}

func (r *reader) readInfo(size int) error {
	r.prepareUnreadBuf(size)
	n, err := r.infoFdWithDigest.ReadAllAndValidate(r.unreadBuf[:size], r.expectedInfoDigest)
	if err != nil {
		return err
	}
	r.decoder.Reset(r.unreadBuf[:n])
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

func (r *reader) readIndex(size int) error {
	// NB(r): need to entirely consume index file so we don't stripe
	// between index and data contents as we read the data contents.
	r.prepareUnreadBuf(size)
	n, err := r.indexFdWithDigest.ReadAllAndValidate(r.unreadBuf[:size], r.expectedIndexDigest)
	if err != nil {
		return err
	}
	r.decoder.Reset(r.unreadBuf[:n][:])
	return nil
}

func (r *reader) Read() (ts.ID, checked.Bytes, uint32, error) {
	var none ts.ID
	entry, err := r.decoder.DecodeIndexEntry()
	if err != nil {
		return none, nil, 0, err
	}
	n, err := r.dataFdWithDigest.ReadBytes(r.prologue)
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

	n, err = r.dataFdWithDigest.ReadBytes(data.Get())
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
	entry, err := r.decoder.DecodeIndexEntry()
	if err != nil {
		return none, 0, 0, err
	}

	r.entriesRead++

	return r.entryID(entry.ID), int(entry.Size), uint32(entry.Checksum), nil
}

func (r *reader) entryID(id []byte) ts.ID {
	var idClone checked.Bytes
	if r.bytesPool != nil {
		idClone = r.bytesPool.Get(len(id))
		idClone.IncRef()
		defer idClone.DecRef()
	} else {
		idClone = checked.NewBytes(nil, nil)
		idClone.IncRef()
		defer idClone.DecRef()
	}

	idClone.AppendAll(id)

	return ts.BinaryID(idClone)
}

// NB(xichen): Validate should be called after all data are read because the
// digest is calculated for the entire data file.
func (r *reader) Validate() error {
	return r.dataFdWithDigest.Validate(r.expectedDataDigest)
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
	return r.dataFdWithDigest.Close()
}
