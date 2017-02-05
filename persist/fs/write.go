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
	"os"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/schema"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/time"
)

type writer struct {
	blockSize        time.Duration
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode

	infoFdWithDigest           digest.FdWithDigestWriter
	indexFdWithDigest          digest.FdWithDigestWriter
	dataFdWithDigest           digest.FdWithDigestWriter
	digestFdWithDigestContents digest.FdWithDigestContentsWriter
	checkpointFilePath         string

	start      time.Time
	currIdx    int64
	currOffset int64
	encoder    encoding.Encoder
	digestBuf  digest.Buffer
	idxData    []byte
	err        error
}

// NewWriter returns a new writer for a filePathPrefix
func NewWriter(
	blockSize time.Duration,
	filePathPrefix string,
	bufferSize int,
	newFileMode os.FileMode,
	newDirectoryMode os.FileMode,
) FileSetWriter {
	return &writer{
		blockSize:                  blockSize,
		filePathPrefix:             filePathPrefix,
		newFileMode:                newFileMode,
		newDirectoryMode:           newDirectoryMode,
		infoFdWithDigest:           digest.NewFdWithDigestWriter(bufferSize),
		indexFdWithDigest:          digest.NewFdWithDigestWriter(bufferSize),
		dataFdWithDigest:           digest.NewFdWithDigestWriter(bufferSize),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsWriter(bufferSize),
		encoder:                    msgpack.NewEncoder(),
		digestBuf:                  digest.NewBuffer(),
		idxData:                    make([]byte, idxLen),
	}
}

// Open initializes the internal state for writing to the given shard,
// specifically creating the shard directory if it doesn't exist, and
// opening / truncating files associated with that shard for writing.
func (w *writer) Open(namespace ts.ID, shard uint32, blockStart time.Time) error {
	shardDir := ShardDirPath(w.filePathPrefix, namespace, shard)
	if err := os.MkdirAll(shardDir, w.newDirectoryMode); err != nil {
		return err
	}
	w.start = blockStart
	w.currIdx = 0
	w.currOffset = 0
	w.checkpointFilePath = filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
	w.err = nil

	var infoFd, indexFd, dataFd, digestFd *os.File
	if err := openFiles(
		w.openWritable,
		map[string]**os.File{
			filesetPathFromTime(shardDir, blockStart, infoFileSuffix):   &infoFd,
			filesetPathFromTime(shardDir, blockStart, indexFileSuffix):  &indexFd,
			filesetPathFromTime(shardDir, blockStart, dataFileSuffix):   &dataFd,
			filesetPathFromTime(shardDir, blockStart, digestFileSuffix): &digestFd,
		},
	); err != nil {
		return err
	}

	w.infoFdWithDigest.Reset(infoFd)
	w.indexFdWithDigest.Reset(indexFd)
	w.dataFdWithDigest.Reset(dataFd)
	w.digestFdWithDigestContents.Reset(digestFd)

	return nil
}

func (w *writer) writeData(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	written, err := w.dataFdWithDigest.WriteBytes(data)
	if err != nil {
		return err
	}
	w.currOffset += int64(written)
	return nil
}

func (w *writer) Write(
	id ts.ID,
	data checked.Bytes,
	checksum uint32,
) error {
	return w.WriteAll(id, []checked.Bytes{data}, checksum)
}

func (w *writer) WriteAll(
	id ts.ID,
	data []checked.Bytes,
	checksum uint32,
) error {
	if w.err != nil {
		return w.err
	}

	if err := w.writeAll(id, data, checksum); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) writeAll(
	id ts.ID,
	data []checked.Bytes,
	checksum uint32,
) error {
	var size int64
	for _, d := range data {
		if d == nil {
			continue
		}
		size += int64(d.Len())
	}
	if size == 0 {
		return nil
	}

	entry := schema.IndexEntry{
		Index:    w.currIdx,
		ID:       id.Data().Get(),
		Size:     size,
		Offset:   w.currOffset,
		Checksum: int64(checksum),
	}
	w.encoder.Reset()
	if err := w.encoder.EncodeIndexEntry(entry); err != nil {
		return err
	}

	if err := w.writeData(marker); err != nil {
		return err
	}
	endianness.PutUint64(w.idxData, uint64(w.currIdx))
	if err := w.writeData(w.idxData); err != nil {
		return err
	}
	for _, d := range data {
		if d == nil {
			continue
		}
		if err := w.writeData(d.Get()); err != nil {
			return err
		}
	}
	if _, err := w.indexFdWithDigest.WriteBytes(w.encoder.Bytes()); err != nil {
		return err
	}
	w.currIdx++

	return nil
}

func (w *writer) close() error {
	info := schema.IndexInfo{
		Start:     xtime.ToNanoseconds(w.start),
		BlockSize: int64(w.blockSize),
		Entries:   w.currIdx,
	}

	w.encoder.Reset()
	if err := w.encoder.EncodeIndexInfo(info); err != nil {
		return err
	}

	if _, err := w.infoFdWithDigest.WriteBytes(w.encoder.Bytes()); err != nil {
		return err
	}

	if err := w.digestFdWithDigestContents.WriteDigests(
		w.infoFdWithDigest.Digest().Sum32(),
		w.indexFdWithDigest.Digest().Sum32(),
		w.dataFdWithDigest.Digest().Sum32(),
	); err != nil {
		return err
	}

	if err := closeAll(
		w.infoFdWithDigest,
		w.indexFdWithDigest,
		w.dataFdWithDigest,
		w.digestFdWithDigestContents,
	); err != nil {
		return err
	}

	return nil
}

func (w *writer) Close() error {
	err := w.close()
	if w.err != nil {
		return w.err
	}
	if err != nil {
		w.err = err
		return err
	}
	// NB(xichen): only write out the checkpoint file if there are no errors
	// encountered between calling writer.Open() and writer.Close().
	if err := w.writeCheckpointFile(); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) writeCheckpointFile() error {
	fd, err := w.openWritable(w.checkpointFilePath)
	if err != nil {
		return err
	}
	defer fd.Close()
	if err := w.digestBuf.WriteDigestToFile(fd, w.digestFdWithDigestContents.Digest().Sum32()); err != nil {
		return err
	}
	return nil
}

func (w *writer) openWritable(filePath string) (*os.File, error) {
	return OpenWritable(filePath, w.newFileMode)
}
