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

	schema "github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
)

var (
	defaultNewFileMode      = os.FileMode(0666)
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)
)

type writer struct {
	blockSize        time.Duration
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode

	infoFd             *os.File
	indexFd            *os.File
	dataFd             *os.File
	checkpointFilePath string

	start        time.Time
	currEntry    schema.IndexEntry
	currIdx      int64
	currOffset   int64
	infoBuffer   *proto.Buffer
	indexBuffer  *proto.Buffer
	varintBuffer *proto.Buffer
	idxData      []byte
	err          error
}

// WriterOptions provides options for a Writer
type WriterOptions interface {
	// NewFileMode sets the new file mode.
	NewFileMode(value os.FileMode) WriterOptions

	// GetNewFileMode returns the new file mode.
	GetNewFileMode() os.FileMode

	// NewDirectoryMode sets the new directory mode.
	NewDirectoryMode(value os.FileMode) WriterOptions

	// GetNewDirectoryMode returns the new directory mode.
	GetNewDirectoryMode() os.FileMode
}

type writerOptions struct {
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
}

// NewWriterOptions creates a writer options.
func NewWriterOptions() WriterOptions {
	return &writerOptions{
		newFileMode:      defaultNewFileMode,
		newDirectoryMode: defaultNewDirectoryMode,
	}
}

func (o *writerOptions) NewFileMode(value os.FileMode) WriterOptions {
	opts := *o
	opts.newFileMode = value
	return &opts
}

func (o *writerOptions) GetNewFileMode() os.FileMode {
	return o.newFileMode
}

func (o *writerOptions) NewDirectoryMode(value os.FileMode) WriterOptions {
	opts := *o
	opts.newDirectoryMode = value
	return &opts
}

func (o *writerOptions) GetNewDirectoryMode() os.FileMode {
	return o.newDirectoryMode
}

// NewWriter returns a new writer for a filePathPrefix
func NewWriter(
	blockSize time.Duration,
	filePathPrefix string,
	options WriterOptions,
) m3db.FileSetWriter {
	if options == nil {
		options = NewWriterOptions()
	}
	return &writer{
		blockSize:        blockSize,
		filePathPrefix:   filePathPrefix,
		newFileMode:      options.GetNewFileMode(),
		newDirectoryMode: options.GetNewDirectoryMode(),
		infoBuffer:       proto.NewBuffer(nil),
		indexBuffer:      proto.NewBuffer(nil),
		varintBuffer:     proto.NewBuffer(nil),
		idxData:          make([]byte, idxLen),
	}
}

// Open initializes the internal state for writing to the given shard,
// specifically creating the shard directory if it doesn't exist, and
// opening / truncating files associated with that shard for writing.
func (w *writer) Open(shard uint32, blockStart time.Time) error {
	shardDir := ShardDirPath(w.filePathPrefix, shard)
	if err := os.MkdirAll(shardDir, w.newDirectoryMode); err != nil {
		return err
	}
	w.start = blockStart
	w.currIdx = 0
	w.currOffset = 0
	w.checkpointFilePath = filepathFromTime(shardDir, blockStart, checkpointFileSuffix)
	w.err = nil
	if err := openFiles(
		w.openWritable,
		map[string]**os.File{
			filepathFromTime(shardDir, blockStart, infoFileSuffix):  &w.infoFd,
			filepathFromTime(shardDir, blockStart, indexFileSuffix): &w.indexFd,
			filepathFromTime(shardDir, blockStart, dataFileSuffix):  &w.dataFd,
		},
	); err != nil {
		closeFiles(validFiles(w.infoFd, w.indexFd, w.dataFd)...)
		return err
	}
	return nil
}

func (w *writer) writeData(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	written, err := w.dataFd.Write(data)
	if err != nil {
		return err
	}
	w.currOffset += int64(written)
	return nil
}

func (w *writer) Write(key string, data []byte) error {
	return w.WriteAll(key, [][]byte{data})
}

func (w *writer) WriteAll(key string, data [][]byte) error {
	if w.err != nil {
		return w.err
	}

	if err := w.writeAll(key, data); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) writeAll(key string, data [][]byte) error {
	var size int64
	for _, d := range data {
		size += int64(len(d))
	}
	if size == 0 {
		return nil
	}

	entry := &w.currEntry
	entry.Reset()
	entry.Idx = w.currIdx
	entry.Size = size
	entry.Key = key
	entry.Offset = w.currOffset

	w.indexBuffer.Reset()
	if err := w.indexBuffer.Marshal(entry); err != nil {
		return err
	}

	w.varintBuffer.Reset()
	entryBytes := w.indexBuffer.Bytes()
	if err := w.varintBuffer.EncodeVarint(uint64(len(entryBytes))); err != nil {
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
		if err := w.writeData(d); err != nil {
			return err
		}
	}

	if _, err := w.indexFd.Write(w.varintBuffer.Bytes()); err != nil {
		return err
	}
	if _, err := w.indexFd.Write(entryBytes); err != nil {
		return err
	}

	w.currIdx++

	return nil
}

func (w *writer) close() error {
	if err := w.infoFd.Truncate(0); err != nil {
		return err
	}

	info := &schema.IndexInfo{
		Start:     xtime.ToNanoseconds(w.start),
		BlockSize: int64(w.blockSize),
		Entries:   w.currIdx,
	}
	w.infoBuffer.Reset()
	if err := w.infoBuffer.Marshal(info); err != nil {
		return err
	}

	if _, err := w.infoFd.Write(w.infoBuffer.Bytes()); err != nil {
		return err
	}

	if err := closeFiles(w.infoFd, w.indexFd, w.dataFd); err != nil {
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
	return w.writeCheckpointFile()
}

func (w *writer) writeCheckpointFile() error {
	fd, err := w.openWritable(w.checkpointFilePath)
	if err != nil {
		return err
	}
	fd.Close()
	return nil
}

func (w *writer) openWritable(filePath string) (*os.File, error) {
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, w.newFileMode)
	if err != nil {
		return nil, err
	}
	return fd, nil
}
