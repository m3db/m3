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

package m3db

import (
	"io"
	"time"

	"github.com/m3db/m3x/time"
)

// FileSetWriter provides an unsynchronized writer for a TSDB file set.
type FileSetWriter interface {
	io.Closer

	// Open opens the files for writing data to the given shard.
	Open(shard uint32, start time.Time) error

	// Write will write the key and data pair and returns an error on a write error
	Write(key string, data []byte) error

	// WriteAll will write the key and all byte slices and returns an error on a write error.
	WriteAll(key string, data [][]byte) error
}

// FileSetReader provides an unsynchronized reader for a TSDB file set.
type FileSetReader interface {
	io.Closer

	// Open opens the files for the given shard and version for reading.
	Open(shard uint32, start time.Time) error

	// Read returns the next key and data pair or error, will return io.EOF at end of volume
	Read() (key string, data []byte, err error)

	// Validate validates the data and returns an error if the data are corrupted
	Validate() error

	// Range returns the time range associated with data in the volume.
	Range() xtime.Range

	// Entries returns the count of entries in the volume
	Entries() int

	// EntriesRead returns the position read into the volume
	EntriesRead() int
}

// NewFileSetWriterFn creates a new fileset writer.
type NewFileSetWriterFn func(blockSize time.Duration, filePathPrefix string, writerBufferSize int) FileSetWriter

// NewFileSetReaderFn creates a new fileset reader.
type NewFileSetReaderFn func(filePathPrefix string, readerBufferSize int) FileSetReader
