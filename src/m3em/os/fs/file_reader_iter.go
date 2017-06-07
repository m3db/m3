// Copyright (c) 2017 Uber Technologies, Inc.
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
	"io"
	"os"

	"github.com/m3db/m3em/checksum"
)

const (
	defaultBufferSize = 1024
)

type bufferedFileReaderIter struct {
	currentBytes   []byte
	checksum       checksum.Accumulator
	bufferedReader *bufio.Reader
	fileHandle     *os.File
	err            error
	done           bool
	init           bool
}

// NewFileReaderIter creates a new buffered FileReaderIter
func NewFileReaderIter(
	filepath string,
) (FileReaderIter, error) {
	return NewSizedFileReaderIter(filepath, defaultBufferSize)
}

// NewSizedFileReaderIter creates a new buffered FileReaderIter
func NewSizedFileReaderIter(
	filepath string,
	bufferSize int,
) (FileReaderIter, error) {
	fhandle, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	var (
		bytes      = make([]byte, bufferSize)
		buffReader = bufio.NewReaderSize(fhandle, bufferSize)
		iter       = &bufferedFileReaderIter{
			currentBytes:   bytes,
			checksum:       checksum.NewAccumulator(),
			bufferedReader: buffReader,
			fileHandle:     fhandle,
		}
	)
	return iter, nil
}

func (r *bufferedFileReaderIter) Current() []byte {
	if r.Err() != nil {
		return nil
	}

	return r.currentBytes
}

func (r *bufferedFileReaderIter) Next() bool {
	if !r.init {
		r.init = true
	}

	if r.done || r.err != nil {
		return false
	}

	n, err := r.bufferedReader.Read(r.currentBytes)
	if err != nil && err != io.EOF {
		r.err = err
		r.Close()
		return false
	}

	bytes := r.currentBytes[:n]
	r.checksum.Update(bytes)
	r.currentBytes = bytes
	if err == io.EOF {
		r.done = true
		r.Close()
	}

	return true
}

func (r *bufferedFileReaderIter) Close() error {
	handle := r.fileHandle
	r.fileHandle = nil
	r.currentBytes = nil
	r.bufferedReader = nil
	if handle != nil {
		return handle.Close()
	}
	return nil
}

func (r *bufferedFileReaderIter) Checksum() uint32 {
	return r.checksum.Current()
}

func (r *bufferedFileReaderIter) Err() error {
	return r.err
}
