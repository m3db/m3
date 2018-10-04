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
	"github.com/m3db/m3/src/m3em/checksum"
)

type bytesReaderIter struct {
	bytes []byte
	count int
}

// NewBytesReaderIter creates a new FileReaderIter to iterate
// over a fixed byte slice.
func NewBytesReaderIter(bytes []byte) FileReaderIter {
	return &bytesReaderIter{
		bytes: bytes,
	}
}

func (b *bytesReaderIter) Current() []byte {
	if b.count == 1 {
		return b.bytes
	}
	return nil
}

func (b *bytesReaderIter) Next() bool {
	b.count++
	return b.count == 1
}

func (b *bytesReaderIter) Err() error {
	return nil
}

func (b *bytesReaderIter) Checksum() uint32 {
	return checksum.Fn(b.bytes)
}

func (b *bytesReaderIter) Close() error {
	return nil
}
