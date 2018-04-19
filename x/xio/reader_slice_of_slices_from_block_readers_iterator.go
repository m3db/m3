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

package xio

import (
	"time"
)

type readerSliceOfSlicesIterator struct {
	blocks [][]BlockReader
	idx    int
	len    int
	closed bool
}

// NewReaderSliceOfSlicesFromBlockReadersIterator creates a new reader slice of slices iterator
func NewReaderSliceOfSlicesFromBlockReadersIterator(
	blocks [][]BlockReader,
) ReaderSliceOfSlicesFromBlockReadersIterator {
	it := &readerSliceOfSlicesIterator{}
	it.Reset(blocks)
	return it
}

func (it *readerSliceOfSlicesIterator) Next() bool {
	if !(it.idx+1 < it.len) {
		return false
	}
	it.idx++
	return true
}

var timeZero = time.Time{}

func (it *readerSliceOfSlicesIterator) Current() (int, time.Time, time.Time) {
	if len(it.blocks) < it.arrayIdx() {
		return 0, timeZero, timeZero
	}
	currentLen := len(it.blocks[it.arrayIdx()])
	if currentLen == 0 {
		return 0, timeZero, timeZero
	}
	currBlock := it.blocks[it.arrayIdx()][0]
	return currentLen, currBlock.Start(), currBlock.End()
}

func (it *readerSliceOfSlicesIterator) CurrentAt(idx int) Reader {
	return it.blocks[it.arrayIdx()][idx]
}

func (it *readerSliceOfSlicesIterator) Reset(blocks [][]BlockReader) {
	it.blocks = blocks
	it.idx = -1
	it.len = len(blocks)
	it.closed = false
}

func (it *readerSliceOfSlicesIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
}

func (it *readerSliceOfSlicesIterator) arrayIdx() int {
	idx := it.idx
	if idx == -1 {
		idx = 0
	}
	return idx
}
