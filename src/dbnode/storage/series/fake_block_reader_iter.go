// Copyright (c) 2021 Uber Technologies, Inc.
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

package series

import (
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
)

// FakeBlockReaderIter iterates over the configured BlockReaders for testing.
type FakeBlockReaderIter struct {
	Readers [][]xio.BlockReader
	cur     []xio.BlockReader
	idx     int
}

// Next gets the next set of block readers.
func (i *FakeBlockReaderIter) Next(_ context.Context) bool {
	if i.idx >= len(i.Readers) {
		return false
	}
	i.cur = i.Readers[i.idx]
	i.idx++
	return true
}

// Current gets the current set of block readers.
func (i *FakeBlockReaderIter) Current() []xio.BlockReader {
	return i.cur
}

// Err is non-nil if an error occurred when calling Next.
func (i *FakeBlockReaderIter) Err() error {
	return nil
}

// ToSlices returns the configured BlockReaders.
func (i *FakeBlockReaderIter) ToSlices(_ context.Context) ([][]xio.BlockReader, error) {
	return i.Readers, nil
}
