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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReaderSliceOfSlicesFromBlockReadersIterator(t *testing.T) {
	var a, b, c, d, e, f BlockReader
	all := []BlockReader{a, b, c, d, e, f}
	for i := range all {
		all[i] = BlockReader{
			SegmentReader: nullSegmentReader{},
		}
	}

	readers := [][]BlockReader{
		[]BlockReader{a, b, c},
		[]BlockReader{d},
		[]BlockReader{e, f},
	}

	iter := NewReaderSliceOfSlicesFromBlockReadersIterator(readers)
	validateIterReaders(t, iter, readers)
}

func TestRewind(t *testing.T) {
	var a, b, c, d, e, f BlockReader
	all := []BlockReader{a, b, c, d, e, f}
	for i := range all {
		all[i] = BlockReader{
			SegmentReader: nullSegmentReader{},
		}
	}

	readers := [][]BlockReader{
		[]BlockReader{a, b, c},
		[]BlockReader{d},
		[]BlockReader{e, f},
	}

	iter := NewReaderSliceOfSlicesFromBlockReadersIterator(readers)
	idx := iter.Index()
	validateIterReaders(t, iter, readers)

	iter.RewindToIndex(idx)
	validateIterReaders(t, iter, readers)
}

func validateIterReaders(t *testing.T, iter ReaderSliceOfSlicesFromBlockReadersIterator, readers [][]BlockReader) {
	for i := range readers {
		assert.True(t, iter.Next())
		l, _, _ := iter.CurrentReaders()
		assert.Len(t, readers[i], l)
		for j, r := range readers[i] {
			assert.Equal(t, r, iter.CurrentReaderAt(j))
		}
	}
	assert.False(t, iter.Next())
}
