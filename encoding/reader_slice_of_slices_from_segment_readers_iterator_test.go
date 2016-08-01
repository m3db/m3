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

package encoding

import (
	"testing"

	"github.com/m3db/m3db/generated/mocks/mocks"
	"github.com/m3db/m3db/interfaces/m3db"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestReaderSliceOfSlicesFromSegmentReadersIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var a, b, c, d, e, f m3db.SegmentReader
	all := []*m3db.SegmentReader{&a, &b, &c, &d, &e, &f}
	for _, r := range all {
		*r = mocks.NewMockSegmentReader(ctrl)
	}

	readers := [][]m3db.SegmentReader{
		[]m3db.SegmentReader{a, b, c},
		[]m3db.SegmentReader{d},
		[]m3db.SegmentReader{e, f},
	}

	iter := NewReaderSliceOfSlicesFromSegmentReadersIterator(readers)
	for i := range readers {
		assert.True(t, iter.Next())
		assert.Equal(t, len(readers[i]), iter.CurrentLen())
		for j, r := range readers[i] {
			assert.Equal(t, r, iter.CurrentAt(j))
		}
	}
	assert.False(t, iter.Next())
}
