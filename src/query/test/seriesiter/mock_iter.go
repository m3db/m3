// Copyright (c) 2018 Uber Technologies, Inc.
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

package seriesiter

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	m3ts "github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
)

// GenerateSingleSampleTagIterator generates a new tag iterator
func GenerateSingleSampleTagIterator(ctrl *gomock.Controller, tag ident.Tag) ident.TagIterator {
	mockTagIterator := ident.NewMockTagIterator(ctrl)
	mockTagIterator.EXPECT().Remaining().Return(1).Times(1)
	mockTagIterator.EXPECT().Next().Return(true).Times(1)
	mockTagIterator.EXPECT().Current().Return(tag).Times(1)
	mockTagIterator.EXPECT().Next().Return(false).Times(1)
	mockTagIterator.EXPECT().Err().Return(nil).Times(1)
	mockTagIterator.EXPECT().Close().Times(1)

	return mockTagIterator
}

// GenerateTag generates a new tag
func GenerateTag() ident.Tag {
	return ident.Tag{
		Name:  ident.StringID("foo"),
		Value: ident.StringID("bar"),
	}
}

// NewMockSeriesIterSlice generates a slice of mock series iterators
func NewMockSeriesIterSlice(ctrl *gomock.Controller, tagGenerator func() ident.TagIterator, len int, numValues int) []encoding.SeriesIterator {
	iteratorList := make([]encoding.SeriesIterator, 0, len)
	for i := 0; i < len; i++ {
		mockIter := encoding.NewMockSeriesIterator(ctrl)
		mockIter.EXPECT().Next().Return(true).Times(numValues)
		mockIter.EXPECT().Next().Return(false).Times(1)
		now := time.Now()
		for i := 0; i < numValues; i++ {
			mockIter.EXPECT().Current().Return(m3ts.Datapoint{Timestamp: now.Add(time.Duration(i*10) * time.Second), Value: float64(i)}, xtime.Millisecond, nil).Times(1)
		}

		tags := tagGenerator()
		mockIter.EXPECT().ID().Return(ident.StringID("foo")).Times(1)
		mockIter.EXPECT().Tags().Return(tags).Times(1)
		mockIter.EXPECT().Close().Do(func() {
			// Make sure to close the tags generated when closing the iter
			tags.Close()
		})

		iteratorList = append(iteratorList, mockIter)
	}
	return iteratorList
}

// NewMockSeriesIters generates a new mock series iters
func NewMockSeriesIters(ctrl *gomock.Controller, tags ident.Tag, len int, numValues int) encoding.SeriesIterators {
	tagGenerator := func() ident.TagIterator {
		return GenerateSingleSampleTagIterator(ctrl, tags)
	}
	iteratorList := NewMockSeriesIterSlice(ctrl, tagGenerator, len, numValues)

	mockIters := encoding.NewMockSeriesIterators(ctrl)
	mockIters.EXPECT().Iters().Return(iteratorList)
	mockIters.EXPECT().Len().Return(len)
	mockIters.EXPECT().Close().Do(func() {
		// Make sure to close the iterators
		for _, iter := range iteratorList {
			iter.Close()
		}
	})

	return mockIters
}
