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
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
)

// GenerateSingleSampleTagIterator generates a new tag iterator
func GenerateSingleSampleTagIterator(
	ctrl *gomock.Controller, tag ident.Tag,
) ident.TagIterator {
	mockTagIterator := ident.NewMockTagIterator(ctrl)
	mockTagIterator.EXPECT().Duplicate().Return(mockTagIterator).MaxTimes(1)
	mockTagIterator.EXPECT().Remaining().Return(1).MaxTimes(1)
	mockTagIterator.EXPECT().Next().Return(true).MaxTimes(1)
	mockTagIterator.EXPECT().Current().Return(tag).MaxTimes(1)
	mockTagIterator.EXPECT().Next().Return(false).MaxTimes(1)
	mockTagIterator.EXPECT().Err().Return(nil).AnyTimes()
	mockTagIterator.EXPECT().Rewind().Return().MaxTimes(1)
	mockTagIterator.EXPECT().Close().AnyTimes()

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
func NewMockSeriesIterSlice(
	ctrl *gomock.Controller,
	tagGenerator func() ident.TagIterator,
	len int,
	numValues int,
) []encoding.SeriesIterator {
	iteratorList := make([]encoding.SeriesIterator, 0, len)
	for i := 0; i < len; i++ {
		mockIter := NewMockSeriesIterator(ctrl, tagGenerator, numValues)
		iteratorList = append(iteratorList, mockIter)
	}
	return iteratorList
}

// NewMockSeriesIterator constructs a MockSeriesIterator return numValues
// datapoints, using tagGenerator, and otherwise configured with sensible defaults.
func NewMockSeriesIterator(
	ctrl *gomock.Controller, tagGenerator func() ident.TagIterator, numValues int,
) *encoding.MockSeriesIterator {
	it := encoding.NewMockSeriesIterator(ctrl)
	return NewMockSeriesIteratorFromBase(it, tagGenerator, numValues)
}

// NewMockSeriesIteratorFromBase constructs a MockSeriesIterator return numValues
// datapoints, using tagGenerator, and otherwise configured with sensible
// defaults. Any expectations already set on mockIter will be respected.
func NewMockSeriesIteratorFromBase(
	mockIter *encoding.MockSeriesIterator,
	tagGenerator func() ident.TagIterator,
	numValues int,
) *encoding.MockSeriesIterator {
	mockIter.EXPECT().Next().Return(true).MaxTimes(numValues)
	mockIter.EXPECT().Next().Return(false).MaxTimes(1)
	now := xtime.Now()
	for i := 0; i < numValues; i++ {
		mockIter.EXPECT().Current().Return(
			m3ts.Datapoint{
				TimestampNanos: now.Add(time.Duration(i*10) * time.Second),
				Value:          float64(i),
			}, xtime.Millisecond, nil,
		).MaxTimes(1)
	}

	tags := tagGenerator()
	mockIter.EXPECT().Namespace().Return(ident.StringID("foo")).AnyTimes()
	mockIter.EXPECT().ID().Return(ident.StringID("bar")).AnyTimes()
	mockIter.EXPECT().Tags().Return(tags).AnyTimes()
	mockIter.EXPECT().Start().Return(now.Add(-time.Hour)).AnyTimes()
	mockIter.EXPECT().End().Return(now).AnyTimes()
	mockIter.EXPECT().FirstAnnotation().Return(nil).AnyTimes()
	mockIter.EXPECT().Close().Do(func() {
		// Make sure to close the tags generated when closing the iter
		tags.Close()
	}).AnyTimes()
	mockIter.EXPECT().Err().Return(nil).AnyTimes()
	return mockIter
}

// NewMockSeriesIters generates a new mock series iters
func NewMockSeriesIters(
	ctrl *gomock.Controller,
	tags ident.Tag,
	l int,
	numValues int,
) encoding.SeriesIterators {
	tagGenerator := func() ident.TagIterator {
		return GenerateSingleSampleTagIterator(ctrl, tags)
	}

	iteratorList := NewMockSeriesIterSlice(ctrl, tagGenerator, l, numValues)
	mockIters := encoding.NewMockSeriesIterators(ctrl)
	mockIters.EXPECT().Iters().Return(iteratorList).AnyTimes()
	mockIters.EXPECT().Len().Return(l).AnyTimes()
	mockIters.EXPECT().Close().Do(func() {
		// Make sure to close the iterators
		for _, iter := range iteratorList {
			if iter != nil {
				iter.Close()
			}
		}
	}).AnyTimes()

	return mockIters
}

// NewMockValidTagGenerator wraps around GenerateSimpleTagIterator to
// construct a default TagIterator function.
func NewMockValidTagGenerator(ctrl *gomock.Controller) func() ident.TagIterator {
	return func() ident.TagIterator {
		return GenerateSingleSampleTagIterator(ctrl, GenerateTag())
	}
}
