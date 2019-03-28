// Copyright (c) 2019 Uber Technologies, Inc.
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

package m3

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const (
	common       = "common"
	short        = "short"
	long         = "long"
	unaggregated = "unaggregated"
)

// NB: Each seriesIterators has two seriesIterator; one with a constant ID which
// will be overwritten as necessary by multi_fetch_result, and one per namespace
// which should not be overwritten and should appear in the results.
func generateSeriesIterators(
	ctrl *gomock.Controller, ns string) encoding.SeriesIterators {
	iter := encoding.NewMockSeriesIterator(ctrl)
	iter.EXPECT().ID().Return(ident.StringID(common)).MinTimes(1)
	iter.EXPECT().Namespace().Return(ident.StringID(ns)).MaxTimes(1)

	unique := encoding.NewMockSeriesIterator(ctrl)
	unique.EXPECT().ID().Return(ident.StringID(ns)).MinTimes(1)
	unique.EXPECT().Namespace().Return(ident.StringID(ns)).MaxTimes(1)

	iters := encoding.NewMockSeriesIterators(ctrl)
	iters.EXPECT().Close().Return().Times(1)
	iters.EXPECT().Len().Return(1).MaxTimes(1)
	iters.EXPECT().Iters().Return([]encoding.SeriesIterator{iter, unique})

	return iters
}

func generateIteratorPools(ctrl *gomock.Controller) encoding.IteratorPools {
	pools := encoding.NewMockIteratorPools(ctrl)

	mutablePool := encoding.NewMockMutableSeriesIteratorsPool(ctrl)
	mutablePool.EXPECT().
		Get(gomock.Any()).
		DoAndReturn(func(size int) encoding.MutableSeriesIterators {
			return encoding.NewSeriesIterators(make([]encoding.SeriesIterator, 0, size), mutablePool)
		}).
		AnyTimes()
	mutablePool.EXPECT().Put(gomock.Any()).AnyTimes()

	pools.EXPECT().MutableSeriesIterators().Return(mutablePool).AnyTimes()

	return pools
}

func TestMultiResult(t *testing.T) {
	testMultiResult(t, namespaceCoversPartialQueryRange, long)
	testMultiResult(t, namespaceCoversAllQueryRange, unaggregated)
}

func testMultiResult(t *testing.T, fanoutType queryFanoutType, expected string) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	namespaces := []struct {
		attrs storage.Attributes
		ns    string
	}{
		{
			attrs: storage.Attributes{
				MetricsType: storage.UnaggregatedMetricsType,
				Retention:   24 * time.Hour,
				Resolution:  0 * time.Minute,
			},
			ns: unaggregated,
		},
		{
			attrs: storage.Attributes{
				MetricsType: storage.AggregatedMetricsType,
				Retention:   360 * time.Hour,
				Resolution:  2 * time.Minute,
			},
			ns: short,
		},
		{
			attrs: storage.Attributes{
				MetricsType: storage.AggregatedMetricsType,
				Retention:   17520 * time.Hour,
				Resolution:  10 * time.Minute,
			},
			ns: long,
		},
	}

	pools := generateIteratorPools(ctrl)
	r := newMultiFetchResult(fanoutType, pools)

	for _, ns := range namespaces {
		iters := generateSeriesIterators(ctrl, ns.ns)
		r.Add(ns.attrs, iters, nil)
	}

	iters, err := r.FinalResult()
	assert.NoError(t, err)
	assert.Equal(t, 4, iters.Len())
	assert.Equal(t, 4, len(iters.Iters()))

	for _, n := range iters.Iters() {
		id := n.ID().String()
		// NB: if this is the common id, check against expected for the fanout type.
		if id == common {
			assert.Equal(t, expected, n.Namespace().String())
		} else {
			assert.Equal(t, id, n.Namespace().String())
		}
	}

	assert.NoError(t, r.Close())
}
