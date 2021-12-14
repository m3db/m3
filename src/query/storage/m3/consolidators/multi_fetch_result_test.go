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

package consolidators

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultTestOpts = MatchOptions{
	MatchType: defaultMatchType,
}

const (
	common       = "common"
	short        = "short"
	long         = "long"
	unaggregated = "unaggregated"
)

// NB: Each seriesIterators has two seriesIterator; one with a constant ID which
// will be overwritten as necessary by multi_fetch_result, and one per namespace
// which should not be overwritten and should appear in the results.
func generateSeriesIterators(ctrl *gomock.Controller, ns string) encoding.SeriesIterators {
	var (
		end   = xtime.Now().Truncate(time.Hour)
		start = end.Add(-24 * time.Hour)
	)

	iter := encoding.NewMockSeriesIterator(ctrl)
	iter.EXPECT().ID().Return(ident.StringID(common)).MinTimes(1)
	iter.EXPECT().Namespace().Return(ident.StringID(ns)).AnyTimes()
	iter.EXPECT().Tags().Return(ident.EmptyTagIterator).AnyTimes()
	iter.EXPECT().Start().Return(start).AnyTimes()
	iter.EXPECT().End().Return(end).AnyTimes()
	iter.EXPECT().Close().AnyTimes()

	unique := encoding.NewMockSeriesIterator(ctrl)
	unique.EXPECT().ID().Return(ident.StringID(ns)).MinTimes(1)
	unique.EXPECT().Namespace().Return(ident.StringID(ns)).AnyTimes()
	unique.EXPECT().Tags().Return(ident.EmptyTagIterator).AnyTimes()
	unique.EXPECT().Start().Return(start).AnyTimes()
	unique.EXPECT().End().Return(end).AnyTimes()
	unique.EXPECT().Close().AnyTimes()

	iters := encoding.NewMockSeriesIterators(ctrl)
	iters.EXPECT().Close().MinTimes(1)
	iters.EXPECT().Len().Return(1).AnyTimes()
	iters.EXPECT().Iters().Return([]encoding.SeriesIterator{iter, unique}).AnyTimes()

	return iters
}

func generateUnreadSeriesIterators(ctrl *gomock.Controller, ns string) encoding.SeriesIterators {
	iter := encoding.NewMockSeriesIterator(ctrl)
	iter.EXPECT().Namespace().Return(ident.StringID(ns)).AnyTimes()

	unique := encoding.NewMockSeriesIterator(ctrl)
	unique.EXPECT().Namespace().Return(ident.StringID(ns)).AnyTimes()

	iters := encoding.NewMockSeriesIterators(ctrl)
	iters.EXPECT().Len().Return(1).AnyTimes()
	iters.EXPECT().Iters().Return([]encoding.SeriesIterator{iter, unique}).AnyTimes()
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

var namespaces = []struct {
	attrs storagemetadata.Attributes
	ns    string
}{
	{
		attrs: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   24 * time.Hour,
			Resolution:  0 * time.Minute,
		},
		ns: unaggregated,
	},
	{
		attrs: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   360 * time.Hour,
			Resolution:  2 * time.Minute,
		},
		ns: short,
	},
	{
		attrs: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   17520 * time.Hour,
			Resolution:  10 * time.Minute,
		},
		ns: long,
	},
}

func TestMultiResultPartialQueryRange(t *testing.T) {
	testMultiResult(t, NamespaceCoversPartialQueryRange, long)
}

func TestMultiResultAllQueryRange(t *testing.T) {
	testMultiResult(t, NamespaceCoversAllQueryRange, unaggregated)
}

func testMultiResult(t *testing.T, fanoutType QueryFanoutType, expected string) {
	ctrl := xtest.NewController(t)

	pools := generateIteratorPools(ctrl)
	r := NewMultiFetchResult(fanoutType, pools,
		defaultTestOpts, models.NewTagOptions(), LimitOptions{Limit: 1000})

	meta := block.NewResultMetadata()
	meta.FetchedSeriesCount = 4
	for _, ns := range namespaces {
		iters := generateSeriesIterators(ctrl, ns.ns)
		res := MultiFetchResults{
			SeriesIterators: iters,
			Metadata:        meta,
			Attrs:           ns.attrs,
			Err:             nil,
		}
		r.Add(res)
	}

	result, err := r.FinalResult()
	assert.NoError(t, err)

	assert.True(t, result.Metadata.Exhaustive)
	assert.True(t, result.Metadata.LocalOnly)
	assert.Equal(t, 0, len(result.Metadata.Warnings))

	iters := result.seriesData.seriesIterators
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

func TestLimit(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	pools := generateIteratorPools(ctrl)
	r := NewMultiFetchResult(NamespaceCoversPartialQueryRange, pools,
		defaultTestOpts, models.NewTagOptions(), LimitOptions{
			Limit:             2,
			RequireExhaustive: false,
		})

	meta := block.NewResultMetadata()
	for _, ns := range namespaces[0:2] {
		iters := generateSeriesIterators(ctrl, ns.ns)
		res := MultiFetchResults{
			SeriesIterators: iters,
			Metadata:        meta,
			Attrs:           ns.attrs,
			Err:             nil,
		}
		r.Add(res)
	}
	longNs := namespaces[2]
	res := MultiFetchResults{
		SeriesIterators: generateUnreadSeriesIterators(ctrl, longNs.ns),
		Metadata:        meta,
		Attrs:           longNs.attrs,
		Err:             nil,
	}
	r.Add(res)

	result, err := r.FinalResult()
	assert.NoError(t, err)
	assert.False(t, result.Metadata.Exhaustive)
	assert.True(t, result.Metadata.LocalOnly)
	assert.Equal(t, 2, result.Metadata.FetchedSeriesCount)
	assert.Equal(t, 0, len(result.Metadata.Warnings))

	iters := result.seriesData.seriesIterators
	assert.Equal(t, 2, iters.Len())
	assert.Equal(t, 2, len(iters.Iters()))

	for _, iter := range iters.Iters() {
		ns := iter.Namespace().String()
		if ns != short {
			assert.Equal(t, iter.ID().String(), ns)
		}
	}
	assert.NoError(t, r.Close())
}

func TestLimitRequireExhaustive(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	pools := generateIteratorPools(ctrl)
	r := NewMultiFetchResult(NamespaceCoversPartialQueryRange, pools,
		defaultTestOpts, models.NewTagOptions(), LimitOptions{
			Limit:             2,
			RequireExhaustive: true,
		})

	meta := block.NewResultMetadata()
	for _, ns := range namespaces[0:2] {
		iters := generateSeriesIterators(ctrl, ns.ns)
		res := MultiFetchResults{
			SeriesIterators: iters,
			Metadata:        meta,
			Attrs:           ns.attrs,
			Err:             nil,
		}
		r.Add(res)
	}
	longNs := namespaces[2]
	res := MultiFetchResults{
		SeriesIterators: generateUnreadSeriesIterators(ctrl, longNs.ns),
		Metadata:        meta,
		Attrs:           longNs.attrs,
		Err:             nil,
	}
	r.Add(res)

	_, err := r.FinalResult()
	require.Error(t, err)
	assert.NoError(t, r.Close())
}

var exhaustTests = []struct {
	name        string
	exhaustives []bool
	expected    bool
}{
	{"single exhaustive", []bool{true}, true},
	{"single non-exhaustive", []bool{false}, false},
	{"multiple exhaustive", []bool{true, true}, true},
	{"multiple non-exhaustive", []bool{false, false}, false},
	{"some exhaustive", []bool{true, false}, false},
	{"mixed", []bool{true, false, true}, false},
}

func TestExhaustiveMerge(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	pools := generateIteratorPools(ctrl)
	for _, tt := range exhaustTests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewMultiFetchResult(NamespaceCoversAllQueryRange, pools,
				defaultTestOpts, models.NewTagOptions(), LimitOptions{Limit: 1000})
			for i, ex := range tt.exhaustives {
				iters := encoding.NewSeriesIterators([]encoding.SeriesIterator{
					encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
						ID:        ident.StringID(fmt.Sprint(i)),
						Namespace: ident.StringID("ns"),
					}, nil),
				}, nil)

				meta := block.NewResultMetadata()
				meta.Exhaustive = ex
				res := MultiFetchResults{
					SeriesIterators: iters,
					Metadata:        meta,
					Attrs:           storagemetadata.Attributes{},
					Err:             nil,
				}
				r.Add(res)
			}

			result, err := r.FinalResult()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result.Metadata.Exhaustive)
			assert.NoError(t, r.Close())
		})
	}
}

func TestAddWarningsPreservedFollowedByAdd(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	pools := generateIteratorPools(ctrl)
	r := NewMultiFetchResult(NamespaceCoversPartialQueryRange, pools,
		defaultTestOpts, models.NewTagOptions(), LimitOptions{
			Limit:             100,
			RequireExhaustive: true,
		})

	r.AddWarnings(block.Warning{
		Name:    "foo",
		Message: "bar",
	})
	r.AddWarnings(block.Warning{
		Name:    "baz",
		Message: "qux",
	})

	for i := 0; i < 3; i++ {
		iters := encoding.NewSeriesIterators([]encoding.SeriesIterator{
			encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
				ID:        ident.StringID(fmt.Sprintf("series-%d", i)),
				Namespace: ident.StringID(fmt.Sprintf("ns-%d", i)),
			}, nil),
		}, nil)

		meta := block.NewResultMetadata()
		meta.Exhaustive = true
		res := MultiFetchResults{
			SeriesIterators: iters,
			Metadata:        meta,
			Attrs:           storagemetadata.Attributes{},
			Err:             nil,
		}
		r.Add(res)
	}

	finalResult, err := r.FinalResult()
	require.NoError(t, err)

	assert.Equal(t, 2, len(finalResult.Metadata.Warnings))

	assert.NoError(t, r.Close())
}
