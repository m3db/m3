// Copyright (c) 2020 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type insertEntry struct {
	iter encoding.SeriesIterators
	attr storagemetadata.Attributes
	meta block.ResultMetadata
	err  error
}

type dedupeTest struct {
	name     string
	entries  []insertEntry
	expected []expectedSeries
	exMeta   block.ResultMetadata
	exErr    error
	limit    int
	exAttrs  []storagemetadata.Attributes
}

type expectedSeries struct {
	tags []string
	dps  []dp
}

//nolint:dupl
func TestMultiFetchResultTagDedupeMap(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	start := xtime.Now().Truncate(time.Hour)
	step := func(i int) xtime.UnixNano {
		return start.Add(time.Minute * time.Duration(i))
	}

	unaggHr := storagemetadata.Attributes{
		MetricsType: storagemetadata.UnaggregatedMetricsType,
		Resolution:  time.Hour,
	}

	warn1Meta := block.NewResultMetadata()
	warn1Meta.AddWarning("warn", "1")

	warn2Meta := block.NewResultMetadata()
	warn2Meta.AddWarning("warn", "2")

	combinedMeta := warn1Meta.CombineMetadata(warn2Meta)

	nonExhaustiveMeta := block.NewResultMetadata()
	nonExhaustiveMeta.Exhaustive = false

	tests := []dedupeTest{
		{
			name: "same tags, same ids",
			entries: []insertEntry{
				{
					attr: unaggHr,
					meta: warn1Meta,
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar"),
						it(ctrl, dp{t: step(5), val: 6}, "id1", "foo", "bar"),
					}, nil),
				},
			},
			expected: []expectedSeries{
				{
					tags: []string{"foo", "bar"},
					dps:  []dp{{t: step(1), val: 1}, {t: step(5), val: 6}},
				},
			},
			exMeta:  warn1Meta,
			exErr:   nil,
			exAttrs: []storagemetadata.Attributes{unaggHr},
		},
		{
			name: "same tags, different ids",
			entries: []insertEntry{
				{
					attr: unaggHr,
					meta: warn1Meta,
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar"),
						it(ctrl, dp{t: step(5), val: 6}, "id2", "foo", "bar"),
					}, nil),
				},
			},
			expected: []expectedSeries{
				{
					tags: []string{"foo", "bar"},
					dps:  []dp{{t: step(1), val: 1}, {t: step(5), val: 6}},
				},
			},
			exMeta:  warn1Meta,
			exErr:   nil,
			exAttrs: []storagemetadata.Attributes{unaggHr},
		},

		{
			name: "different tags, same ids",
			entries: []insertEntry{
				{
					attr: unaggHr,
					meta: warn1Meta,
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar"),
						it(ctrl, dp{t: step(5), val: 6}, "id1", "foo", "baz"),
					}, nil),
				},
			},
			expected: []expectedSeries{
				{
					tags: []string{"foo", "bar"},
					dps:  []dp{{t: step(1), val: 1}},
				},
				{
					tags: []string{"foo", "baz"},
					dps:  []dp{{t: step(5), val: 6}},
				},
			},
			exMeta:  warn1Meta,
			exErr:   nil,
			exAttrs: []storagemetadata.Attributes{unaggHr, unaggHr},
		},

		{
			name: "limit",
			entries: []insertEntry{
				{
					attr: unaggHr,
					meta: block.NewResultMetadata(),
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar"),
						notReadIt(ctrl, dp{t: step(2), val: 2}, "id1", "foo", "baz"),
					}, nil),
				},
			},
			limit: 1,
			expected: []expectedSeries{
				{
					tags: []string{"foo", "bar"},
					dps:  []dp{{t: step(1), val: 1}},
				},
			},
			exMeta:  nonExhaustiveMeta,
			exErr:   nil,
			exAttrs: []storagemetadata.Attributes{unaggHr},
		},

		{
			name: "limit can still update",
			entries: []insertEntry{
				{
					attr: unaggHr,
					meta: block.NewResultMetadata(),
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar"),
						it(ctrl, dp{t: step(1), val: 2}, "id1", "foo", "bar"),
						notReadIt(ctrl, dp{t: step(2), val: 2}, "id1", "foo", "baz"),
					}, nil),
				},
			},
			limit: 1,
			expected: []expectedSeries{
				{
					tags: []string{"foo", "bar"},
					dps:  []dp{{t: step(1), val: 2}},
				},
			},
			exMeta:  nonExhaustiveMeta,
			exErr:   nil,
			exAttrs: []storagemetadata.Attributes{unaggHr},
		},

		{
			name: "one iterator, mixed scenario",
			entries: []insertEntry{
				{
					attr: unaggHr,
					meta: warn1Meta,
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						// Same tags, different IDs.
						it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar", "qux", "quail"),
						it(ctrl, dp{t: step(2), val: 2}, "id2", "foo", "bar", "qux", "quail"),
						// Different tags, same IDs.
						it(ctrl, dp{t: step(1), val: 3}, "id3", "foo", "bar", "qux", "quart"),
						it(ctrl, dp{t: step(2), val: 4}, "id3", "foo", "bar", "qux", "quz"),
						// Same tags same IDs.
						it(ctrl, dp{t: step(1), val: 5}, "id4", "foo", "bar", "qux", "queen"),
						it(ctrl, dp{t: step(2), val: 6}, "id4", "foo", "bar", "qux", "queen"),
					}, nil),
				},
			},
			expected: []expectedSeries{
				{
					tags: []string{"foo", "bar", "qux", "quail"},
					dps:  []dp{{t: step(1), val: 1}, {t: step(2), val: 2}},
				},
				{
					tags: []string{"foo", "bar", "qux", "quart"},
					dps:  []dp{{t: step(1), val: 3}},
				},
				{
					tags: []string{"foo", "bar", "qux", "queen"},
					dps:  []dp{{t: step(1), val: 5}, {t: step(2), val: 6}},
				},
				{
					tags: []string{"foo", "bar", "qux", "quz"},
					dps:  []dp{{t: step(2), val: 4}},
				},
			},
			exMeta:  warn1Meta,
			exErr:   nil,
			exAttrs: []storagemetadata.Attributes{unaggHr, unaggHr, unaggHr, unaggHr},
		},

		{
			name: "multiple iterators, mixed scenario",
			entries: []insertEntry{
				{
					attr: unaggHr,
					meta: warn1Meta,
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar", "qux", "quail"),
						it(ctrl, dp{t: step(2), val: 2}, "id2", "foo", "bar", "qux", "quail"),
					}, nil),
				},
				{
					attr: unaggHr,
					meta: warn2Meta,
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 3}, "id3", "foo", "bar", "qux", "quart"),
						it(ctrl, dp{t: step(2), val: 4}, "id3", "foo", "bar", "qux", "quz"),
					}, nil),
				},
				{
					attr: unaggHr,
					meta: warn1Meta,
					err:  nil,
					iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
						it(ctrl, dp{t: step(1), val: 5}, "id4", "foo", "bar", "qux", "queen"),
						it(ctrl, dp{t: step(2), val: 6}, "id4", "foo", "bar", "qux", "queen"),
					}, nil),
				},
			},
			expected: []expectedSeries{
				{
					tags: []string{"foo", "bar", "qux", "quail"},
					dps:  []dp{{t: step(1), val: 1}, {t: step(2), val: 2}},
				},
				{
					tags: []string{"foo", "bar", "qux", "quart"},
					dps:  []dp{{t: step(1), val: 3}},
				},
				{
					tags: []string{"foo", "bar", "qux", "queen"},
					dps:  []dp{{t: step(1), val: 5}, {t: step(2), val: 6}},
				},
				{
					tags: []string{"foo", "bar", "qux", "quz"},
					dps:  []dp{{t: step(2), val: 4}},
				},
			},
			exMeta:  combinedMeta,
			exErr:   nil,
			exAttrs: []storagemetadata.Attributes{unaggHr, unaggHr, unaggHr, unaggHr},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testMultiFetchResultTagDedupeMap(t, ctrl, tt, models.NewTagOptions())
		})
	}
}

func testMultiFetchResultTagDedupeMap(
	t *testing.T,
	ctrl *gomock.Controller,
	test dedupeTest,
	tagOptions models.TagOptions,
) {
	require.True(t, len(test.entries) > 0,
		"must have more than 1 iterator in testMultiFetchResultTagDedupeMap")

	pools := generateIteratorPools(ctrl)
	opts := MatchOptions{
		MatchType: MatchTags,
	}

	limitOpts := LimitOptions{Limit: 1000}
	if test.limit > 0 {
		limitOpts.Limit = test.limit
	}
	r := NewMultiFetchResult(NamespaceCoversAllQueryRange, pools, opts, tagOptions, limitOpts)

	for _, entry := range test.entries {
		r.Add(MultiFetchResults{
			SeriesIterators: entry.iter,
			Metadata:        entry.meta,
			Attrs:           entry.attr,
			Err:             entry.err,
		})
	}

	result, attrs, err := r.FinalResultWithAttrs()
	require.NoError(t, err)

	test.exMeta.FetchedSeriesCount = len(test.expected)
	assert.Equal(t, test.exMeta, result.Metadata)
	require.Equal(t, len(test.exAttrs), len(attrs))
	for i, ex := range test.exAttrs {
		assert.Equal(t, ex, attrs[i])
	}

	c := len(result.SeriesIterators())
	require.Equal(t, len(test.expected), c)

	for i, ex := range test.expected {
		iter, tags, err := result.IterTagsAtIndex(i, tagOptions)
		require.NoError(t, err)

		exTags := models.MustMakeTags(ex.tags...)
		assert.Equal(t, exTags.String(), tags.String())
		for _, exDp := range ex.dps {
			require.True(t, iter.Next())
			dp, _, _ := iter.Current()
			assert.Equal(t, exDp.val, dp.Value)
			assert.Equal(t, exDp.t, dp.TimestampNanos)
		}
		assert.False(t, iter.Next())

		assert.NoError(t, iter.Err())
	}

	assert.NoError(t, r.Close())
}

func TestFilteredInsert(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	start := xtime.Now().Truncate(time.Hour)
	step := func(i int) xtime.UnixNano {
		return start.Add(time.Minute * time.Duration(i))
	}

	unaggHr := storagemetadata.Attributes{
		MetricsType: storagemetadata.UnaggregatedMetricsType,
		Resolution:  time.Hour,
	}

	warn1Meta := block.NewResultMetadata()
	warn1Meta.AddWarning("warn", "1")
	warn1Meta.FetchedSeriesCount = 1

	warn2Meta := block.NewResultMetadata()
	warn2Meta.AddWarning("warn", "2")
	warn2Meta.FetchedSeriesCount = 1

	dedupe := dedupeTest{
		name: "same tags, same ids",
		entries: []insertEntry{
			{
				attr: unaggHr,
				meta: warn1Meta,
				err:  nil,
				iter: encoding.NewSeriesIterators([]encoding.SeriesIterator{
					it(ctrl, dp{t: step(1), val: 1}, "id1", "foo", "bar"),
					notReadIt(ctrl, dp{t: step(5), val: 6}, "id1", "foo", "baz"),
				}, nil),
			},
		},
		expected: []expectedSeries{
			{
				tags: []string{"foo", "bar"},
				dps:  []dp{{t: step(1), val: 1}},
			},
		},
		exMeta:  warn1Meta,
		exErr:   nil,
		exAttrs: []storagemetadata.Attributes{unaggHr},
	}

	opts := models.NewTagOptions().SetFilters(models.Filters{
		models.Filter{Name: b("foo"), Values: [][]byte{b("baz")}},
	})

	testMultiFetchResultTagDedupeMap(t, ctrl, dedupe, opts)
}
