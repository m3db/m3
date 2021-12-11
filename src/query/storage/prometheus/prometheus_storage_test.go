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

package prometheus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/prometheus/prometheus/pkg/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectWithMetaInContext(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var resMutex sync.Mutex
	res := block.NewResultMetadata()
	resultMetadataReceiveFn := func(m block.ResultMetadata) {
		resMutex.Lock()
		defer resMutex.Unlock()
		res = res.CombineMetadata(m)
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, FetchOptionsContextKey, storage.NewFetchOptions())
	ctx = context.WithValue(ctx, BlockResultMetadataFnKey, resultMetadataReceiveFn)

	store := storage.NewMockStorage(ctrl)
	opts := PrometheusOptions{
		Storage:           store,
		InstrumentOptions: instrument.NewOptions(),
	}

	queryable := NewPrometheusQueryable(opts)
	q, err := queryable.Querier(ctx, 0, 0)
	require.NoError(t, err)

	start := time.Now().Truncate(time.Hour)
	end := start.Add(time.Hour)
	step := int64(time.Second)

	hints := &promstorage.SelectHints{
		Start: start.Unix() * 1000,
		End:   end.Unix() * 1000,

		Step: step / int64(time.Millisecond),
	}

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
		labels.MustNewMatcher(labels.MatchRegexp, "qux", "q.z"),
	}

	m, err := models.NewMatcher(models.MatchEqual, []byte("foo"), []byte("bar"))
	require.NoError(t, err)
	m2, err := models.NewMatcher(models.MatchRegexp, []byte("qux"), []byte("q.z"))
	require.NoError(t, err)

	exQuery := &storage.FetchQuery{
		TagMatchers: models.Matchers{m, m2},
		Start:       start,
		End:         end,
		Interval:    time.Duration(step),
	}

	meta := block.NewResultMetadata()
	meta.AddWarning("warn", "warning")
	store.EXPECT().FetchProm(ctx, exQuery, gomock.Any()).DoAndReturn(
		func(_ context.Context, arg1 *storage.FetchQuery, _ *storage.FetchOptions) (storage.PromResult, error) {
			return storage.PromResult{
				Metadata: meta,
				PromResult: &prompb.QueryResult{
					Timeseries: []*prompb.TimeSeries{
						{
							Labels: []prompb.Label{
								{Name: []byte("foo"), Value: []byte("bar")},
								{Name: []byte("qux"), Value: []byte("qzz")},
							},
							Samples: []prompb.Sample{
								prompb.Sample{Value: 1, Timestamp: 100},
							},
						},
						{
							Labels: []prompb.Label{
								{Name: []byte("foo"), Value: []byte("bar")},
								{Name: []byte("qux"), Value: []byte("qaz")},
							},
							Samples: []prompb.Sample{
								prompb.Sample{Value: 100, Timestamp: 200},
							},
						},
					},
				},
			}, nil
		})

	series := q.Select(true, hints, matchers...)
	warnings := series.Warnings()
	assert.NoError(t, series.Err())

	type dp struct {
		v float64
		t int64
	}

	acDp := make([][]dp, 0, 2)
	acTags := make([]string, 0, 2)
	for series.Next() {
		curr := series.At()
		acTags = append(acTags, curr.Labels().String())
		it := curr.Iterator()
		ac := make([]dp, 0, 1)
		for it.Next() {
			t, v := it.At()
			ac = append(ac, dp{t: t, v: v})
		}

		assert.NoError(t, it.Err())
		acDp = append(acDp, ac)
	}

	assert.NoError(t, series.Err())

	exDp := [][]dp{{{v: 100, t: 200}}, {{v: 1, t: 100}}}
	exTags := []string{`{foo="bar", qux="qaz"}`, `{foo="bar", qux="qzz"}`}
	assert.Equal(t, exTags, acTags)
	assert.Equal(t, exDp, acDp)
	require.Equal(t, 1, len(warnings))
	require.EqualError(t, warnings[0], "warn_warning")
	require.NoError(t, q.Close())

	// NB: assert warnings on context were propagated.
	assert.Equal(t, []string{"warn_warning"}, res.WarningStrings())
}
