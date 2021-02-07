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

package index

import (
	"math"
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/x/instrument"
)

// NewQueryMetrics returns a new QueryMetrics.
func NewQueryMetrics(name string, scope tally.Scope) QueryMetrics {
	return QueryMetrics{
		ByRange: NewQueryRangeMetrics(name, scope),
		ByDocs:  NewDocCountMetrics(name, scope),
	}
}

// QueryMetrics is a composite type of QueryDurationMetrics and QueryDocCountMetrics.
type QueryMetrics struct {
	ByRange QueryDurationMetrics
	ByDocs  QueryDocCountMetrics
}

type queryRangeHist struct {
	threshold time.Duration
	timing    tally.Histogram
}

func newQueryRangeHist(
	threshold time.Duration,
	value string,
	scope tally.Scope,
) *queryRangeHist {
	return &queryRangeHist{
		threshold: threshold,
		timing: scope.
			Tagged(map[string]string{"range": value}).
			Histogram("timing", instrument.SparseHistogramTimerHistogramBuckets()),
	}
}

type queryDurationMetrics struct {
	histograms []*queryRangeHist
}

func (bm *queryDurationMetrics) Record(
	queryRange time.Duration,
	queryRuntime time.Duration,
) {
	for _, m := range bm.histograms {
		if queryRange <= m.threshold {
			m.timing.RecordDuration(queryRuntime)
			return
		}
	}
}

// NewQueryRangeMetrics creates QueryDurationMetrics.
func NewQueryRangeMetrics(metricType string, scope tally.Scope) QueryDurationMetrics {
	scope = scope.SubScope(metricType).SubScope("query_range")
	return &queryDurationMetrics{
		histograms: []*queryRangeHist{
			newQueryRangeHist(time.Minute*5, "5m", scope),
			newQueryRangeHist(time.Minute*30, "30m", scope),
			newQueryRangeHist(time.Hour, "1hr", scope),
			newQueryRangeHist(time.Hour*6, "6hr", scope),
			newQueryRangeHist(time.Hour*24, "1d", scope),
			newQueryRangeHist(time.Hour*24*5, "5d", scope),
			newQueryRangeHist(time.Hour*24*10, "10d", scope),
			newQueryRangeHist(time.Hour*24*30, "30d", scope),
			newQueryRangeHist(time.Duration(math.MaxInt32), "max", scope),
		},
	}
}

// QueryDurationMetrics are timing metrics bucketed by the query window (start, end).
type QueryDurationMetrics interface {
	// Record records the duration for queries given their range.
	Record(queryRange time.Duration, duration time.Duration)
}

type docCountHist struct {
	threshold int
	timing    tally.Histogram
}

func newDocCountHist(
	threshold int,
	docCount string,
	scope tally.Scope,
) *docCountHist {
	return &docCountHist{
		threshold: threshold,
		timing: scope.
			Tagged(map[string]string{"count": docCount}).
			Histogram("timing", instrument.SparseHistogramTimerHistogramBuckets()),
	}
}

type queryDocCount struct {
	docCountHists []*docCountHist
}

func (bm *queryDocCount) Record(docCount int, duration time.Duration) {
	for _, m := range bm.docCountHists {
		if docCount <= m.threshold {
			m.timing.RecordDuration(duration)
			return
		}
	}
}

// NewDocCountMetrics creates QueryDocCountMetrics.
func NewDocCountMetrics(metricType string, scope tally.Scope) QueryDocCountMetrics {
	scope = scope.SubScope(metricType).SubScope("doc_count")
	return &queryDocCount{
		docCountHists: []*docCountHist{
			newDocCountHist(50, "50", scope),
			newDocCountHist(200, "200", scope),
			newDocCountHist(1000, "1k", scope),
			newDocCountHist(5000, "5k", scope),
			newDocCountHist(10_000, "10k", scope),
			newDocCountHist(50_000, "50k", scope),
			newDocCountHist(100_000, "100k", scope),
			newDocCountHist(250_000, "250k", scope),
			newDocCountHist(500_000, "500k", scope),
			newDocCountHist(750_000, "750k", scope),
			newDocCountHist(1_000_000, "1M", scope),
			newDocCountHist(int(math.MaxInt32), "max", scope),
		},
	}
}

// QueryDocCountMetrics are metrics bucketed by the # of documents returned by a query result.
type QueryDocCountMetrics interface {
	// Record records the duration given their document count.
	Record(docCount int, duration time.Duration)
}
