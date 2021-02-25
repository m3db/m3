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
	return NewQueryMetricsWithLabels(name, scope, nil)
}

// NewQueryMetricsWithLabels returns a new QueryMetrics with additional labels.
func NewQueryMetricsWithLabels(name string, scope tally.Scope, labels map[string]string) QueryMetrics {
	return QueryMetrics{
		ByDocs: NewDocCountMetrics(name, scope, labels),
	}
}

// QueryMetrics is a composite type of QueryDurationMetrics and QueryDocCountMetrics.
type QueryMetrics struct {
	ByDocs QueryDocCountMetrics
}

type docCountHist struct {
	threshold int
	timing    tally.Histogram
}

func newDocCountHist(
	threshold int,
	docCount string,
	scope tally.Scope,
	labels map[string]string,
) *docCountHist {
	if labels == nil {
		labels = make(map[string]string)
	}
	labels["count"] = docCount
	return &docCountHist{
		threshold: threshold,
		timing: scope.
			Tagged(labels).
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
func NewDocCountMetrics(metricType string, scope tally.Scope, labels map[string]string) QueryDocCountMetrics {
	scope = scope.SubScope(metricType).SubScope("doc_count")
	return &queryDocCount{
		docCountHists: []*docCountHist{
			newDocCountHist(50, "50", scope, labels),
			newDocCountHist(200, "200", scope, labels),
			newDocCountHist(1000, "1k", scope, labels),
			newDocCountHist(5000, "5k", scope, labels),
			newDocCountHist(10_000, "10k", scope, labels),
			newDocCountHist(50_000, "50k", scope, labels),
			newDocCountHist(100_000, "100k", scope, labels),
			newDocCountHist(250_000, "250k", scope, labels),
			newDocCountHist(500_000, "500k", scope, labels),
			newDocCountHist(750_000, "750k", scope, labels),
			newDocCountHist(1_000_000, "1M", scope, labels),
			newDocCountHist(int(math.MaxInt32), "max", scope, labels),
		},
	}
}

// QueryDocCountMetrics are metrics bucketed by the # of documents returned by a query result.
type QueryDocCountMetrics interface {
	// Record records the duration given their document count.
	Record(docCount int, duration time.Duration)
}
