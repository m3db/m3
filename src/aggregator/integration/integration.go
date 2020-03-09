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

package integration

import (
	"time"

	"github.com/m3db/m3/src/metrics/metric/aggregated"
)

type conditionFn func() bool

func waitUntil(fn conditionFn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

// aggregatedMetric is useful for JSON comparison of expected metrics.
type aggregatedMetric struct {
	ID            string
	Type          string
	Time          time.Time
	Value         float64
	StoragePolicy string
}

func newAggregatedMetric(
	m aggregated.MetricWithStoragePolicy,
) aggregatedMetric {
	return aggregatedMetric{
		ID:            m.Metric.ID.String(),
		Type:          m.Metric.Type.String(),
		Time:          time.Unix(0, m.Metric.TimeNanos),
		Value:         m.Metric.Value,
		StoragePolicy: m.StoragePolicy.String(),
	}
}

func newAggregatedMetrics(
	m []aggregated.MetricWithStoragePolicy,
) []aggregatedMetric {
	results := make([]aggregatedMetric, 0, len(m))
	for _, m := range m {
		results = append(results, newAggregatedMetric(m))
	}
	return results
}
