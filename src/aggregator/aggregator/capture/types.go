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

package capture

import (
	aggr "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
)

// Aggregator provide an aggregator for testing purposes.
type Aggregator interface {
	aggr.Aggregator

	// NumMetricsAdded returns the number of metrics added.
	NumMetricsAdded() int

	// Snapshot returns a copy of the aggregated data, resets
	// aggregations and number of metrics added.
	Snapshot() SnapshotResult
}

// SnapshotResult is the snapshot result.
type SnapshotResult struct {
	CountersWithMetadatas         []unaggregated.CounterWithMetadatas
	BatchTimersWithMetadatas      []unaggregated.BatchTimerWithMetadatas
	GaugesWithMetadatas           []unaggregated.GaugeWithMetadatas
	ForwardedMetricsWithMetadata  []aggregated.ForwardedMetricWithMetadata
	TimedMetricWithMetadata       []aggregated.TimedMetricWithMetadata
	PassThroughMetricWithMetadata []aggregated.TimedMetricWithMetadata
}
