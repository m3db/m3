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

package aggregated

import (
	"fmt"
	"time"

	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
)

// Metric is a metric, which is essentially a named value at certain time.
type Metric struct {
	ID        id.RawID
	TimeNanos int64
	Value     float64
}

// String is the string representation of a metric.
func (m Metric) String() string {
	return fmt.Sprintf(
		"{id:%s,timestamp:%s,value:%f}",
		m.ID.String(),
		time.Unix(0, m.TimeNanos).String(),
		m.Value,
	)
}

// ChunkedMetric is a metric with a chunked ID.
type ChunkedMetric struct {
	id.ChunkedID
	TimeNanos int64
	Value     float64
}

// RawMetric is a metric in its raw form (e.g., encoded bytes associated with
// a metric object).
type RawMetric interface {
	// ID is the metric identifier.
	ID() (id.RawID, error)

	// TimeNanos is the metric timestamp in nanoseconds.
	TimeNanos() (int64, error)

	// Value is the metric value.
	Value() (float64, error)

	// Metric is the metric object represented by the raw metric.
	Metric() (Metric, error)

	// Bytes are the bytes backing this raw metric.
	Bytes() []byte

	// Reset resets the raw data.
	Reset(data []byte)
}

// MetricWithStoragePolicy is a metric with applicable storage policy.
type MetricWithStoragePolicy struct {
	Metric
	policy.StoragePolicy
}

// String is the string representation of a metric with storage policy.
func (mp MetricWithStoragePolicy) String() string {
	return fmt.Sprintf("{metric:%s,policy:%s}", mp.Metric.String(), mp.StoragePolicy.String())
}

// ChunkedMetricWithStoragePolicy is a chunked metric with applicable storage policy.
type ChunkedMetricWithStoragePolicy struct {
	ChunkedMetric
	policy.StoragePolicy
}

// RawMetricWithStoragePolicy is a raw metric with applicable storage policy.
type RawMetricWithStoragePolicy struct {
	RawMetric
	policy.StoragePolicy
}
