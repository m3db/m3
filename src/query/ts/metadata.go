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

package ts

// M3MetricType is the enum for M3 metric types.
type M3MetricType int

const (
	// M3MetricTypeGauge is the gauge metric type.
	M3MetricTypeGauge M3MetricType = iota

	// M3MetricTypeCounter is the counter metric type.
	M3MetricTypeCounter

	// M3MetricTypeTimer is the timer metric type.
	M3MetricTypeTimer
)

// PromMetricType is the enum for Prometheus metric types.
type PromMetricType int

const (
	// PromMetricTypeUnknown is the unknown Prometheus metric type.
	PromMetricTypeUnknown PromMetricType = iota

	// PromMetricTypeUnknown is the counter Prometheus metric type.
	PromMetricTypeCounter

	// PromMetricTypeUnknown is the gauge Prometheus metric type.
	PromMetricTypeGauge

	// PromMetricTypeUnknown is the histogram Prometheus metric type.
	PromMetricTypeHistogram

	// PromMetricTypeUnknown is the gauge histogram Prometheus metric type.
	PromMetricTypeGaugeHistogram

	// PromMetricTypeUnknown is the summary Prometheus metric type.
	PromMetricTypeSummary

	// PromMetricTypeUnknown is the info Prometheus metric type.
	PromMetricTypeInfo

	// PromMetricTypeUnknown is the state set Prometheus metric type.
	PromMetricTypeStateSet
)

// SourceType is the enum for metric source types.
type SourceType int

const (
	// SourceTypePrometheus is the prometheus source type.
	SourceTypePrometheus SourceType = iota

	// SourceTypeGraphite is the graphite source type.
	SourceTypeGraphite
)

// SeriesAttributes has attributes about the time series.
type SeriesAttributes struct {
	M3Type   M3MetricType
	PromType PromMetricType
	Source   SourceType
}

// DefaultSeriesAttributes returns a default series attributes.
func DefaultSeriesAttributes() SeriesAttributes {
	return SeriesAttributes{
		M3Type:   M3MetricTypeGauge,
		PromType: PromMetricTypeUnknown,
		Source:   SourceTypePrometheus,
	}
}

// Metadata is metadata associated with a time series.
type Metadata struct {
	DropUnaggregated bool
}
