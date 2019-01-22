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

package graphite

import (
	"testing"

	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
)

func TestConsolidationFuncForMetricType(t *testing.T) {
	ca := ConsolidationFuncForMetricType(Counts)
	assert.Equal(t, ts.ConsolidationSum, ca)

	ca = ConsolidationFuncForMetricType(Timers)
	assert.Equal(t, ts.ConsolidationAvg, ca)

	ca = ConsolidationFuncForMetricType(Gauges)
	assert.Equal(t, ts.ConsolidationAvg, ca)

	ca = ConsolidationFuncForMetricType(Ratios)
	assert.Equal(t, ts.ConsolidationAvg, ca)

	ca = ConsolidationFuncForMetricType(MetricType(999))
	assert.Equal(t, ts.ConsolidationAvg, ca)
}

func TestMetricTypeFromID(t *testing.T) {
	assert.Equal(t, metric.UnknownType, MetricTypeFromID("foo.bar.baz.qux.qaz"))
	assert.Equal(t, metric.CounterType, MetricTypeFromID("foo.bar.counts.qaz=bar"))
	assert.Equal(t, metric.GaugeType, MetricTypeFromID("foo.bar.gauges.qux=bar"))
	assert.Equal(t, metric.TimerType, MetricTypeFromID("foo.bar.timers.baz.p99"))
}
