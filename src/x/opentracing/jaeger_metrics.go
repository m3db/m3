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

package opentracing

import (
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/jaeger-lib/metrics"
)

// jaegerMetricsFactory adapts a tally/v4 Scope to a jaeger-lib metrics.Factory.
// It is a port of github.com/uber/jaeger-lib/metrics/tally, which is pinned to
// tally v3 and cannot accept a v4 scope.
type jaegerMetricsFactory struct {
	scope tally.Scope
}

func newJaegerMetricsFactory(scope tally.Scope) metrics.Factory {
	return &jaegerMetricsFactory{scope: scope}
}

func (f *jaegerMetricsFactory) tagged(tags map[string]string) tally.Scope {
	if len(tags) == 0 {
		return f.scope
	}
	return f.scope.Tagged(tags)
}

func (f *jaegerMetricsFactory) Counter(options metrics.Options) metrics.Counter {
	return jaegerCounter{f.tagged(options.Tags).Counter(options.Name)}
}

func (f *jaegerMetricsFactory) Gauge(options metrics.Options) metrics.Gauge {
	return jaegerGauge{f.tagged(options.Tags).Gauge(options.Name)}
}

func (f *jaegerMetricsFactory) Timer(options metrics.TimerOptions) metrics.Timer {
	return jaegerTimer{f.tagged(options.Tags).Timer(options.Name)}
}

func (f *jaegerMetricsFactory) Histogram(options metrics.HistogramOptions) metrics.Histogram {
	return jaegerHistogram{
		f.tagged(options.Tags).Histogram(options.Name, tally.ValueBuckets(options.Buckets)),
	}
}

func (f *jaegerMetricsFactory) Namespace(scope metrics.NSOptions) metrics.Factory {
	return &jaegerMetricsFactory{scope: f.scope.SubScope(scope.Name).Tagged(scope.Tags)}
}

type jaegerCounter struct{ counter tally.Counter }

func (c jaegerCounter) Inc(delta int64) { c.counter.Inc(delta) }

type jaegerGauge struct{ gauge tally.Gauge }

func (g jaegerGauge) Update(value int64) { g.gauge.Update(float64(value)) }

type jaegerTimer struct{ timer tally.Timer }

func (t jaegerTimer) Record(delta time.Duration) { t.timer.Record(delta) }

type jaegerHistogram struct{ histogram tally.Histogram }

func (h jaegerHistogram) Record(value float64) { h.histogram.RecordValue(value) }
