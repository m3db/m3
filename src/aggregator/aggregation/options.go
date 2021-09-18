// Copyright (c) 2017 Uber Technologies, Inc.
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

package aggregation

import (
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
)

var (
	defaultHasExpensiveAggregations = false
)

// Options is the options for aggregations.
type Options struct {
	// Metrics is as set of aggregation metrics.
	Metrics Metrics
	// HasExpensiveAggregations means expensive (multiplication／division)
	// aggregation types are enabled.
	HasExpensiveAggregations bool
}

// Metrics is a set of metrics that can be used by elements.
type Metrics struct {
	Counter CounterMetrics
	Gauge   GaugeMetrics
}

// CounterMetrics is a set of counter metrics can be used by all counters.
type CounterMetrics struct {
	valuesOutOfOrder tally.Counter
}

// GaugeMetrics is a set of gauge metrics can be used by all gauges.
type GaugeMetrics struct {
	valuesOutOfOrder tally.Counter
}

// NewMetrics is a set of aggregation metrics.
func NewMetrics(scope tally.Scope) Metrics {
	scope = scope.SubScope("aggregation")
	return Metrics{
		Counter: newCounterMetrics(scope.SubScope("counters")),
		Gauge:   newGaugeMetrics(scope.SubScope("gauges")),
	}
}

func newCounterMetrics(scope tally.Scope) CounterMetrics {
	return CounterMetrics{
		valuesOutOfOrder: scope.Counter("values-out-of-order"),
	}
}

// IncValuesOutOfOrder increments value or if not initialized is a no-op.
func (m CounterMetrics) IncValuesOutOfOrder() {
	if m.valuesOutOfOrder != nil {
		m.valuesOutOfOrder.Inc(1)
	}
}

func newGaugeMetrics(scope tally.Scope) GaugeMetrics {
	return GaugeMetrics{
		valuesOutOfOrder: scope.Counter("values-out-of-order"),
	}
}

// IncValuesOutOfOrder increments value or if not initialized is a no-op.
func (m GaugeMetrics) IncValuesOutOfOrder() {
	if m.valuesOutOfOrder != nil {
		m.valuesOutOfOrder.Inc(1)
	}
}

// NewOptions creates a new aggregation options.
func NewOptions(instrumentOpts instrument.Options) Options {
	return Options{
		HasExpensiveAggregations: defaultHasExpensiveAggregations,
		Metrics:                  NewMetrics(instrumentOpts.MetricsScope()),
	}
}

// ResetSetData resets the aggregation options.
func (o *Options) ResetSetData(aggTypes aggregation.Types) {
	o.HasExpensiveAggregations = isExpensive(aggTypes)
}
