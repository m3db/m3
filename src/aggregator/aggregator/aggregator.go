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

package aggregator

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

var (
	errAggregatorClosed  = errors.New("aggregator is closed")
	errInvalidMetricType = errors.New("invalid metric type")
)

type aggregatorMetrics struct {
	tickDuration              tally.Timer
	tickExpired               tally.Counter
	counters                  tally.Counter
	timers                    tally.Counter
	gauges                    tally.Counter
	invalidMetricTypes        tally.Counter
	addMetricWithPoliciesList instrument.MethodMetrics
}

func newAggregatorMetrics(scope tally.Scope, samplingRate float64) aggregatorMetrics {
	tickScope := scope.SubScope("tick")
	return aggregatorMetrics{
		tickDuration:              tickScope.Timer("duration"),
		tickExpired:               tickScope.Counter("expired"),
		counters:                  scope.Counter("counters"),
		timers:                    scope.Counter("timers"),
		gauges:                    scope.Counter("gauges"),
		invalidMetricTypes:        scope.Counter("invalid-metric-types"),
		addMetricWithPoliciesList: instrument.NewMethodMetrics(scope, "addMetricWithPoliciesList", samplingRate),
	}
}

type addMetricWithPoliciesListFn func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error

type waitForFn func(d time.Duration) <-chan time.Time

// aggregator stores aggregations of different types of metrics (e.g., counter,
// timer, gauges) and periodically flushes them out.
type aggregator struct {
	opts                        Options
	nowFn                       clock.NowFn
	checkInterval               time.Duration
	closed                      int32
	doneCh                      chan struct{}
	wg                          sync.WaitGroup
	metricMap                   *metricMap
	addMetricWithPoliciesListFn addMetricWithPoliciesListFn
	waitForFn                   waitForFn
	metrics                     aggregatorMetrics
}

// NewAggregator creates a new aggregator.
func NewAggregator(opts Options) Aggregator {
	doneCh := make(chan struct{})
	iOpts := opts.InstrumentOptions()
	agg := &aggregator{
		opts:          opts,
		nowFn:         opts.ClockOptions().NowFn(),
		checkInterval: opts.EntryCheckInterval(),
		doneCh:        doneCh,
		metricMap:     newMetricMap(opts),
		metrics:       newAggregatorMetrics(iOpts.MetricsScope(), iOpts.MetricsSamplingRate()),
	}
	agg.addMetricWithPoliciesListFn = agg.metricMap.AddMetricWithPoliciesList
	agg.waitForFn = time.After

	if agg.checkInterval > 0 {
		agg.wg.Add(1)
		go agg.tick()
	}

	agg.wg.Add(1)
	go agg.reportMetrics()

	return agg
}

func (agg *aggregator) AddMetricWithPoliciesList(
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	callStart := agg.nowFn()
	if atomic.LoadInt32(&agg.closed) == 1 {
		agg.metrics.addMetricWithPoliciesList.ReportError(agg.nowFn().Sub(callStart))
		return errAggregatorClosed
	}
	switch mu.Type {
	case unaggregated.CounterType:
		agg.metrics.counters.Inc(1)
	case unaggregated.BatchTimerType:
		agg.metrics.timers.Inc(1)
	case unaggregated.GaugeType:
		agg.metrics.gauges.Inc(1)
	default:
		agg.metrics.invalidMetricTypes.Inc(1)
		agg.metrics.addMetricWithPoliciesList.ReportError(agg.nowFn().Sub(callStart))
		return errInvalidMetricType
	}
	err := agg.addMetricWithPoliciesListFn(mu, pl)
	agg.metrics.addMetricWithPoliciesList.ReportSuccessOrError(err, agg.nowFn().Sub(callStart))
	return err
}

func (agg *aggregator) Close() {
	if !atomic.CompareAndSwapInt32(&agg.closed, 0, 1) {
		return
	}

	// Waiting for the ticking goroutine to return.
	close(agg.doneCh)
	agg.wg.Wait()

	// Closing metric map.
	agg.metricMap.Close()
}

func (agg *aggregator) tick() {
	defer agg.wg.Done()

	for {
		select {
		case <-agg.doneCh:
			return
		default:
			agg.tickInternal()
		}
	}
}

func (agg *aggregator) tickInternal() {
	start := agg.nowFn()
	tickExpired := agg.metricMap.DeleteExpired(agg.checkInterval)
	tickDuration := agg.nowFn().Sub(start)
	agg.metrics.tickExpired.Inc(tickExpired)
	agg.metrics.tickDuration.Record(tickDuration)
	if tickDuration < agg.checkInterval {
		// NB(xichen): use a channel here instead of sleeping in case
		// server needs to close and we don't tick frequently enough.
		select {
		case <-agg.waitForFn(agg.checkInterval - tickDuration):
		case <-agg.doneCh:
		}
	}
}

func (agg *aggregator) reportMetrics() {
	defer agg.wg.Done()

	reportInterval := agg.opts.InstrumentOptions().ReportInterval()
	t := time.NewTicker(reportInterval)

	for {
		select {
		case <-t.C:
			agg.metricMap.reportMetrics()
		case <-agg.doneCh:
			t.Stop()
			return
		}
	}
}
