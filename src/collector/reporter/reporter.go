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

package reporter

import (
	"time"

	"github.com/m3db/m3collector/backend"
	"github.com/m3db/m3collector/rules"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

// Reporter reports aggregated metrics.
type Reporter interface {
	// ReportCounter reports a counter metric.
	ReportCounter(id id.ID, value int64) error

	// ReportBatchTimer reports a batched timer metric.
	ReportBatchTimer(id id.ID, value []float64) error

	// ReportGauge reports a gauge metric.
	ReportGauge(id id.ID, value float64) error

	// Flush flushes any buffered metrics.
	Flush() error

	// Close closes the reporter.
	Close() error
}

type reporterMetrics struct {
	reportCounter    instrument.MethodMetrics
	reportBatchTimer instrument.MethodMetrics
	reportGauge      instrument.MethodMetrics
	flush            instrument.MethodMetrics
}

func newReporterMetrics(scope tally.Scope, samplingRate float64) reporterMetrics {
	return reporterMetrics{
		reportCounter:    instrument.NewMethodMetrics(scope, "report-counter", samplingRate),
		reportBatchTimer: instrument.NewMethodMetrics(scope, "report-batch-timer", samplingRate),
		reportGauge:      instrument.NewMethodMetrics(scope, "report-gauge", samplingRate),
		flush:            instrument.NewMethodMetrics(scope, "flush", samplingRate),
	}
}

type reporter struct {
	matcher         rules.Matcher
	server          backend.Server
	nowFn           clock.NowFn
	maxNegativeSkew time.Duration
	metrics         reporterMetrics
}

// NewReporter creates a new reporter.
func NewReporter(
	matcher rules.Matcher,
	server backend.Server,
	opts Options,
) Reporter {
	clockOpts := opts.ClockOptions()
	instrumentOpts := opts.InstrumentOptions()
	return &reporter{
		matcher:         matcher,
		server:          server,
		nowFn:           clockOpts.NowFn(),
		maxNegativeSkew: clockOpts.MaxNegativeSkew(),
		metrics:         newReporterMetrics(instrumentOpts.MetricsScope(), instrumentOpts.MetricsSamplingRate()),
	}
}

func (r *reporter) ReportCounter(id id.ID, value int64) error {
	callStart := r.nowFn()
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	earliestNanos := r.nowFn().Add(-r.maxNegativeSkew).UnixNano()
	if err := r.server.WriteCounterWithPoliciesList(
		id.Bytes(),
		value,
		matchRes.MappingsAt(earliestNanos),
	); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollup, tombstoned := matchRes.RollupsAt(idx, earliestNanos)
		if tombstoned {
			continue
		}
		if err := r.server.WriteCounterWithPoliciesList(rollup.ID, value, rollup.PoliciesList); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	err := multiErr.FinalError()
	r.metrics.reportCounter.ReportSuccessOrError(err, r.nowFn().Sub(callStart))
	return err
}

func (r *reporter) ReportBatchTimer(id id.ID, value []float64) error {
	callStart := r.nowFn()
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	earliestNanos := r.nowFn().Add(-r.maxNegativeSkew).UnixNano()
	if err := r.server.WriteBatchTimerWithPoliciesList(
		id.Bytes(),
		value,
		matchRes.MappingsAt(earliestNanos),
	); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollup, tombstoned := matchRes.RollupsAt(idx, earliestNanos)
		if tombstoned {
			continue
		}
		if err := r.server.WriteBatchTimerWithPoliciesList(rollup.ID, value, rollup.PoliciesList); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	err := multiErr.FinalError()
	r.metrics.reportBatchTimer.ReportSuccessOrError(err, r.nowFn().Sub(callStart))
	return err
}

func (r *reporter) ReportGauge(id id.ID, value float64) error {
	callStart := r.nowFn()
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	earliestNanos := r.nowFn().Add(-r.maxNegativeSkew).UnixNano()
	if err := r.server.WriteGaugeWithPoliciesList(
		id.Bytes(),
		value,
		matchRes.MappingsAt(earliestNanos),
	); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollup, tombstoned := matchRes.RollupsAt(idx, earliestNanos)
		if tombstoned {
			continue
		}
		if err := r.server.WriteGaugeWithPoliciesList(rollup.ID, value, rollup.PoliciesList); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	err := multiErr.FinalError()
	r.metrics.reportGauge.ReportSuccessOrError(err, r.nowFn().Sub(callStart))
	return err
}

func (r *reporter) Flush() error {
	callStart := r.nowFn()
	err := r.server.Flush()
	r.metrics.flush.ReportSuccessOrError(err, r.nowFn().Sub(callStart))
	return err
}

func (r *reporter) Close() error {
	multiErr := xerrors.NewMultiError()
	if err := r.server.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}
	if err := r.matcher.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}
