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

package m3aggregator

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/aggregator/client"
	creporter "github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errReporterAlreadyClosed = errors.New("reporter is already closed")
)

type reporterMetrics struct {
	reportCounter    instrument.MethodMetrics
	reportBatchTimer instrument.MethodMetrics
	reportGauge      instrument.MethodMetrics
	reportPending    tally.Gauge
	flush            instrument.MethodMetrics

	counterWritesCounter   tally.Counter
	r2counterWritesCounter tally.Counter
	timerWritesCounter     tally.Counter
	r2timerWritesCounter   tally.Counter
	gaugeWritesCounter     tally.Counter
	r2gaugeWritesCounter   tally.Counter
}

func newReporterMetrics(instrumentOpts instrument.Options) reporterMetrics {
	scope := instrumentOpts.MetricsScope()
	timerOpts := instrumentOpts.TimerOptions()
	hostName := "unknown"
	if name, err := os.Hostname(); err == nil {
		hostName = name
	} else {
		instrumentOpts.Logger().Warn("unable to determine hostname when creating reporter", zap.Error(err))
	}
	hostScope := scope.Tagged(map[string]string{"host": hostName})
	return reporterMetrics{
		reportCounter:    instrument.NewMethodMetrics(scope, "report-counter", timerOpts),
		reportBatchTimer: instrument.NewMethodMetrics(scope, "report-batch-timer", timerOpts),
		reportGauge:      instrument.NewMethodMetrics(scope, "report-gauge", timerOpts),
		flush:            instrument.NewMethodMetrics(scope, "flush", timerOpts),
		reportPending:    hostScope.Gauge("report-pending"),
		counterWritesCounter: scope.Tagged(map[string]string{
			"metric_type": "counter",
		}).Counter("writes"),
		r2counterWritesCounter: scope.Tagged(map[string]string{
			"metric_type": "counter",
			"r2":          "true",
		}).Counter("writes"),
		timerWritesCounter: scope.Tagged(map[string]string{
			"metric_type": "timer",
		}).Counter("writes"),
		r2timerWritesCounter: scope.Tagged(map[string]string{
			"metric_type": "timer",
			"r2":          "true",
		}).Counter("writes"),
		gaugeWritesCounter: scope.Tagged(map[string]string{
			"metric_type": "gauge",
		}).Counter("writes"),
		r2gaugeWritesCounter: scope.Tagged(map[string]string{
			"metric_type": "gauge",
			"r2":          "true",
		}).Counter("writes"),
	}
}

type reporter struct {
	matcher         matcher.Matcher
	client          client.Client
	nowFn           clock.NowFn
	maxPositiveSkew time.Duration
	maxNegativeSkew time.Duration
	reportInterval  time.Duration

	closed        int32
	doneCh        chan struct{}
	wg            sync.WaitGroup
	reportPending int64
	metrics       reporterMetrics
}

// NewReporter creates a new reporter.
func NewReporter(
	matcher matcher.Matcher,
	client client.Client,
	opts ReporterOptions,
) creporter.Reporter {
	clockOpts := opts.ClockOptions()
	instrumentOpts := opts.InstrumentOptions()
	r := &reporter{
		matcher:         matcher,
		client:          client,
		nowFn:           clockOpts.NowFn(),
		maxPositiveSkew: clockOpts.MaxPositiveSkew(),
		maxNegativeSkew: clockOpts.MaxNegativeSkew(),
		reportInterval:  instrumentOpts.ReportInterval(),
		doneCh:          make(chan struct{}),
		metrics:         newReporterMetrics(instrumentOpts),
	}

	r.wg.Add(1)
	go r.reportMetrics()
	return r
}

func (r *reporter) ReportCounter(id id.ID, value int64) error {
	var (
		reportAt  = r.nowFn()
		fromNanos = reportAt.Add(-r.maxNegativeSkew).UnixNano()
		toNanos   = reportAt.Add(r.maxPositiveSkew).UnixNano()
		multiErr  = xerrors.NewMultiError()
	)

	r.incrementReportPending()

	var (
		counter         = unaggregated.Counter{ID: id.Bytes(), Value: value}
		matchResult     = r.matcher.ForwardMatch(id, fromNanos, toNanos)
		numNewIDs       = matchResult.NumNewRollupIDs()
		stagedMetadatas = matchResult.ForExistingIDAt(fromNanos)
		hasDropPolicy   = stagedMetadatas.IsDropPolicyApplied()
		dropOriginal    = numNewIDs > 0 && (!matchResult.KeepOriginal() || hasDropPolicy)
	)

	r.metrics.counterWritesCounter.Inc(1)
	r.metrics.r2counterWritesCounter.Inc(int64(numNewIDs))

	if !dropOriginal {
		err := r.client.WriteUntimedCounter(counter, stagedMetadatas)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	for idx := 0; idx < matchResult.NumNewRollupIDs(); idx++ {
		var (
			rollupIDWithMetadatas = matchResult.ForNewRollupIDsAt(idx, fromNanos)
			rollupID              = rollupIDWithMetadatas.ID
			metadatas             = rollupIDWithMetadatas.Metadatas
		)
		if isTombstoned(metadatas, fromNanos) {
			continue
		}
		newRollupCounter := unaggregated.Counter{ID: rollupID, Value: value}
		if err := r.client.WriteUntimedCounter(newRollupCounter, metadatas); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	err := multiErr.FinalError()
	r.metrics.reportCounter.ReportSuccessOrError(err, r.nowFn().Sub(reportAt))
	r.decrementReportPending()
	return err
}

func (r *reporter) ReportBatchTimer(id id.ID, value []float64) error {
	var (
		reportAt  = r.nowFn()
		fromNanos = reportAt.Add(-r.maxNegativeSkew).UnixNano()
		toNanos   = reportAt.Add(r.maxPositiveSkew).UnixNano()
		multiErr  = xerrors.NewMultiError()
	)

	r.incrementReportPending()

	var (
		batchTimer      = unaggregated.BatchTimer{ID: id.Bytes(), Values: value}
		matchResult     = r.matcher.ForwardMatch(id, fromNanos, toNanos)
		numNewIDs       = matchResult.NumNewRollupIDs()
		stagedMetadatas = matchResult.ForExistingIDAt(fromNanos)
		hasDropPolicy   = stagedMetadatas.IsDropPolicyApplied()
		dropOriginal    = numNewIDs > 0 && (!matchResult.KeepOriginal() || hasDropPolicy)
	)

	r.metrics.timerWritesCounter.Inc(1)
	r.metrics.r2timerWritesCounter.Inc(int64(numNewIDs))

	if !dropOriginal {
		err := r.client.WriteUntimedBatchTimer(batchTimer, stagedMetadatas)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	for idx := 0; idx < matchResult.NumNewRollupIDs(); idx++ {
		var (
			rollupIDWithMetadatas = matchResult.ForNewRollupIDsAt(idx, fromNanos)
			rollupID              = rollupIDWithMetadatas.ID
			metadatas             = rollupIDWithMetadatas.Metadatas
		)
		if isTombstoned(metadatas, fromNanos) {
			continue
		}
		newRollupTimer := unaggregated.BatchTimer{ID: rollupID, Values: value}
		if err := r.client.WriteUntimedBatchTimer(newRollupTimer, metadatas); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	err := multiErr.FinalError()
	r.metrics.reportBatchTimer.ReportSuccessOrError(err, r.nowFn().Sub(reportAt))
	r.decrementReportPending()
	return err
}

func (r *reporter) ReportGauge(id id.ID, value float64) error {
	var (
		reportAt  = r.nowFn()
		fromNanos = reportAt.Add(-r.maxNegativeSkew).UnixNano()
		toNanos   = reportAt.Add(r.maxPositiveSkew).UnixNano()
		multiErr  = xerrors.NewMultiError()
	)

	r.incrementReportPending()

	var (
		gauge           = unaggregated.Gauge{ID: id.Bytes(), Value: value}
		matchResult     = r.matcher.ForwardMatch(id, fromNanos, toNanos)
		numNewIDs       = matchResult.NumNewRollupIDs()
		stagedMetadatas = matchResult.ForExistingIDAt(fromNanos)
		hasDropPolicy   = stagedMetadatas.IsDropPolicyApplied()
		dropOriginal    = numNewIDs > 0 && (!matchResult.KeepOriginal() || hasDropPolicy)
	)

	r.metrics.gaugeWritesCounter.Inc(1)
	r.metrics.r2gaugeWritesCounter.Inc(int64(numNewIDs))

	if !dropOriginal {
		err := r.client.WriteUntimedGauge(gauge, stagedMetadatas)
		if err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	for idx := 0; idx < matchResult.NumNewRollupIDs(); idx++ {
		var (
			rollupIDWithMetadatas = matchResult.ForNewRollupIDsAt(idx, fromNanos)
			rollupID              = rollupIDWithMetadatas.ID
			metadatas             = rollupIDWithMetadatas.Metadatas
		)
		if isTombstoned(metadatas, fromNanos) {
			continue
		}
		newRollupGauge := unaggregated.Gauge{ID: rollupID, Value: value}
		if err := r.client.WriteUntimedGauge(newRollupGauge, metadatas); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	err := multiErr.FinalError()
	r.metrics.reportGauge.ReportSuccessOrError(err, r.nowFn().Sub(reportAt))
	r.decrementReportPending()
	return err
}

func (r *reporter) Flush() error {
	callStart := r.nowFn()
	err := r.client.Flush()
	r.metrics.flush.ReportSuccessOrError(err, r.nowFn().Sub(callStart))
	return err
}

func (r *reporter) Close() error {
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return errReporterAlreadyClosed
	}
	close(r.doneCh)
	multiErr := xerrors.NewMultiError()
	if err := r.client.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}
	if err := r.matcher.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}
	r.wg.Wait()
	return multiErr.FinalError()
}

func (r *reporter) currentReportPending() int64 { return atomic.LoadInt64(&r.reportPending) }
func (r *reporter) incrementReportPending()     { atomic.AddInt64(&r.reportPending, 1) }
func (r *reporter) decrementReportPending()     { atomic.AddInt64(&r.reportPending, -1) }

func (r *reporter) reportMetrics() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.reportInterval)
	for {
		select {
		case <-ticker.C:
			r.metrics.reportPending.Update(float64(r.currentReportPending()))
		case <-r.doneCh:
			ticker.Stop()
			return
		}
	}
}

// isTombstoned checks to see if the last metadata is currently active and indicates
// the metric ID has been tombstoned. This is a small optimization so that we don't
// send tombstoned rollup metrics to the m3aggregator only to be rejected there to
// save CPU cycles and network bandwidth. This optimization doesn't affect correctness.
func isTombstoned(
	metadatas metadata.StagedMetadatas,
	fromNanos int64,
) bool {
	if len(metadatas) == 0 {
		return false
	}
	lastMetadata := metadatas[len(metadatas)-1]
	return lastMetadata.CutoverNanos <= fromNanos && lastMetadata.Tombstoned
}
