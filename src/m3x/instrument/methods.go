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

package instrument

import (
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
)

var (
	nullStopWatchStart tally.Stopwatch
)

// sampledTimer is a sampled timer that implements the tally timer interface.
// NB(xichen): the sampling logic should eventually be implemented in tally.
type sampledTimer struct {
	tally.Timer

	cnt  uint64
	rate uint64
}

// NB(xichen): return an error instead of panicing.
func newSampledTimer(base tally.Timer, rate float64) tally.Timer {
	if rate <= 0.0 || rate > 1.0 {
		panic("sampled timer must have a sampling rate between 0.0 and 1.0")
	}
	return &sampledTimer{
		Timer: base,
		rate:  uint64(1.0 / rate),
	}
}

func (t *sampledTimer) shouldSample() bool {
	return atomic.AddUint64(&t.cnt, 1)%t.rate == 0
}

func (t *sampledTimer) Start() tally.Stopwatch {
	if !t.shouldSample() {
		return nullStopWatchStart
	}
	return t.Timer.Start()
}

func (t *sampledTimer) Stop(startTime tally.Stopwatch) {
	if startTime == nullStopWatchStart {
		// If startTime is nullStopWatchStart, do nothing.
		return
	}
	startTime.Stop()
}

func (t *sampledTimer) Record(d time.Duration) {
	if !t.shouldSample() {
		return
	}
	t.Timer.Record(d)
}

// MethodMetrics is a bundle of common metrics with a uniform naming scheme.
type MethodMetrics struct {
	Errors         tally.Counter
	Success        tally.Counter
	ErrorsLatency  tally.Timer
	SuccessLatency tally.Timer
}

// ReportSuccess reports a success.
func (m *MethodMetrics) ReportSuccess(d time.Duration) {
	m.Success.Inc(1)
	m.SuccessLatency.Record(d)
}

// ReportError reports an error.
func (m *MethodMetrics) ReportError(d time.Duration) {
	m.Errors.Inc(1)
	m.ErrorsLatency.Record(d)
}

// ReportSuccessOrError increments Error/Success counter dependending on the error.
func (m *MethodMetrics) ReportSuccessOrError(e error, d time.Duration) {
	if e != nil {
		m.ReportError(d)
	} else {
		m.ReportSuccess(d)
	}
}

// NewMethodMetrics returns a new Method metrics for the given method name.
func NewMethodMetrics(scope tally.Scope, methodName string, samplingRate float64) MethodMetrics {
	return MethodMetrics{
		Errors:         scope.Counter(methodName + ".errors"),
		Success:        scope.Counter(methodName + ".success"),
		ErrorsLatency:  newSampledTimer(scope.Timer(methodName+".errors-latency"), samplingRate),
		SuccessLatency: newSampledTimer(scope.Timer(methodName+".success-latency"), samplingRate),
	}
}

// BatchMethodMetrics is a bundle of common metrics for methods with batch semantics.
type BatchMethodMetrics struct {
	RetryableErrors    tally.Counter
	NonRetryableErrors tally.Counter
	Errors             tally.Counter
	Success            tally.Counter
	Latency            tally.Timer
}

// NewBatchMethodMetrics creates new batch method metrics.
func NewBatchMethodMetrics(
	scope tally.Scope,
	methodName string,
	samplingRate float64,
) BatchMethodMetrics {
	return BatchMethodMetrics{
		RetryableErrors:    scope.Counter(methodName + ".retryable-errors"),
		NonRetryableErrors: scope.Counter(methodName + ".non-retryable-errors"),
		Errors:             scope.Counter(methodName + ".errors"),
		Success:            scope.Counter(methodName + ".success"),
		Latency:            newSampledTimer(scope.Timer(methodName+".latency"), samplingRate),
	}
}

// ReportSuccess reports successess.
func (m *BatchMethodMetrics) ReportSuccess(n int) {
	m.Success.Inc(int64(n))
}

// ReportRetryableErrors reports retryable errors.
func (m *BatchMethodMetrics) ReportRetryableErrors(n int) {
	m.RetryableErrors.Inc(int64(n))
	m.Errors.Inc(int64(n))
}

// ReportNonRetryableErrors reports non-retryable errors.
func (m *BatchMethodMetrics) ReportNonRetryableErrors(n int) {
	m.NonRetryableErrors.Inc(int64(n))
	m.Errors.Inc(int64(n))
}

// ReportLatency reports latency.
func (m *BatchMethodMetrics) ReportLatency(d time.Duration) {
	m.Latency.Record(d)
}
