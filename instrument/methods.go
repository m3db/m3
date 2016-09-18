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
	nullStopWatchStart tally.StopwatchStart
)

// sampledTimer is a sampled timer that implements the tally timer interface
// NB(xichen): the sampling logic should eventually be implemented in tally
type sampledTimer struct {
	tally.Timer

	cnt  uint64
	rate uint64
}

// NB(xichen): return an error instead of panicing
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

func (t *sampledTimer) Start() tally.StopwatchStart {
	if !t.shouldSample() {
		return nullStopWatchStart
	}
	return t.Timer.Start()
}

func (t *sampledTimer) Stop(startTime tally.StopwatchStart) {
	if startTime == nullStopWatchStart {
		// If startTime is nullStopWatchStart, do nothing
		return
	}
	t.Timer.Stop(startTime)
}

func (t *sampledTimer) Record(d time.Duration) {
	if !t.shouldSample() {
		return
	}
	t.Timer.Record(d)
}

// MethodMetrics is a bundle of common metrics with a uniform naming scheme
type MethodMetrics struct {
	Error   tally.Counter
	Success tally.Counter
	Latency tally.Timer
}

// ReportSuccessOrFailure increments Error/Success counter dependant on the error
func (m *MethodMetrics) ReportSuccessOrFailure(e error) {
	if e != nil {
		m.Error.Inc(1)
	} else {
		m.Success.Inc(1)
	}
}

// NewMethodMetrics returns a new Method metrics for the given method name
func NewMethodMetrics(scope tally.Scope, methodName string, samplingRate float64) MethodMetrics {
	return MethodMetrics{
		Error:   scope.Counter(methodName + ".error"),
		Success: scope.Counter(methodName + ".success"),
		Latency: newSampledTimer(scope.Timer(methodName+".latency"), samplingRate),
	}
}
