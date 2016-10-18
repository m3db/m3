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

package xmetrics

import (
	"sync"
	"time"

	"github.com/uber-go/tally"
)

// TestStatsReporter is a test reporter that collects metrics and makes
// them accessible for testing purposes
type TestStatsReporter interface {
	tally.StatsReporter
	Counters() map[string]int64
	Gauges() map[string]int64
	Timers() map[string][]time.Duration
}

// testStatsReporter should probably be moved to the tally project for better testing
type testStatsReporter struct {
	sync.RWMutex
	counters       map[string]int64
	gauges         map[string]int64
	timers         map[string][]time.Duration
	timersDisabled bool
}

// NewTestStatsReporter returns a new TestStatsReporter
func NewTestStatsReporter(opts TestStatsReporterOptions) TestStatsReporter {
	return &testStatsReporter{
		counters:       make(map[string]int64),
		gauges:         make(map[string]int64),
		timers:         make(map[string][]time.Duration),
		timersDisabled: opts.TimersDisabled(),
	}
}

func (r *testStatsReporter) Flush() {}

func (r *testStatsReporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.Lock()
	r.counters[name] += value
	r.Unlock()
}

func (r *testStatsReporter) ReportGauge(name string, tags map[string]string, value int64) {
	r.Lock()
	r.gauges[name] = value
	r.Unlock()
}

func (r *testStatsReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	if r.timersDisabled {
		return
	}
	r.Lock()
	if _, ok := r.timers[name]; !ok {
		r.timers[name] = make([]time.Duration, 0, 1)
	}
	r.timers[name] = append(r.timers[name], interval)
	r.Unlock()
}

func (r *testStatsReporter) Counters() map[string]int64 {
	r.RLock()
	result := make(map[string]int64, len(r.counters))
	for k, v := range r.counters {
		result[k] = v
	}
	r.RUnlock()
	return result
}

func (r *testStatsReporter) Gauges() map[string]int64 {
	r.RLock()
	result := make(map[string]int64, len(r.gauges))
	for k, v := range r.gauges {
		result[k] = v
	}
	r.RUnlock()
	return result
}

func (r *testStatsReporter) Timers() map[string][]time.Duration {
	r.RLock()
	result := make(map[string][]time.Duration, len(r.timers))
	for k, v := range r.timers {
		result[k] = v
	}
	r.RUnlock()
	return result
}

// TestStatsReporterOptions is a set of options for a test stats reporter
type TestStatsReporterOptions interface {
	// SetTimersDisable sets whether to disable timers
	SetTimersDisable(value bool) TestStatsReporterOptions

	// SetTimersDisable returns whether to disable timers
	TimersDisabled() bool
}

type testStatsReporterOptions struct {
	timersDisabled bool
}

// NewTestStatsReporterOptions creates a new set of options for a test stats reporter
func NewTestStatsReporterOptions() TestStatsReporterOptions {
	return &testStatsReporterOptions{
		timersDisabled: true,
	}
}

func (o *testStatsReporterOptions) SetTimersDisable(value bool) TestStatsReporterOptions {
	opts := *o
	opts.timersDisabled = value
	return &opts
}

func (o *testStatsReporterOptions) TimersDisabled() bool {
	return o.timersDisabled
}
