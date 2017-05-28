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
	Gauges() map[string]float64
	Timers() map[string][]time.Duration
	Events() []TestStatsReporterEvent
}

// TestStatsReporterEvent is an event that was reported to the test reporter
type TestStatsReporterEvent interface {
	Name() string
	Tags() map[string]string
	IsCounter() bool
	IsGauge() bool
	IsTimer() bool
	Value() int64
	TimerValue() time.Duration
}

// testStatsReporter should probably be moved to the tally project for better testing
type testStatsReporter struct {
	sync.RWMutex
	counters       map[string]int64
	gauges         map[string]float64
	timers         map[string][]time.Duration
	events         []*event
	timersDisabled bool
	captureEvents  bool
}

// NewTestStatsReporter returns a new TestStatsReporter
func NewTestStatsReporter(opts TestStatsReporterOptions) TestStatsReporter {
	return &testStatsReporter{
		counters:       make(map[string]int64),
		gauges:         make(map[string]float64),
		timers:         make(map[string][]time.Duration),
		timersDisabled: opts.TimersDisabled(),
		captureEvents:  opts.CaptureEvents(),
	}
}

func (r *testStatsReporter) Flush() {}

func (r *testStatsReporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.Lock()
	r.counters[name] += value
	if r.captureEvents {
		r.events = append(r.events, &event{
			eventType: eventTypeCounter,
			name:      name,
			tags:      tags,
			value:     value,
		})
	}
	r.Unlock()
}

func (r *testStatsReporter) ReportGauge(name string, tags map[string]string, value float64) {
	r.Lock()
	r.gauges[name] = value
	if r.captureEvents {
		r.events = append(r.events, &event{
			eventType: eventTypeGauge,
			name:      name,
			tags:      tags,
			value:     int64(value),
		})
	}
	r.Unlock()
}

func (r *testStatsReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	r.Lock()
	if r.captureEvents {
		r.events = append(r.events, &event{
			eventType:  eventTypeTimer,
			name:       name,
			tags:       tags,
			timerValue: interval,
		})
	}
	if r.timersDisabled {
		r.Unlock()
		return
	}
	if _, ok := r.timers[name]; !ok {
		r.timers[name] = make([]time.Duration, 0, 1)
	}
	r.timers[name] = append(r.timers[name], interval)
	r.Unlock()
}

func (r *testStatsReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64,
) {
	// TODO: implement
}

func (r *testStatsReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64,
) {
	// TODO: implement
}

func (r *testStatsReporter) Capabilities() tally.Capabilities {
	return r
}

func (r *testStatsReporter) Reporting() bool {
	return true
}

func (r *testStatsReporter) Tagging() bool {
	return true
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

func (r *testStatsReporter) Gauges() map[string]float64 {
	r.RLock()
	result := make(map[string]float64, len(r.gauges))
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

func (r *testStatsReporter) Events() []TestStatsReporterEvent {
	r.RLock()
	events := make([]TestStatsReporterEvent, len(r.events))
	for i := range r.events {
		events[i] = r.events[i]
	}
	r.RUnlock()
	return events
}

type event struct {
	eventType  eventType
	name       string
	tags       map[string]string
	value      int64
	timerValue time.Duration
}

type eventType int

const (
	eventTypeCounter eventType = iota
	eventTypeGauge
	eventTypeTimer
)

func (e *event) Name() string {
	return e.name
}

func (e *event) Tags() map[string]string {
	return e.tags
}

func (e *event) IsCounter() bool {
	return e.eventType == eventTypeCounter
}

func (e *event) IsGauge() bool {
	return e.eventType == eventTypeGauge
}

func (e *event) IsTimer() bool {
	return e.eventType == eventTypeTimer
}

func (e *event) Value() int64 {
	return e.value
}

func (e *event) TimerValue() time.Duration {
	return e.timerValue
}

// TestStatsReporterOptions is a set of options for a test stats reporter
type TestStatsReporterOptions interface {
	// SetTimersDisable sets whether to disable timers
	SetTimersDisable(value bool) TestStatsReporterOptions

	// SetTimersDisable returns whether to disable timers
	TimersDisabled() bool

	// SetCaptureEvents sets whether to capture each event
	SetCaptureEvents(value bool) TestStatsReporterOptions

	// SetCaptureEvents returns whether to capture each event
	CaptureEvents() bool
}

type testStatsReporterOptions struct {
	timersDisabled bool
	captureEvents  bool
}

// NewTestStatsReporterOptions creates a new set of options for a test stats reporter
func NewTestStatsReporterOptions() TestStatsReporterOptions {
	return &testStatsReporterOptions{
		timersDisabled: true,
		captureEvents:  false,
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

func (o *testStatsReporterOptions) SetCaptureEvents(value bool) TestStatsReporterOptions {
	opts := *o
	opts.captureEvents = value
	return &opts
}

func (o *testStatsReporterOptions) CaptureEvents() bool {
	return o.captureEvents
}
