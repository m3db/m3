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

package tally

import "time"

// BaseStatsReporter implements the shared reporter methods
type BaseStatsReporter interface {
	Capabilities() Capabilities
	Flush()
}

// StatsReporter is a backend for Scopes to report metrics to
type StatsReporter interface {
	BaseStatsReporter

	// ReportCounter reports a counter value
	ReportCounter(name string, tags map[string]string, value int64)

	// ReportGauge reports a gauge value
	ReportGauge(name string, tags map[string]string, value float64)

	// ReportTimer reports a timer value
	ReportTimer(name string, tags map[string]string, interval time.Duration)
}

// CachedStatsReporter is a backend for Scopes that pre allocates all
// counter, gauges & timers. This is harder to implement but more performant
type CachedStatsReporter interface {
	BaseStatsReporter

	// AllocateCounter pre allocates a counter data structure with name & tags.
	AllocateCounter(name string, tags map[string]string) CachedCount

	// AllocateGauge pre allocates a gauge data structure with name & tags.
	AllocateGauge(name string, tags map[string]string) CachedGauge

	// AllocateTimer pre allocates a timer data structure with name & tags.
	AllocateTimer(name string, tags map[string]string) CachedTimer
}

// CachedCount interface for reporting an individual counter
type CachedCount interface {
	ReportCount(value int64)
}

// CachedGauge interface for reporting an individual gauge
type CachedGauge interface {
	ReportGauge(value float64)
}

// CachedTimer interface for reporting an individual timer
type CachedTimer interface {
	ReportTimer(interval time.Duration)
}
