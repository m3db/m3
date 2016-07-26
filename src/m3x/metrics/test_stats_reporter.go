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
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/caio/go-tdigest"
)

// TestStatsReporter captures metrics updates in-memory for testing purposes
type TestStatsReporter interface {
	// Counter returns the value of the given counter
	Counter(name string, tags map[string]string) int64

	// Gauge returns the value of the given gauge
	Gauge(name string, tags map[string]string) int64

	// Timer returns the digest of the given timer
	Timer(name string, tags map[string]string) *tdigest.TDigest

	// IncCounter increments the given counter
	IncCounter(name string, tags map[string]string, n int64)

	// UpdateGauge updates the given gauge value
	UpdateGauge(name string, tags map[string]string, value int64)

	// RecordTimer records the given timer value
	RecordTimer(name string, tags map[string]string, d time.Duration)
}

type testStatsReporter struct {
	sync.RWMutex

	counters map[string]int64
	gauges   map[string]int64
	timers   map[string]*tdigest.TDigest
}

// NewTestStatsReporter returns a new TestStatsReporter
func NewTestStatsReporter() TestStatsReporter {
	return &testStatsReporter{
		counters: make(map[string]int64),
		gauges:   make(map[string]int64),
		timers:   make(map[string]*tdigest.TDigest),
	}
}

func (sr *testStatsReporter) Counter(name string, tags map[string]string) int64 {
	id := toID(name, tags)
	sr.RLock()
	defer sr.RUnlock()
	return sr.counters[id]
}

func (sr *testStatsReporter) Gauge(name string, tags map[string]string) int64 {
	id := toID(name, tags)
	sr.RLock()
	defer sr.RUnlock()
	return sr.gauges[id]
}

func (sr *testStatsReporter) Timer(name string, tags map[string]string) *tdigest.TDigest {
	id := toID(name, tags)
	sr.RLock()
	defer sr.RUnlock()
	return sr.timers[id]
}

func (sr *testStatsReporter) IncCounter(name string, tags map[string]string, n int64) {
	id := toID(name, tags)
	sr.Lock()
	sr.counters[id] += n
	sr.Unlock()
}

func (sr *testStatsReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	id := toID(name, tags)
	sr.Lock()
	sr.gauges[id] = value
	sr.Unlock()
}

func (sr *testStatsReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	id := toID(name, tags)
	sr.Lock()
	t := sr.timers[id]
	if t == nil {
		t = tdigest.New(100)
		sr.timers[id] = t
	}

	t.Add(float64(d/time.Millisecond), 1.0)
	sr.Unlock()
}

// toID converts the given name + tags to an internal ID
func toID(name string, tags map[string]string) string {
	if len(tags) == 0 {
		return name
	}

	// NB(mmihic): This is insanely expensive, but should only be used for testing
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b bytes.Buffer
	fmt.Fprintf(&b, "%s?", name)
	for _, k := range keys {
		v := tags[k]
		fmt.Fprintf(&b, "%s=%s&", k, v)
	}

	return b.String()
}
