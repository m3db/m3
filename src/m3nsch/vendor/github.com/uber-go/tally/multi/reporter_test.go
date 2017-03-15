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

package multi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestMultiReporter(t *testing.T) {
	a, b, c :=
		newCapturingStatsReporter(),
		newCapturingStatsReporter(),
		newCapturingStatsReporter()
	all := []*capturingStatsReporter{a, b, c}

	r := NewMultiReporter(a, b, c)

	tags := []map[string]string{
		{"foo": "bar"},
		{"foo": "baz"},
		{"foo": "qux"},
	}

	r.ReportCounter("foo", tags[0], 42)
	r.ReportCounter("foo", tags[0], 84)
	r.ReportGauge("baz", tags[1], 42.0)
	r.ReportTimer("qux", tags[2], 126*time.Millisecond)
	for _, r := range all {
		require.Equal(t, 2, len(r.counts))

		assert.Equal(t, "foo", r.counts[0].name)
		assert.Equal(t, tags[0], r.counts[0].tags)
		assert.Equal(t, int64(42), r.counts[0].value)

		assert.Equal(t, "foo", r.counts[1].name)
		assert.Equal(t, tags[0], r.counts[1].tags)
		assert.Equal(t, int64(84), r.counts[1].value)

		assert.Equal(t, "baz", r.gauges[0].name)
		assert.Equal(t, tags[1], r.gauges[0].tags)
		assert.Equal(t, float64(42.0), r.gauges[0].value)

		assert.Equal(t, "qux", r.timers[0].name)
		assert.Equal(t, tags[2], r.timers[0].tags)
		assert.Equal(t, 126*time.Millisecond, r.timers[0].value)
	}

	assert.NotNil(t, r.Capabilities())

	r.Flush()
	for _, r := range all {
		assert.Equal(t, 1, r.flush)
	}
}

func TestMultiCachedReporter(t *testing.T) {
	a, b, c :=
		newCapturingStatsReporter(),
		newCapturingStatsReporter(),
		newCapturingStatsReporter()
	all := []*capturingStatsReporter{a, b, c}

	r := NewMultiCachedReporter(a, b, c)

	tags := []map[string]string{
		{"foo": "bar"},
		{"foo": "baz"},
		{"foo": "qux"},
	}

	ctr := r.AllocateCounter("foo", tags[0])
	ctr.ReportCount(42)
	ctr.ReportCount(84)

	gauge := r.AllocateGauge("baz", tags[1])
	gauge.ReportGauge(42.0)

	tmr := r.AllocateTimer("qux", tags[2])
	tmr.ReportTimer(126 * time.Millisecond)

	for _, r := range all {
		require.Equal(t, 2, len(r.counts))

		assert.Equal(t, "foo", r.counts[0].name)
		assert.Equal(t, tags[0], r.counts[0].tags)
		assert.Equal(t, int64(42), r.counts[0].value)

		assert.Equal(t, "foo", r.counts[1].name)
		assert.Equal(t, tags[0], r.counts[1].tags)
		assert.Equal(t, int64(84), r.counts[1].value)

		assert.Equal(t, "baz", r.gauges[0].name)
		assert.Equal(t, tags[1], r.gauges[0].tags)
		assert.Equal(t, float64(42.0), r.gauges[0].value)

		assert.Equal(t, "qux", r.timers[0].name)
		assert.Equal(t, tags[2], r.timers[0].tags)
		assert.Equal(t, 126*time.Millisecond, r.timers[0].value)
	}

	assert.NotNil(t, r.Capabilities())

	r.Flush()
	for _, r := range all {
		assert.Equal(t, 1, r.flush)
	}
}

type capturingStatsReporter struct {
	counts       []capturedCount
	gauges       []capturedGauge
	timers       []capturedTimer
	capabilities int
	flush        int
}

type capturedCount struct {
	name  string
	tags  map[string]string
	value int64
}

type capturedGauge struct {
	name  string
	tags  map[string]string
	value float64
}

type capturedTimer struct {
	name  string
	tags  map[string]string
	value time.Duration
}

func newCapturingStatsReporter() *capturingStatsReporter {
	return &capturingStatsReporter{}
}

func (r *capturingStatsReporter) ReportCounter(
	name string,
	tags map[string]string,
	value int64,
) {
	r.counts = append(r.counts, capturedCount{name, tags, value})
}

func (r *capturingStatsReporter) ReportGauge(
	name string,
	tags map[string]string,
	value float64,
) {
	r.gauges = append(r.gauges, capturedGauge{name, tags, value})
}

func (r *capturingStatsReporter) ReportTimer(
	name string,
	tags map[string]string,
	value time.Duration,
) {
	r.timers = append(r.timers, capturedTimer{name, tags, value})
}

func (r *capturingStatsReporter) AllocateCounter(
	name string,
	tags map[string]string,
) tally.CachedCount {
	return cachedCount{fn: func(value int64) {
		r.counts = append(r.counts, capturedCount{name, tags, value})
	}}
}

func (r *capturingStatsReporter) AllocateGauge(
	name string,
	tags map[string]string,
) tally.CachedGauge {
	return cachedGauge{fn: func(value float64) {
		r.gauges = append(r.gauges, capturedGauge{name, tags, value})
	}}
}

func (r *capturingStatsReporter) AllocateTimer(
	name string,
	tags map[string]string,
) tally.CachedTimer {
	return cachedTimer{fn: func(value time.Duration) {
		r.timers = append(r.timers, capturedTimer{name, tags, value})
	}}
}

func (r *capturingStatsReporter) Capabilities() tally.Capabilities {
	r.capabilities++
	return r
}

func (r *capturingStatsReporter) Reporting() bool {
	return true
}

func (r *capturingStatsReporter) Tagging() bool {
	return true
}

func (r *capturingStatsReporter) Flush() {
	r.flush++
}

type cachedCount struct {
	fn func(value int64)
}

func (c cachedCount) ReportCount(value int64) {
	c.fn(value)
}

type cachedGauge struct {
	fn func(value float64)
}

func (c cachedGauge) ReportGauge(value float64) {
	c.fn(value)
}

type cachedTimer struct {
	fn func(value time.Duration)
}

func (c cachedTimer) ReportTimer(value time.Duration) {
	c.fn(value)
}
