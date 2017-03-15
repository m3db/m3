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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type statsTestReporter struct {
	last interface{}
}

func (r *statsTestReporter) ReportCounter(name string, tags map[string]string, value int64) {
	r.last = value
}

func (r *statsTestReporter) ReportGauge(name string, tags map[string]string, value float64) {
	r.last = value
}

func (r *statsTestReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	r.last = interval
}

func (r *statsTestReporter) Capabilities() Capabilities {
	return capabilitiesReportingNoTagging
}

func (r *statsTestReporter) Flush() {}

func TestCounter(t *testing.T) {
	counter := newCounter(nil)
	r := &statsTestReporter{}

	counter.Inc(1)
	counter.report("", nil, r)
	assert.Equal(t, int64(1), r.last)

	counter.Inc(1)
	counter.report("", nil, r)
	assert.Equal(t, int64(1), r.last)

	counter.Inc(1)
	counter.report("", nil, r)
	assert.Equal(t, int64(1), r.last)
}

func TestGauge(t *testing.T) {
	gauge := newGauge(nil)
	r := &statsTestReporter{}

	gauge.Update(42)
	gauge.report("", nil, r)
	assert.Equal(t, float64(42), r.last)

	gauge.Update(1234)
	gauge.Update(5678)
	gauge.report("", nil, r)
	assert.Equal(t, float64(5678), r.last)
}

func TestTimer(t *testing.T) {
	r := &statsTestReporter{}
	timer := newTimer("t1", nil, r, nil)

	timer.Record(42 * time.Millisecond)
	assert.Equal(t, 42*time.Millisecond, r.last)

	timer.Record(128 * time.Millisecond)
	assert.Equal(t, 128*time.Millisecond, r.last)
}
