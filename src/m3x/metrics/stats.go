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
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

// StatsReporter is the the interface used to report stats
type StatsReporter interface {
	IncCounter(name string, tags map[string]string, value int64)
	UpdateGauge(name string, tags map[string]string, value int64)
	RecordTimer(name string, tags map[string]string, d time.Duration)
}

// NullStatsReporter is a stats reporter that discards the statistics
var NullStatsReporter StatsReporter = nullStatsReporter{}

type nullStatsReporter struct{}

func (nullStatsReporter) IncCounter(name string, tags map[string]string, value int64)      {}
func (nullStatsReporter) UpdateGauge(name string, tags map[string]string, value int64)     {}
func (nullStatsReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {}

type cactusStatsReporter struct {
	statter statsd.Statter
}

// NewCactusStatsReporter creates a new cactus statsd reporter
func NewCactusStatsReporter(statter statsd.Statter) StatsReporter {
	return &cactusStatsReporter{statter: statter}
}

func (s *cactusStatsReporter) IncCounter(name string, tags map[string]string, value int64) {
	s.statter.Inc(name, value, 1.0)
}

func (s *cactusStatsReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	s.statter.Gauge(name, value, 1.0)
}

func (s *cactusStatsReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	s.statter.TimingDuration(name, d, 1.0)
}
