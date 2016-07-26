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
	"fmt"
	"time"

	"github.com/facebookgo/clock"
)

// A Scope is a namespace wrapper around a stats reporter, ensuring that
// all emitted values have a given prefix or set of tags
type Scope interface {
	// IncCounter increments the given counter value
	IncCounter(name string, v int64)

	// UpdateGauge updates the given gauge
	UpdateGauge(name string, v int64)

	// RecordTimer records the given timer value
	RecordTimer(name string, d time.Duration)

	// StartTiming starts timing execution, returning a function to stop and record the timing
	StartTiming(name string, clock clock.Clock) func()

	// StartCall starts timing an execution call, returning a function to stop and record the
	// timing and whether the call succeeded or failed
	StartCall(name string, clock clock.Clock) func(bool)

	// SubScope returns a new child scope with the given name
	SubScope(name string) Scope

	// Tagged returns a new scope with the given tags
	Tagged(tags map[string]string) Scope
}

// NoopScope is a scope that does nothing
var NoopScope = NewScope("", NullStatsReporter)

// NewScope creates a new Scope around a given stats reporter with the given prefix
func NewScope(prefix string, stats StatsReporter) Scope {
	return &scope{
		prefix: prefix,
		stats:  stats,
		tags:   nil,
	}
}

type scope struct {
	prefix string
	stats  StatsReporter
	tags   map[string]string
}

func (s *scope) UpdateGauge(name string, v int64) {
	s.stats.UpdateGauge(s.scopedName(name), s.tags, v)
}

func (s *scope) RecordTimer(name string, d time.Duration) {
	s.stats.RecordTimer(s.scopedName(name), s.tags, d)
}

func (s *scope) IncCounter(name string, n int64) {
	s.stats.IncCounter(s.scopedName(name), s.tags, n)
}

func (s *scope) SubScope(prefix string) Scope {
	return &scope{
		prefix: s.scopedName(prefix),
		tags:   s.tags,
		stats:  s.stats,
	}
}

func (s *scope) Tagged(tags map[string]string) Scope {
	mergedTags := make(map[string]string, len(s.tags)+len(tags))
	for k, v := range s.tags {
		mergedTags[k] = v
	}

	for k, v := range tags {
		mergedTags[k] = v
	}

	return &scope{
		prefix: s.prefix,
		tags:   mergedTags,
		stats:  s.stats,
	}
}

func (s *scope) StartTiming(name string, clock clock.Clock) func() {
	if clock == nil {
		clock = globalClock
	}

	startTime := clock.Now()
	return func() {
		endTime := clock.Now()
		s.RecordTimer(name, endTime.Sub(startTime))
	}
}

func (s *scope) StartCall(name string, clock clock.Clock) func(bool) {
	timing := s.StartTiming(name, clock)
	return func(success bool) {
		if success {
			s.IncCounter(fmt.Sprintf("%s.success", name), 1)
			timing()
		} else {
			// Don't time errors since this will skew results
			s.IncCounter(fmt.Sprintf("%s.errors", name), 1)
		}
	}
}

func (s *scope) scopedName(name string) string {
	// TODO(mmihic): Consider maintaining a map[string]string for common names so we
	// avoid the cost of continual allocations
	if len(s.prefix) == 0 {
		return name
	}

	return fmt.Sprintf("%s.%s", s.prefix, name)
}

var globalClock = clock.New()
