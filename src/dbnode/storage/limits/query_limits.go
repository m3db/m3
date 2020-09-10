// Copyright (c) 2020 Uber Technologies, Inc.
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

package limits

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

// DefaultLookback is the default lookback used for query limits.
const DefaultLookback = time.Second * 15

type queryLimits struct {
	scope          tally.Scope
	docsLimit      LookbackLimit
	bytesReadLimit LookbackLimit
}

type lookbackLimit struct {
	name    string
	options LookbackLimitOptions
	metrics lookbackLimitMetrics
	recent  *atomic.Int64
	stopCh  chan struct{}
}

type lookbackLimitMetrics struct {
	recent     tally.Gauge
	recentPeak tally.Gauge
	total      tally.Counter
	exceeded   tally.Counter
}

var (
	_ QueryLimits   = (*queryLimits)(nil)
	_ LookbackLimit = (*lookbackLimit)(nil)
)

// QueryLimits provides an interface for managing query limits.
type QueryLimits interface {
	DocsLimit() LookbackLimit
	BytesReadLimit() LookbackLimit

	AnyExceeded() error
	Start()
	Stop()
}

// LookbackLimit provides an interface for a specific query limit.
type LookbackLimit interface {
	Inc(new int) error
	Exceeded() error
	Start()
	Stop()

	// For testing purposes.
	current() int64
	reset()
}

// LookbackLimitOptions holds options for a lookback limit to be enforced.
type LookbackLimitOptions struct {
	// Limit past which errors will be returned.
	Limit int64
	// Lookback is the period over which the limit is enforced.
	Lookback time.Duration
}

// NewQueryLimits returns a new query limits manager.
func NewQueryLimits(
	instrumentOpts instrument.Options,
	docsLimitOpts LookbackLimitOptions,
	bytesReadLimitOpts LookbackLimitOptions,
) QueryLimits {
	scope := instrumentOpts.
		MetricsScope().
		SubScope("query-limits")
	docsLimit := NewLookbackLimit("docs-matched", scope, docsLimitOpts)
	bytesReadLimit := NewLookbackLimit("bytes-read", scope, bytesReadLimitOpts)
	return &queryLimits{
		scope:          scope,
		docsLimit:      docsLimit,
		bytesReadLimit: bytesReadLimit,
	}
}

// NewLookbackLimit returns a new lookback limit.
func NewLookbackLimit(name string, scope tally.Scope, opts LookbackLimitOptions) LookbackLimit {
	return &lookbackLimit{
		name:    name,
		options: opts,
		metrics: lookbackLimitMetrics{
			recent:     scope.Gauge(fmt.Sprintf("recent-%s", name)),
			recentPeak: scope.Gauge(fmt.Sprintf("recent-peak-%s", name)),
			total:      scope.Counter(fmt.Sprintf("total-%s", name)),
			exceeded:   scope.Tagged(map[string]string{"limit": name}).Counter("limit-exceeded"),
		},
		recent: atomic.NewInt64(0),
		stopCh: make(chan struct{}),
	}
}

func (q *queryLimits) DocsLimit() LookbackLimit {
	return q.docsLimit
}

func (q *queryLimits) BytesReadLimit() LookbackLimit {
	return q.bytesReadLimit
}

func (q *queryLimits) Start() {
	q.docsLimit.Start()
	q.bytesReadLimit.Start()
}

func (q *queryLimits) Stop() {
	q.docsLimit.Stop()
	q.bytesReadLimit.Stop()
}

func (q *queryLimits) AnyExceeded() error {
	if err := q.docsLimit.Exceeded(); err != nil {
		return err
	}
	if err := q.bytesReadLimit.Exceeded(); err != nil {
		return err
	}
	return nil
}

// Inc increments the current value and returns an error if above the limit.
func (q *lookbackLimit) Inc(new int) error {
	if q == nil {
		return nil
	}
	if new < 0 {
		return fmt.Errorf("invalid negative query limit inc %d", new)
	}
	if new == 0 {
		return q.Exceeded()
	}

	// Add the new stats to the global state.
	newI64 := int64(new)
	recent := q.recent.Add(newI64)

	// Update metrics.
	q.metrics.recent.Update(float64(recent))
	q.metrics.total.Inc(newI64)

	// Enforce limit (if specified).
	return q.exceeded(recent)
}

func (q *lookbackLimit) Exceeded() error {
	return q.exceeded(q.recent.Load())
}

func (q *lookbackLimit) exceeded(recent int64) error {
	if q.options.Limit > 0 && recent > q.options.Limit {
		q.metrics.exceeded.Inc(1)
		return fmt.Errorf(
			"query aborted due to limit: name=%s, limit=%d, current=%d, within=%s",
			q.name, q.options.Limit, recent, q.options.Lookback)
	}
	return nil
}

// Start initializes background processing for enforcing the query limit.
func (q *lookbackLimit) Start() {
	if q == nil {
		return
	}
	ticker := time.NewTicker(q.options.Lookback)
	go func() {
		for {
			select {
			case <-ticker.C:
				q.reset()
			case <-q.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

func (q *lookbackLimit) Stop() {
	if q == nil {
		return
	}
	close(q.stopCh)
}

// Validate returns an error if the query stats options are invalid.
func (opts LookbackLimitOptions) Validate() error {
	if opts.Limit < 0 {
		return fmt.Errorf("query limit requires limit >= 0 (%d)", opts.Limit)
	}
	if opts.Lookback <= 0 {
		return fmt.Errorf("query limit requires lookback > 0 (%d)", opts.Lookback)
	}
	return nil
}

func (q *lookbackLimit) current() int64 {
	return q.recent.Load()
}

func (q *lookbackLimit) reset() {
	// Update peak gauge only on resets so it only tracks
	// the peak values for each lookback period.
	recent := q.recent.Load()
	q.metrics.recentPeak.Update(float64(recent))

	// Update the standard recent gauge to reflect drop back to zero.
	q.metrics.recent.Update(0)

	q.recent.Store(0)

}
