// Copyright (c) 2021 Uber Technologies, Inc.
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
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	disabledLimitValue = 0
	defaultLookback    = time.Second * 15
)

type queryLimits struct {
	docsLimit           *lookbackLimit
	bytesReadLimit      *lookbackLimit
	seriesDiskReadLimit *lookbackLimit
}

type lookbackLimit struct {
	name      string
	options   LookbackLimitOptions
	metrics   lookbackLimitMetrics
	logger    *zap.Logger
	recent    *atomic.Int64
	stopCh    chan struct{}
	stoppedCh chan struct{}
	lock      sync.RWMutex
}

type lookbackLimitMetrics struct {
	optionsLimit    tally.Gauge
	optionsLookback tally.Gauge
	recentCount     tally.Gauge
	recentMax       tally.Gauge
	total           tally.Counter
	exceeded        tally.Counter

	sourceLogger SourceLogger
}

var (
	_ QueryLimits   = (*queryLimits)(nil)
	_ LookbackLimit = (*lookbackLimit)(nil)
)

// DefaultLookbackLimitOptions returns a new query limits manager.
func DefaultLookbackLimitOptions() LookbackLimitOptions {
	return LookbackLimitOptions{
		// Default to no limit.
		Limit:    disabledLimitValue,
		Lookback: defaultLookback,
	}
}

// NewQueryLimits returns a new query limits manager.
func NewQueryLimits(options Options) (QueryLimits, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	var (
		iOpts                   = options.InstrumentOptions()
		docsLimitOpts           = options.DocsLimitOpts()
		bytesReadLimitOpts      = options.BytesReadLimitOpts()
		diskSeriesReadLimitOpts = options.DiskSeriesReadLimitOpts()
		sourceLoggerBuilder     = options.SourceLoggerBuilder()

		docsLimit = newLookbackLimit(
			iOpts, docsLimitOpts, "docs-matched", sourceLoggerBuilder)
		bytesReadLimit = newLookbackLimit(
			iOpts, bytesReadLimitOpts, "disk-bytes-read", sourceLoggerBuilder)
		seriesDiskReadLimit = newLookbackLimit(
			iOpts, diskSeriesReadLimitOpts, "disk-series-read", sourceLoggerBuilder)
	)

	return &queryLimits{
		docsLimit:           docsLimit,
		bytesReadLimit:      bytesReadLimit,
		seriesDiskReadLimit: seriesDiskReadLimit,
	}, nil
}

func newLookbackLimit(
	instrumentOpts instrument.Options,
	opts LookbackLimitOptions,
	name string,
	sourceLoggerBuilder SourceLoggerBuilder,
) *lookbackLimit {
	return &lookbackLimit{
		name:      name,
		options:   opts,
		metrics:   newLookbackLimitMetrics(instrumentOpts, name, sourceLoggerBuilder),
		logger:    instrumentOpts.Logger(),
		recent:    atomic.NewInt64(0),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

func newLookbackLimitMetrics(
	instrumentOpts instrument.Options,
	name string,
	sourceLoggerBuilder SourceLoggerBuilder,
) lookbackLimitMetrics {
	scope := instrumentOpts.
		MetricsScope().
		SubScope("query-limit")

	sourceLogger := sourceLoggerBuilder.NewSourceLogger(name,
		instrumentOpts.SetMetricsScope(scope))

	return lookbackLimitMetrics{
		optionsLimit:    scope.Gauge(fmt.Sprintf("current-limit%s", name)),
		optionsLookback: scope.Gauge(fmt.Sprintf("current-lookback-%s", name)),
		recentCount:     scope.Gauge(fmt.Sprintf("recent-count-%s", name)),
		recentMax:       scope.Gauge(fmt.Sprintf("recent-max-%s", name)),
		total:           scope.Counter(fmt.Sprintf("total-%s", name)),
		exceeded:        scope.Tagged(map[string]string{"limit": name}).Counter("exceeded"),

		sourceLogger: sourceLogger,
	}
}

func (q *queryLimits) DocsLimit() LookbackLimit {
	return q.docsLimit
}

func (q *queryLimits) BytesReadLimit() LookbackLimit {
	return q.bytesReadLimit
}

func (q *queryLimits) DiskSeriesReadLimit() LookbackLimit {
	return q.seriesDiskReadLimit
}

func (q *queryLimits) Start() {
	// Lock on explicit start to avoid any collision with asynchronous updating
	// which will call stop/start if the lookback has changed.
	q.docsLimit.startWithLock()
	q.seriesDiskReadLimit.startWithLock()
	q.bytesReadLimit.startWithLock()
}

func (q *queryLimits) Stop() {
	// Lock on explicit stop to avoid any collision with asynchronous updating
	// which will call stop/start if the lookback has changed.
	q.docsLimit.stopWithLock()
	q.seriesDiskReadLimit.stopWithLock()
	q.bytesReadLimit.stopWithLock()
}

func (q *queryLimits) AnyExceeded() error {
	if err := q.docsLimit.exceeded(); err != nil {
		return err
	}
	if err := q.seriesDiskReadLimit.exceeded(); err != nil {
		return err
	}
	return q.bytesReadLimit.exceeded()
}

func (q *lookbackLimit) Options() LookbackLimitOptions {
	q.lock.RLock()
	o := q.options
	q.lock.RUnlock()
	return o
}

// Update updates the limit.
func (q *lookbackLimit) Update(opts LookbackLimitOptions) error {
	if err := opts.validate(); err != nil {
		return err
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	old := q.options
	q.options = opts

	// If the lookback changed, replace the background goroutine that manages the periodic resetting.
	if q.options.Lookback != old.Lookback {
		q.stop()
		q.start()
	}

	q.logger.Info("query limit options updated",
		zap.String("name", q.name),
		zap.Any("new", opts),
		zap.Any("old", old))

	return nil
}

// Inc increments the current value and returns an error if above the limit.
func (q *lookbackLimit) Inc(val int, source []byte) error {
	if val < 0 {
		return fmt.Errorf("invalid negative query limit inc %d", val)
	}
	if val == 0 {
		return q.exceeded()
	}

	// Add the new stats to the global state.
	valI64 := int64(val)
	recent := q.recent.Add(valI64)

	// Update metrics.
	q.metrics.recentCount.Update(float64(recent))
	q.metrics.total.Inc(valI64)

	q.metrics.sourceLogger.LogSourceValue(valI64, source)

	// Enforce limit (if specified).
	return q.checkLimit(recent)
}

func (q *lookbackLimit) exceeded() error {
	return q.checkLimit(q.recent.Load())
}

func (q *lookbackLimit) checkLimit(recent int64) error {
	q.lock.RLock()
	currentOpts := q.options
	q.lock.RUnlock()

	if currentOpts.ForceExceeded {
		q.metrics.exceeded.Inc(1)
		return xerrors.NewInvalidParamsError(NewQueryLimitExceededError(fmt.Sprintf(
			"query aborted due to forced limit: name=%s", q.name)))
	}

	if currentOpts.Limit == disabledLimitValue {
		return nil
	}

	if recent >= currentOpts.Limit {
		q.metrics.exceeded.Inc(1)
		return xerrors.NewInvalidParamsError(NewQueryLimitExceededError(fmt.Sprintf(
			"query aborted due to limit: name=%s, limit=%d, current=%d, within=%s",
			q.name, q.options.Limit, recent, q.options.Lookback)))
	}
	return nil
}

func (q *lookbackLimit) startWithLock() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.start()
}

func (q *lookbackLimit) stopWithLock() {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.stop()
}

func (q *lookbackLimit) start() {
	ticker := time.NewTicker(q.options.Lookback)
	go func() {
		q.logger.Info("query limit interval started", zap.String("name", q.name))
		for {
			select {
			case <-ticker.C:
				q.reset()
			case <-q.stopCh:
				ticker.Stop()
				q.stoppedCh <- struct{}{}
				return
			}
		}
	}()

	q.metrics.optionsLimit.Update(float64(q.options.Limit))
	q.metrics.optionsLookback.Update(q.options.Lookback.Seconds())
}

func (q *lookbackLimit) stop() {
	close(q.stopCh)
	<-q.stoppedCh
	q.stopCh = make(chan struct{})
	q.stoppedCh = make(chan struct{})

	q.logger.Info("query limit interval stopped", zap.String("name", q.name))
}

func (q *lookbackLimit) current() int64 {
	return q.recent.Load()
}

func (q *lookbackLimit) reset() {
	// Update peak gauge only on resets so it only tracks
	// the peak values for each lookback period.
	recent := q.recent.Load()
	q.metrics.recentMax.Update(float64(recent))

	// Update the standard recent gauge to reflect drop back to zero.
	q.metrics.recentCount.Update(0)

	q.recent.Store(0)
}

// Equals returns true if the other options match the current.
func (opts LookbackLimitOptions) Equals(other LookbackLimitOptions) bool {
	return opts.Limit == other.Limit &&
		opts.Lookback == other.Lookback &&
		opts.ForceExceeded == other.ForceExceeded
}

func (opts LookbackLimitOptions) validate() error {
	if opts.Limit < 0 {
		return fmt.Errorf("query limit requires limit >= 0 (%d)", opts.Limit)
	}
	if opts.Lookback <= 0 {
		return fmt.Errorf("query limit requires lookback > 0 (%d)", opts.Lookback)
	}
	return nil
}
