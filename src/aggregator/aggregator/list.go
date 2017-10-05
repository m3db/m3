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

package aggregator

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3metrics/metric/aggregated"
	metricid "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

var (
	errListClosed  = errors.New("metric list is closed")
	errListsClosed = errors.New("metric lists are closed")
)

type metricListMetrics struct {
	flushMetricConsumeSuccess   tally.Counter
	flushMetricConsumeErrors    tally.Counter
	flushMetricDiscarded        tally.Counter
	flushWriterFlushSuccess     tally.Counter
	flushWriterFlushErrors      tally.Counter
	flushElemCollected          tally.Counter
	flushDuration               tally.Timer
	flushBeforeCutover          tally.Counter
	flushBetweenCutoverCutoff   tally.Counter
	flushBetweenCutoffBufferEnd tally.Counter
	flushAfterBufferEnd         tally.Counter
	flushBeforeStale            tally.Counter
	flushBeforeDuration         tally.Timer
	discardBefore               tally.Counter
}

func newMetricListMetrics(scope tally.Scope) metricListMetrics {
	flushScope := scope.SubScope("flush")
	flushBeforeScope := scope.SubScope("flush-before")
	flushWriterScope := scope.SubScope("flush-writer")
	return metricListMetrics{
		flushMetricConsumeSuccess:   flushScope.Counter("metric-consume-success"),
		flushMetricConsumeErrors:    flushScope.Counter("metric-consume-errors"),
		flushMetricDiscarded:        flushScope.Counter("metric-discarded"),
		flushWriterFlushSuccess:     flushWriterScope.Counter("flush-success"),
		flushWriterFlushErrors:      flushWriterScope.Counter("flush-errors"),
		flushElemCollected:          flushScope.Counter("elem-collected"),
		flushDuration:               flushScope.Timer("duration"),
		flushBeforeCutover:          flushScope.Counter("before-cutover"),
		flushBetweenCutoverCutoff:   flushScope.Counter("between-cutover-cutoff"),
		flushBetweenCutoffBufferEnd: flushScope.Counter("between-cutoff-bufferend"),
		flushAfterBufferEnd:         flushScope.Counter("after-bufferend"),
		flushBeforeStale:            flushBeforeScope.Counter("stale"),
		flushBeforeDuration:         flushBeforeScope.Timer("duration"),
		discardBefore:               scope.Counter("discard-before"),
	}
}

type flushBeforeFn func(beforeNanos int64, flushType flushType)

// metricList stores aggregated metrics at a given resolution
// and flushes aggregations periodically.
type metricList struct {
	sync.RWMutex

	shard         uint32
	opts          Options
	nowFn         clock.NowFn
	log           log.Logger
	timeLock      *sync.RWMutex
	flushHandler  Handler
	flushWriter   Writer
	resolution    time.Duration
	flushInterval time.Duration
	flushMgr      FlushManager

	closed             bool
	aggregations       *list.List
	lastFlushedNanos   int64
	toCollect          []*list.Element
	flushBeforeFn      flushBeforeFn
	consumeAggMetricFn aggMetricFn
	discardAggMetricFn aggMetricFn
	metrics            metricListMetrics
}

func newMetricList(shard uint32, resolution time.Duration, opts Options) (*metricList, error) {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("list").Tagged(
		map[string]string{"resolution": resolution.String()},
	)
	flushHandler := opts.FlushHandler()
	flushWriter, err := flushHandler.NewWriter(scope.SubScope("writer"))
	if err != nil {
		return nil, err
	}

	// NB(xichen): by default the flush interval is the same as metric
	// resolution, unless the resolution is smaller than the minimum flush
	// interval, in which case we use the min flush interval to avoid excessing
	// CPU overhead due to flushing.
	flushInterval := resolution
	if minFlushInterval := opts.MinFlushInterval(); flushInterval < minFlushInterval {
		flushInterval = minFlushInterval
	}
	l := &metricList{
		shard:         shard,
		opts:          opts,
		nowFn:         opts.ClockOptions().NowFn(),
		log:           opts.InstrumentOptions().Logger(),
		timeLock:      opts.TimeLock(),
		flushHandler:  flushHandler,
		flushWriter:   flushWriter,
		resolution:    resolution,
		flushInterval: flushInterval,
		flushMgr:      opts.FlushManager(),
		aggregations:  list.New(),
		metrics:       newMetricListMetrics(scope),
	}
	l.flushBeforeFn = l.flushBefore
	l.consumeAggMetricFn = l.consumeAggregatedMetric
	l.discardAggMetricFn = l.discardAggregatedMetric
	l.flushMgr.Register(l)

	return l, nil
}

func (l *metricList) Shard() uint32 { return l.shard }

func (l *metricList) LastFlushedNanos() int64 { return atomic.LoadInt64(&l.lastFlushedNanos) }

// Resolution returns the resolution of the list.
func (l *metricList) Resolution() time.Duration { return l.resolution }

// FlushInterval returns the flush interval of the list.
func (l *metricList) FlushInterval() time.Duration { return l.flushInterval }

// Len returns the number of elements in the list.
func (l *metricList) Len() int {
	l.RLock()
	numElems := l.aggregations.Len()
	l.RUnlock()
	return numElems
}

// PushBack adds an element to the list.
// NB(xichen): the container list doesn't provide an API to directly
// insert a list element, therefore making it impossible to pool the
// elements and manage their lifetimes. If this becomes an issue,
// need to switch to a custom type-specific list implementation.
func (l *metricList) PushBack(value interface{}) (*list.Element, error) {
	l.Lock()
	if l.closed {
		l.Unlock()
		return nil, errListClosed
	}
	elem := l.aggregations.PushBack(value)
	l.Unlock()
	return elem, nil
}

// Close closes the list.
func (l *metricList) Close() {
	l.Lock()
	if l.closed {
		l.Unlock()
		return
	}
	l.flushWriter.Close()
	l.closed = true
	l.Unlock()

	// Unregister the list outside the list lock to avoid holding the list lock
	// while attempting to acquire the flush manager lock potentially causing a
	// deadlock.
	if err := l.flushMgr.Unregister(l); err != nil {
		l.log.Errorf("error unregistering list: %v", err)
	}
}

func (l *metricList) Flush(req FlushRequest) {
	start := l.nowFn()

	defer func() {
		took := l.nowFn().Sub(start)
		l.metrics.flushDuration.Record(took)
	}()

	// NB(xichen): it is important to determine ticking start time within the time lock
	// because this ensures all the actions before `now` have completed if those actions
	// are protected by the same read lock.
	l.timeLock.Lock()
	nowNanos := l.nowFn().UnixNano()
	l.timeLock.Unlock()

	// Metrics before shard cutover are discarded.
	if nowNanos <= req.CutoverNanos {
		l.flushBeforeFn(nowNanos, discardType)
		l.metrics.flushBeforeCutover.Inc(1)
		return
	}

	// Metrics between shard cutover and shard cutoff are consumed.
	if req.CutoverNanos > 0 {
		l.flushBeforeFn(req.CutoverNanos, discardType)
	}
	if nowNanos <= req.CutoffNanos {
		l.flushBeforeFn(nowNanos, consumeType)
		l.metrics.flushBetweenCutoverCutoff.Inc(1)
		return
	}

	// Metrics after now-keepAfterCutoff are retained.
	l.flushBeforeFn(req.CutoffNanos, consumeType)
	bufferEndNanos := nowNanos - int64(req.BufferAfterCutoff)
	if bufferEndNanos <= req.CutoffNanos {
		l.metrics.flushBetweenCutoffBufferEnd.Inc(1)
		return
	}

	// Metrics between cutoff and now-bufferAfterCutoff are discarded.
	l.flushBeforeFn(bufferEndNanos, discardType)
	l.metrics.flushAfterBufferEnd.Inc(1)
}

func (l *metricList) DiscardBefore(beforeNanos int64) {
	l.flushBeforeFn(beforeNanos, discardType)
	l.metrics.discardBefore.Inc(1)
}

// flushBefore flushes or discards data before a given time based on the flush type.
// It is not thread-safe.
func (l *metricList) flushBefore(beforeNanos int64, flushType flushType) {
	alignedBeforeNanos := time.Unix(0, beforeNanos).Truncate(l.resolution).UnixNano()
	if l.LastFlushedNanos() >= alignedBeforeNanos {
		l.metrics.flushBeforeStale.Inc(1)
		return
	}

	flushBeforeStart := l.nowFn()
	l.toCollect = l.toCollect[:0]
	flushFn := l.consumeAggMetricFn
	if flushType == discardType {
		flushFn = l.discardAggMetricFn
	}

	// Flush out aggregations, may need to do it in batches if the read lock
	// is held for too long.
	l.RLock()
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		// If the element is eligible for collection after the values are
		// processed, close it and reset the value to nil.
		elem := e.Value.(metricElem)
		if elem.Consume(alignedBeforeNanos, flushFn) {
			elem.Close()
			e.Value = nil
			l.toCollect = append(l.toCollect, e)
		}
	}
	l.RUnlock()

	// Flush remaining bytes buffered in the writer.
	if flushType == consumeType {
		if err := l.flushWriter.Flush(); err != nil {
			l.metrics.flushWriterFlushErrors.Inc(1)
		} else {
			l.metrics.flushWriterFlushSuccess.Inc(1)
		}
	}

	// Collect tombstoned elements.
	l.Lock()
	for _, e := range l.toCollect {
		l.aggregations.Remove(e)
	}
	numCollected := len(l.toCollect)
	l.Unlock()

	atomic.StoreInt64(&l.lastFlushedNanos, alignedBeforeNanos)
	l.metrics.flushElemCollected.Inc(int64(numCollected))
	flushBeforeDuration := l.nowFn().Sub(flushBeforeStart)
	l.metrics.flushBeforeDuration.Record(flushBeforeDuration)
}

func (l *metricList) consumeAggregatedMetric(
	idPrefix []byte,
	id metricid.RawID,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
) {
	chunkedID := metricid.ChunkedID{
		Prefix: idPrefix,
		Data:   []byte(id),
		Suffix: idSuffix,
	}
	chunkedMetricWithPolicy := aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: chunkedID,
			TimeNanos: timeNanos,
			Value:     value,
		},
		StoragePolicy: sp,
	}
	if err := l.flushWriter.Write(chunkedMetricWithPolicy); err != nil {
		l.metrics.flushMetricConsumeErrors.Inc(1)
	} else {
		l.metrics.flushMetricConsumeSuccess.Inc(1)
	}
}

// discardAggregatedMetric discards aggregated metrics.
// nolint: unparam
func (l *metricList) discardAggregatedMetric(
	idPrefix []byte,
	id metricid.RawID,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
) {
	l.metrics.flushMetricDiscarded.Inc(1)
}

type newMetricListFn func(
	shard uint32,
	resolution time.Duration,
	opts Options,
) (*metricList, error)

// metricLists contains all the metric lists.
type metricLists struct {
	sync.RWMutex

	shard           uint32
	opts            Options
	newMetricListFn newMetricListFn
	closed          bool
	lists           map[time.Duration]*metricList
}

func newMetricLists(shard uint32, opts Options) *metricLists {
	return &metricLists{
		shard:           shard,
		opts:            opts,
		newMetricListFn: newMetricList,
		lists:           make(map[time.Duration]*metricList),
	}
}

// Len returns the number of lists.
func (l *metricLists) Len() int {
	l.RLock()
	numLists := len(l.lists)
	l.RUnlock()
	return numLists
}

// FindOrCreate looks up a metric list based on a resolution,
// and if not found, creates one.
func (l *metricLists) FindOrCreate(resolution time.Duration) (*metricList, error) {
	l.RLock()
	if l.closed {
		l.RUnlock()
		return nil, errListsClosed
	}
	list, exists := l.lists[resolution]
	if exists {
		l.RUnlock()
		return list, nil
	}
	l.RUnlock()

	l.Lock()
	if l.closed {
		l.Unlock()
		return nil, errListsClosed
	}
	list, exists = l.lists[resolution]
	if !exists {
		var err error
		list, err = l.newMetricListFn(l.shard, resolution, l.opts)
		if err != nil {
			return nil, err
		}
		l.lists[resolution] = list
	}
	l.Unlock()

	return list, nil
}

// Tick ticks through each list and returns the list sizes.
func (l *metricLists) Tick() map[time.Duration]int {
	l.RLock()
	defer l.RUnlock()

	activeElems := make(map[time.Duration]int, len(l.lists))
	for _, list := range l.lists {
		resolution := list.Resolution()
		numElems := list.Len()
		activeElems[resolution] = numElems
	}
	return activeElems
}

// Close closes the metric lists.
func (l *metricLists) Close() {
	l.Lock()
	defer l.Unlock()

	if l.closed {
		return
	}
	l.closed = true
	for _, list := range l.lists {
		list.Close()
	}
}
