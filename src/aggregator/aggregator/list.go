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
	metricID "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
)

var (
	errListClosed  = errors.New("metric list is closed")
	errListsClosed = errors.New("metric lists are closed")
)

type metricListMetrics struct {
	flushMetricConsumeSuccess tally.Counter
	flushMetricConsumeErrors  tally.Counter
	flushBufferConsumeSuccess tally.Counter
	flushBufferConsumeErrors  tally.Counter
	flushMetricDiscarded      tally.Counter
	flushElemCollected        tally.Counter
	flushDuration             tally.Timer
	flushConsume              tally.Counter
	flushDiscard              tally.Counter
	flushBeforeStale          tally.Counter
	flushBeforeDuration       tally.Timer
	discardBefore             tally.Counter
}

func newMetricListMetrics(scope tally.Scope) metricListMetrics {
	flushScope := scope.SubScope("flush")
	flushBeforeScope := scope.SubScope("flush-before")
	return metricListMetrics{
		flushMetricConsumeSuccess: flushScope.Counter("metric-consume-success"),
		flushMetricConsumeErrors:  flushScope.Counter("metric-consume-errors"),
		flushBufferConsumeSuccess: flushScope.Counter("buffer-consume-success"),
		flushBufferConsumeErrors:  flushScope.Counter("buffer-consume-errors"),
		flushMetricDiscarded:      flushScope.Counter("metric-discarded"),
		flushElemCollected:        flushScope.Counter("elem-collected"),
		flushDuration:             flushScope.Timer("duration"),
		flushConsume:              flushScope.Counter("consume"),
		flushDiscard:              flushScope.Counter("discard"),
		flushBeforeStale:          flushBeforeScope.Counter("stale"),
		flushBeforeDuration:       flushBeforeScope.Timer("duration"),
		discardBefore:             scope.Counter("discard-before"),
	}
}

type encodeFn func(mp aggregated.ChunkedMetricWithStoragePolicy) error
type flushBeforeFn func(beforeNanos int64, flushType flushType)

// metricList stores aggregated metrics at a given resolution
// and flushes aggregations periodically.
type metricList struct {
	sync.RWMutex

	shard         uint32
	opts          Options
	nowFn         clock.NowFn
	log           xlog.Logger
	timeLock      *sync.RWMutex
	maxFlushSize  int
	flushHandler  Handler
	encoderPool   msgpack.BufferedEncoderPool
	resolution    time.Duration
	flushInterval time.Duration
	flushMgr      FlushManager

	aggregations       *list.List
	lastFlushedNanos   int64
	encoder            msgpack.AggregatedEncoder
	toCollect          []*list.Element
	closed             bool
	encodeFn           encodeFn
	flushBeforeFn      flushBeforeFn
	consumeAggMetricFn aggMetricFn
	discardAggMetricFn aggMetricFn
	metrics            metricListMetrics
}

func newMetricList(shard uint32, resolution time.Duration, opts Options) *metricList {
	// NB(xichen): by default the flush interval is the same as metric
	// resolution, unless the resolution is smaller than the minimum flush
	// interval, in which case we use the min flush interval to avoid excessing
	// CPU overhead due to flushing.
	flushInterval := resolution
	if minFlushInterval := opts.MinFlushInterval(); flushInterval < minFlushInterval {
		flushInterval = minFlushInterval
	}
	scope := opts.InstrumentOptions().MetricsScope().SubScope("list").Tagged(
		map[string]string{"resolution": resolution.String()},
	)
	encoderPool := opts.BufferedEncoderPool()
	encoder := encoderPool.Get()
	encoder.Reset()
	l := &metricList{
		shard:         shard,
		opts:          opts,
		nowFn:         opts.ClockOptions().NowFn(),
		log:           opts.InstrumentOptions().Logger(),
		timeLock:      opts.TimeLock(),
		maxFlushSize:  opts.MaxFlushSize(),
		flushHandler:  opts.FlushHandler(),
		encoderPool:   encoderPool,
		resolution:    resolution,
		flushInterval: flushInterval,
		flushMgr:      opts.FlushManager(),
		aggregations:  list.New(),
		encoder:       msgpack.NewAggregatedEncoder(encoder),
		metrics:       newMetricListMetrics(scope),
	}
	l.encodeFn = l.encoder.EncodeChunkedMetricWithStoragePolicy
	l.flushBeforeFn = l.flushBefore
	l.consumeAggMetricFn = l.consumeAggregatedMetric
	l.discardAggMetricFn = l.discardAggregatedMetric
	l.flushMgr.Register(l)

	return l
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
	l.closed = true
	l.Unlock()

	// Unregister the list outside the list lock to avoid holding the list lock
	// while attempting to acquire the flush manager lock potentially causing a
	// deadlock.
	if err := l.flushMgr.Unregister(l); err != nil {
		l.log.Errorf("error unregistering list: %v", err)
	}
}

func (l *metricList) Flush(cutoverNanos, cutoffNanos int64) {
	start := l.nowFn()
	alignedNowNanos := l.alignedNowNanos()

	// NB(xichen): If alignedNowNanos <= cutoverNanos, the aggregated metrics should
	// be discarded because they are aggregated before traffic was cut over. On the
	// other hand, if alignedNowNanos > cutoffNanos, the aggregated metrics should
	// also be discarded because they are aggregated after traffic was cut off.
	if alignedNowNanos > cutoverNanos && alignedNowNanos <= cutoffNanos {
		l.flushBeforeFn(alignedNowNanos, consumeType)
		l.metrics.flushConsume.Inc(1)
	} else {
		l.flushBeforeFn(alignedNowNanos, discardType)
		l.metrics.flushDiscard.Inc(1)
	}
	took := l.nowFn().Sub(start)
	l.metrics.flushDuration.Record(took)
}

func (l *metricList) DiscardBefore(beforeNanos int64) {
	l.flushBeforeFn(beforeNanos, discardType)
	l.metrics.discardBefore.Inc(1)
}

func (l *metricList) flushBefore(beforeNanos int64, flushType flushType) {
	if l.LastFlushedNanos() >= beforeNanos {
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
		if elem.Consume(beforeNanos, flushFn) {
			elem.Close()
			e.Value = nil
			l.toCollect = append(l.toCollect, e)
		}
	}
	l.RUnlock()

	// Flush remaining bytes in the buffer.
	if flushType == consumeType {
		if encoder := l.encoder.Encoder(); len(encoder.Bytes()) > 0 {
			newEncoder := l.encoderPool.Get()
			newEncoder.Reset()
			l.encoder.Reset(newEncoder)
			if err := l.flushHandler.Handle(NewRefCountedBuffer(encoder)); err != nil {
				l.log.Errorf("flushing metrics error: %v", err)
				l.metrics.flushBufferConsumeErrors.Inc(1)
			} else {
				l.metrics.flushBufferConsumeSuccess.Inc(1)
			}
		}
	}

	// Collect tombstoned elements.
	l.Lock()
	for _, e := range l.toCollect {
		l.aggregations.Remove(e)
	}
	numCollected := len(l.toCollect)
	l.Unlock()

	atomic.StoreInt64(&l.lastFlushedNanos, beforeNanos)
	l.metrics.flushElemCollected.Inc(int64(numCollected))
	flushBeforeDuration := l.nowFn().Sub(flushBeforeStart)
	l.metrics.flushBeforeDuration.Record(flushBeforeDuration)
}

func (l *metricList) alignedNowNanos() int64 {
	// NB(xichen): it is important to determine ticking start time within the time lock
	// because this ensures all the actions before `now` have completed if those actions
	// are protected by the same read lock.
	l.timeLock.Lock()
	now := l.nowFn()
	resolution := l.resolution
	l.timeLock.Unlock()
	alignedNowNanos := now.Truncate(resolution).UnixNano()
	return alignedNowNanos
}

func (l *metricList) consumeAggregatedMetric(
	idPrefix []byte,
	id metricID.RawID,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
) {
	encoder := l.encoder.Encoder()
	buffer := encoder.Buffer()
	sizeBefore := buffer.Len()
	if err := l.encodeFn(aggregated.ChunkedMetricWithStoragePolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: metricID.ChunkedID{
				Prefix: idPrefix,
				Data:   []byte(id),
				Suffix: idSuffix,
			},
			TimeNanos: timeNanos,
			Value:     value,
		},
		StoragePolicy: sp,
	}); err != nil {
		l.log.WithFields(
			xlog.NewLogField("idPrefix", string(idPrefix)),
			xlog.NewLogField("id", id.String()),
			xlog.NewLogField("idSuffix", string(idSuffix)),
			xlog.NewLogField("timestamp", time.Unix(0, timeNanos).String()),
			xlog.NewLogField("value", value),
			xlog.NewLogField("policy", sp.String()),
			xlog.NewLogErrField(err),
		).Error("encode metric with policy error")
		l.metrics.flushMetricConsumeErrors.Inc(1)
		buffer.Truncate(sizeBefore)
		// Clear out the encoder error.
		l.encoder.Reset(encoder)
		return
	}
	l.metrics.flushMetricConsumeSuccess.Inc(1)
	sizeAfter := buffer.Len()
	// If the buffer size is not big enough, do nothing.
	if sizeAfter < l.maxFlushSize {
		return
	}
	// Otherwise we get a new buffer and copy the bytes exceeding the max
	// flush size to it, swap the new buffer with the old one, and flush out
	// the old buffer.
	encoder2 := l.encoderPool.Get()
	encoder2.Reset()
	data := encoder.Bytes()
	encoder2.Buffer().Write(data[sizeBefore:sizeAfter])
	l.encoder.Reset(encoder2)
	buffer.Truncate(sizeBefore)
	if err := l.flushHandler.Handle(NewRefCountedBuffer(encoder)); err != nil {
		l.log.Errorf("flushing metrics error: %v", err)
		l.metrics.flushBufferConsumeErrors.Inc(1)
	} else {
		l.metrics.flushBufferConsumeSuccess.Inc(1)
	}
}

// discardAggregatedMetric discards aggregated metrics.
// nolint: unparam
func (l *metricList) discardAggregatedMetric(
	idPrefix []byte,
	id metricID.RawID,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
) {
	l.metrics.flushMetricDiscarded.Inc(1)
}

type newMetricListFn func(shard uint32, resolution time.Duration, opts Options) *metricList

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
		list = l.newMetricListFn(l.shard, resolution, l.opts)
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
