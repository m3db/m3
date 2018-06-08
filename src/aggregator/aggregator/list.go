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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3aggregator/aggregator/handler"
	"github.com/m3db/m3aggregator/aggregator/handler/writer"
	"github.com/m3db/m3aggregator/client"
	"github.com/m3db/m3metrics/metadata"
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

type metricList interface {
	// Resolution returns the resolution of metrics associated with the flusher.
	Resolution() time.Duration

	// Len returns the number of elements in the list.
	Len() int

	// PushBack pushes a metric element to the back of the list.
	PushBack(value metricElem) (*list.Element, error)

	// Close closes the metric list.
	Close()
}

type metricProcessingMetrics struct {
	metricConsumeSuccess tally.Counter
	metricConsumeErrors  tally.Counter
	metricDiscarded      tally.Counter
}

func newMetricProcessingMetrics(scope tally.Scope) metricProcessingMetrics {
	return metricProcessingMetrics{
		metricConsumeSuccess: scope.Counter("metric-consume-success"),
		metricConsumeErrors:  scope.Counter("metric-consume-errors"),
		metricDiscarded:      scope.Counter("metric-discarded"),
	}
}

type writerMetrics struct {
	flushSuccess tally.Counter
	flushErrors  tally.Counter
}

func newWriterMetrics(scope tally.Scope) writerMetrics {
	return writerMetrics{
		flushSuccess: scope.Counter("flush-success"),
		flushErrors:  scope.Counter("flush-errors"),
	}
}

type baseMetricListMetrics struct {
	flushLocal                  metricProcessingMetrics
	flushLocalWriter            writerMetrics
	flushForwarded              metricProcessingMetrics
	flushForwardedWriter        writerMetrics
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

func newMetricListMetrics(scope tally.Scope) baseMetricListMetrics {
	flushScope := scope.SubScope("flush")
	flushBeforeScope := scope.SubScope("flush-before")
	flushLocalScope := flushScope.Tagged(map[string]string{"flush-type": "local"})
	flushLocalWriterScope := flushLocalScope.SubScope("writer")
	flushForwardedScope := flushScope.Tagged(map[string]string{"flush-type": "forwarded"})
	flushForwardedWriterScope := flushForwardedScope.SubScope("writer")
	return baseMetricListMetrics{
		flushLocal:                  newMetricProcessingMetrics(flushLocalScope),
		flushLocalWriter:            newWriterMetrics(flushLocalWriterScope),
		flushForwarded:              newMetricProcessingMetrics(flushForwardedScope),
		flushForwardedWriter:        newWriterMetrics(flushForwardedWriterScope),
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

// baseMetricList is a metric list storing aggregations at a given resolution and
// flushing them periodically.
type baseMetricList struct {
	sync.RWMutex

	shard            uint32
	opts             Options
	nowFn            clock.NowFn
	timeLock         *sync.RWMutex
	flushHandler     handler.Handler
	localWriter      writer.Writer
	forwardedWriter  client.AdminClient
	resolution       time.Duration
	isEarlierThanFn  isEarlierThanFn
	timestampNanosFn timestampNanosFn

	closed           bool
	aggregations     *list.List
	lastFlushedNanos int64
	toCollect        []*list.Element
	metrics          baseMetricListMetrics

	flushBeforeFn            flushBeforeFn
	consumeLocalMetricFn     flushLocalMetricFn
	discardLocalMetricFn     flushLocalMetricFn
	consumeForwardedMetricFn flushForwardedMetricFn
	discardForwardedMetricFn flushForwardedMetricFn
}

func newBaseMetricList(
	shard uint32,
	resolution time.Duration,
	isEarlierThanFn isEarlierThanFn,
	timestampNanosFn timestampNanosFn,
	opts Options,
) (*baseMetricList, error) {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("list").Tagged(
		map[string]string{"resolution": resolution.String()},
	)
	flushHandler := opts.FlushHandler()
	localWriter, err := flushHandler.NewWriter(scope.SubScope("writer"))
	if err != nil {
		return nil, err
	}

	l := &baseMetricList{
		shard:            shard,
		opts:             opts,
		nowFn:            opts.ClockOptions().NowFn(),
		timeLock:         opts.TimeLock(),
		flushHandler:     flushHandler,
		localWriter:      localWriter,
		forwardedWriter:  opts.AdminClient(),
		resolution:       resolution,
		isEarlierThanFn:  isEarlierThanFn,
		timestampNanosFn: timestampNanosFn,
		aggregations:     list.New(),
		metrics:          newMetricListMetrics(scope),
	}
	l.flushBeforeFn = l.flushBefore
	l.consumeLocalMetricFn = l.consumeLocalMetric
	l.discardLocalMetricFn = l.discardLocalMetric
	l.consumeForwardedMetricFn = l.consumeForwardedMetric
	l.discardForwardedMetricFn = l.discardForwardedMetric

	return l, nil
}

func (l *baseMetricList) Shard() uint32                { return l.shard }
func (l *baseMetricList) Resolution() time.Duration    { return l.resolution }
func (l *baseMetricList) FlushInterval() time.Duration { return l.resolution }
func (l *baseMetricList) LastFlushedNanos() int64      { return atomic.LoadInt64(&l.lastFlushedNanos) }

// Len returns the number of elements in the list.
func (l *baseMetricList) Len() int {
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
func (l *baseMetricList) PushBack(value metricElem) (*list.Element, error) {
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
func (l *baseMetricList) Close() bool {
	l.Lock()
	defer l.Unlock()

	if l.closed {
		return false
	}
	l.localWriter.Close()
	// NB: forwardedWriter is shared across lists and closed during
	// aggregator shutdown.
	l.closed = true
	return true
}

func (l *baseMetricList) Flush(req flushRequest) {
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

func (l *baseMetricList) DiscardBefore(beforeNanos int64) {
	l.flushBeforeFn(beforeNanos, discardType)
	l.metrics.discardBefore.Inc(1)
}

// flushBefore flushes or discards data before a given time based on the flush type.
// It is not thread-safe.
func (l *baseMetricList) flushBefore(beforeNanos int64, flushType flushType) {
	if l.LastFlushedNanos() >= beforeNanos {
		l.metrics.flushBeforeStale.Inc(1)
		return
	}

	flushBeforeStart := l.nowFn()
	l.toCollect = l.toCollect[:0]
	flushLocalFn := l.consumeLocalMetricFn
	flushForwardedFn := l.consumeForwardedMetricFn
	if flushType == discardType {
		flushLocalFn = l.discardLocalMetricFn
		flushForwardedFn = l.discardForwardedMetricFn
	}

	// Flush out aggregations, may need to do it in batches if the read lock
	// is held for too long.
	l.RLock()
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		// If the element is eligible for collection after the values are
		// processed, close it and reset the value to nil.
		elem := e.Value.(metricElem)
		if elem.Consume(
			beforeNanos,
			l.isEarlierThanFn,
			l.timestampNanosFn,
			flushLocalFn,
			flushForwardedFn,
		) {
			elem.Close()
			e.Value = nil
			l.toCollect = append(l.toCollect, e)
		}
	}
	l.RUnlock()

	if flushType == consumeType {
		// Flush remaining bytes buffered in the local writer.
		if err := l.localWriter.Flush(); err != nil {
			l.metrics.flushLocalWriter.flushErrors.Inc(1)
		} else {
			l.metrics.flushLocalWriter.flushSuccess.Inc(1)
		}

		// Flush remaining bytes buffered in the forwarded writer.
		if l.forwardedWriter != nil {
			if err := l.forwardedWriter.Flush(); err != nil {
				l.metrics.flushForwardedWriter.flushErrors.Inc(1)
			} else {
				l.metrics.flushForwardedWriter.flushSuccess.Inc(1)
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

func (l *baseMetricList) consumeLocalMetric(
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
	if err := l.localWriter.Write(chunkedMetricWithPolicy); err != nil {
		l.metrics.flushLocal.metricConsumeErrors.Inc(1)
	} else {
		l.metrics.flushLocal.metricConsumeSuccess.Inc(1)
	}
}

// nolint: unparam
func (l *baseMetricList) discardLocalMetric(
	idPrefix []byte,
	id metricid.RawID,
	idSuffix []byte,
	timeNanos int64,
	value float64,
	sp policy.StoragePolicy,
) {
	l.metrics.flushLocal.metricDiscarded.Inc(1)
}

func (l *baseMetricList) consumeForwardedMetric(
	metric aggregated.Metric,
	meta metadata.ForwardMetadata,
) {
	if err := l.forwardedWriter.WriteForwarded(metric, meta); err != nil {
		l.metrics.flushForwarded.metricConsumeErrors.Inc(1)
	} else {
		l.metrics.flushForwarded.metricConsumeSuccess.Inc(1)
	}
}

// nolint: unparam
func (l *baseMetricList) discardForwardedMetric(
	metric aggregated.Metric,
	meta metadata.ForwardMetadata,
) {
	l.metrics.flushForwarded.metricDiscarded.Inc(1)
}

// The timestamp of a standard metric is the end time boundary of the aggregation
// window when the metric is flushed. As such, only the aggregation windows whose
// end time boundaries are no later than the target time can be flushed.
func isStandardMetricEarlierThan(
	windowStartNanos int64,
	resolution time.Duration,
	targetNanos int64,
) bool {
	return windowStartNanos+resolution.Nanoseconds() <= targetNanos
}

// The timestamp of a standard metric is set to the end time boundary of the aggregation
// window when the metric is flushed.
func standardMetricTimestampNanos(windowStartNanos int64, resolution time.Duration) int64 {
	return windowStartNanos + resolution.Nanoseconds()
}

// standardMetricListID is the id of a standard metric list for a given shard.
type standardMetricListID struct {
	resolution time.Duration
}

func (id standardMetricListID) toMetricListID() metricListID {
	return metricListID{listType: standardMetricListType, standard: id}
}

// standardMetricList is a list storing aggregations of incoming standard metrics
// (i.e., currently untimed metrics).
type standardMetricList struct {
	*baseMetricList

	log      log.Logger
	flushMgr FlushManager
}

func newStandardMetricList(
	shard uint32,
	id standardMetricListID,
	opts Options,
) (*standardMetricList, error) {
	l, err := newBaseMetricList(
		shard,
		id.resolution,
		isStandardMetricEarlierThan,
		standardMetricTimestampNanos,
		opts,
	)
	if err != nil {
		return nil, err
	}
	sl := &standardMetricList{
		baseMetricList: l,
		log:            opts.InstrumentOptions().Logger(),
		flushMgr:       opts.FlushManager(),
	}
	sl.flushMgr.Register(sl)
	return sl, nil
}

func (l *standardMetricList) ID() metricListID {
	return standardMetricListID{resolution: l.resolution}.toMetricListID()
}

func (l *standardMetricList) Close() {
	if !l.baseMetricList.Close() {
		return
	}
	if err := l.flushMgr.Unregister(l); err != nil {
		l.log.Errorf("error unregistering list: %v", err)
	}
}

// The timestamp of a forwarded metric is set to the start time boundary of the aggregation
// window when the metric is flushed. This is because the timestamp of a forwarded metric
// is already aligned to the end of the aggregation window the first time it is forwarded
// and as such there is no need to realign it to the end of the window during subsequent flushes.
func forwardedMetricTimestampNanos(windowStartNanos int64, _ time.Duration) int64 {
	return windowStartNanos
}

// forwardedMetricListID is the id of a forwarded metric list.
type forwardedMetricListID struct {
	resolution        time.Duration
	numForwardedTimes int
}

func (id forwardedMetricListID) toMetricListID() metricListID {
	return metricListID{listType: forwardedMetricListType, forwarded: id}
}

// forwardedMetricList is a list storing aggregations of incoming forwarded metrics
// have been forwarded a given number of times at a given resolution.
type forwardedMetricList struct {
	*baseMetricList

	numForwardedTimes int
	flushOffset       time.Duration
	log               log.Logger
	flushMgr          FlushManager
}

func newForwardedMetricList(
	shard uint32,
	id forwardedMetricListID,
	opts Options,
) (*forwardedMetricList, error) {
	var (
		resolution           = id.resolution
		numForwardedTimes    = id.numForwardedTimes
		maxLatenessAllowedFn = opts.MaxAllowedForwardingDelayFn()
		maxLatenessAllowed   = maxLatenessAllowedFn(resolution, numForwardedTimes)
		timestampNanosFn     = forwardedMetricTimestampNanos
	)
	isEarlierThanFn := func(
		windowStartNanos int64,
		_ time.Duration,
		targetNanos int64,
	) bool {
		return targetNanos-windowStartNanos >= maxLatenessAllowed.Nanoseconds()
	}
	l, err := newBaseMetricList(shard, resolution, isEarlierThanFn, timestampNanosFn, opts)
	if err != nil {
		return nil, err
	}

	flushOffset := maxLatenessAllowed - maxLatenessAllowed.Truncate(l.FlushInterval())
	fl := &forwardedMetricList{
		baseMetricList:    l,
		numForwardedTimes: numForwardedTimes,
		flushOffset:       flushOffset,
		log:               opts.InstrumentOptions().Logger(),
		flushMgr:          opts.FlushManager(),
	}
	fl.flushMgr.Register(fl)
	return fl, nil
}

func (l *forwardedMetricList) ID() metricListID {
	return forwardedMetricListID{
		resolution:        l.resolution,
		numForwardedTimes: l.numForwardedTimes,
	}.toMetricListID()
}

func (l *forwardedMetricList) FlushOffset() time.Duration {
	return l.flushOffset
}

func (l *forwardedMetricList) Close() {
	if !l.baseMetricList.Close() {
		return
	}
	if err := l.flushMgr.Unregister(l); err != nil {
		l.log.Errorf("error unregistering list: %v", err)
	}
}

type metricListType int

const (
	standardMetricListType metricListType = iota
	forwardedMetricListType
)

func (t metricListType) String() string {
	switch t {
	case standardMetricListType:
		return "standard"
	case forwardedMetricListType:
		return "forwarded"
	default:
		// Should never get here.
		return "unknown"
	}
}

type metricListID struct {
	listType  metricListType
	standard  standardMetricListID
	forwarded forwardedMetricListID
}

func newMetricList(shard uint32, id metricListID, opts Options) (metricList, error) {
	switch id.listType {
	case standardMetricListType:
		return newStandardMetricList(shard, id.standard, opts)
	case forwardedMetricListType:
		return newForwardedMetricList(shard, id.forwarded, opts)
	default:
		return nil, fmt.Errorf("unknown list type: %v", id.listType)
	}
}

type newMetricListFn func(shard uint32, id metricListID, opts Options) (metricList, error)

// metricLists contains all the metric lists.
type metricLists struct {
	sync.RWMutex

	shard uint32
	opts  Options

	closed          bool
	lists           map[metricListID]metricList
	newMetricListFn newMetricListFn
}

func newMetricLists(shard uint32, opts Options) *metricLists {
	return &metricLists{
		shard:           shard,
		opts:            opts,
		lists:           make(map[metricListID]metricList),
		newMetricListFn: newMetricList,
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
func (l *metricLists) FindOrCreate(id metricListID) (metricList, error) {
	l.RLock()
	if l.closed {
		l.RUnlock()
		return nil, errListsClosed
	}
	list, exists := l.lists[id]
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
	list, exists = l.lists[id]
	if !exists {
		var err error
		list, err = l.newMetricListFn(l.shard, id, l.opts)
		if err != nil {
			return nil, err
		}
		l.lists[id] = list
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
