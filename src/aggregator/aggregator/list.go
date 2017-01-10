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
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
)

var (
	errListClosed  = errors.New("metric list is closed")
	errListsClosed = errors.New("metric lists are closed")
)

type encodeFn func(mp aggregated.ChunkedMetricWithPolicy) error

// MetricList stores aggregated metrics at a given resolution
// and flushes aggregations periodically
// TODO(xichen): add metrics
type MetricList struct {
	sync.RWMutex

	opts         Options
	nowFn        clock.NowFn
	log          xlog.Logger
	timeLock     *sync.RWMutex
	maxFlushSize int
	flushFn      FlushFn
	encoderPool  msgpack.BufferedEncoderPool

	resolution    time.Duration
	flushInterval time.Duration
	aggregations  *list.List
	timer         *time.Timer
	idBuf         bytes.Buffer
	encoder       msgpack.AggregatedEncoder
	toCollect     []*list.Element
	closed        bool
	doneCh        chan struct{}
	wgTick        sync.WaitGroup
	encodeFn      encodeFn
	waitForFn     waitForFn
}

func newMetricList(resolution time.Duration, opts Options) *MetricList {
	// NB(xichen): by default the flush interval is the same as metric
	// resolution, unless the resolution is smaller than the minimum flush
	// interval, in which case we use the min flush interval to avoid excessing
	// CPU overhead due to flushing
	flushInterval := resolution
	if minFlushInterval := opts.MinFlushInterval(); flushInterval < minFlushInterval {
		flushInterval = minFlushInterval
	}

	encoderPool := opts.BufferedEncoderPool()
	l := &MetricList{
		opts:          opts,
		nowFn:         opts.ClockOptions().NowFn(),
		log:           opts.InstrumentOptions().Logger(),
		timeLock:      opts.TimeLock(),
		maxFlushSize:  opts.MaxFlushSize(),
		flushFn:       opts.FlushFn(),
		encoderPool:   encoderPool,
		resolution:    resolution,
		flushInterval: flushInterval,
		aggregations:  list.New(),
		timer:         time.NewTimer(0),
		encoder:       msgpack.NewAggregatedEncoder(encoderPool.Get()),
		doneCh:        make(chan struct{}),
	}
	l.encodeFn = l.encoder.EncodeChunkedMetricWithPolicy
	l.waitForFn = time.After

	// Start ticking
	if l.flushInterval > 0 {
		l.wgTick.Add(1)
		go l.tick()
	}

	return l
}

// Close closes the list
func (l *MetricList) Close() {
	l.Lock()
	if l.closed {
		l.Unlock()
		return
	}
	l.closed = true
	l.Unlock()

	// Waiting for the ticking goroutine to finish
	close(l.doneCh)
	l.wgTick.Wait()
}

// PushBack adds an element to the list
// NB(xichen): the container list doesn't provide an API to directly
// insert a list element, therefore making it impossible to pool the
// elements and manage their lifetimes. If this becomes an issue,
// need to switch to a custom type-specific list implementation.
func (l *MetricList) PushBack(value interface{}) (*list.Element, error) {
	l.Lock()
	if l.closed {
		l.Unlock()
		return nil, errListClosed
	}
	elem := l.aggregations.PushBack(value)
	l.Unlock()
	return elem, nil
}

// tick performs periodic maintenance tasks (e.g., flushing out aggregated metrics)
func (l *MetricList) tick() {
	defer l.wgTick.Done()

	for {
		select {
		case <-l.doneCh:
			return
		default:
			l.tickInternal()
		}
	}
}

func (l *MetricList) tickInternal() {
	// NB(xichen): it is important to determine ticking start time within the time lock
	// because this ensures all the actions before `start` have completed if those actions
	// are protected by the same read lock
	l.timeLock.Lock()
	start := l.nowFn()
	alignedStart := start.Truncate(l.resolution)
	l.timeLock.Unlock()

	// Reset states reused across ticks
	l.idBuf.Reset()
	l.toCollect = l.toCollect[:0]

	// Flush out aggregations, may need to do it in batches if the read lock
	// is held for too long
	l.RLock()
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		switch elem := e.Value.(type) {
		case metricElem:
			// If the element is eligible for collection after the values are
			// processed, close it and reset the value to nil
			if elem.ReadAndDiscard(alignedStart, l.processAggregatedMetric) {
				elem.Close()
				e.Value = nil
				l.toCollect = append(l.toCollect, e)
			}
		default:
			panic(fmt.Errorf("unknown metric element type: %T", elem))
		}
	}
	l.RUnlock()

	// Flush remaining bytes in the buffer
	if encoder := l.encoder.Encoder(); len(encoder.Bytes()) > 0 {
		l.encoder.Reset(l.encoderPool.Get())
		if err := l.flushFn(encoder); err != nil {
			l.log.Errorf("flushing metrics error: %v", err)
		}
	}

	// Collect tombstoned elements
	l.Lock()
	for _, e := range l.toCollect {
		l.aggregations.Remove(e)
	}
	l.Unlock()

	// TODO(xichen): add metrics
	tickDuration := l.nowFn().Sub(start)
	if tickDuration < l.flushInterval {
		// NB(xichen): use a channel here instead of sleeping in case
		// server needs to close and we don't tick frequently enough
		select {
		case <-l.waitForFn(l.flushInterval - tickDuration):
		case <-l.doneCh:
		}
	}
}

func (l *MetricList) processAggregatedMetric(
	idPrefix []byte,
	id metric.ID,
	idSuffix []byte,
	timestamp time.Time,
	value float64,
	policy policy.Policy,
) {
	encoder := l.encoder.Encoder()
	sizeBefore := encoder.Buffer.Len()
	if err := l.encodeFn(aggregated.ChunkedMetricWithPolicy{
		ChunkedMetric: aggregated.ChunkedMetric{
			ChunkedID: metric.ChunkedID{
				Prefix: idPrefix,
				Data:   []byte(id),
				Suffix: idSuffix,
			},
			Timestamp: timestamp,
			Value:     value,
		},
		Policy: policy,
	}); err != nil {
		// TODO(xichen): add metrics
		l.log.WithFields(
			xlog.NewLogField("idPrefix", string(idPrefix)),
			xlog.NewLogField("id", id.String()),
			xlog.NewLogField("idSuffix", string(idSuffix)),
			xlog.NewLogField("timestamp", timestamp.String()),
			xlog.NewLogField("value", value),
			xlog.NewLogField("policy", policy),
		).Error("encode metric with policy error")
		encoder.Buffer.Truncate(sizeBefore)
		// Clear out the encoder error
		l.encoder.Reset(encoder)
		return
	}
	sizeAfter := encoder.Buffer.Len()
	// If the buffer size is not big enough, do nothing
	if sizeAfter < l.maxFlushSize {
		return
	}
	// Otherwise we get a new buffer and copy the bytes exceeding the max
	// flush size to it, swap the new buffer with the old one, and flush out
	// the old buffer
	encoder2 := l.encoderPool.Get()
	data := encoder.Bytes()
	encoder2.Buffer.Write(data[sizeBefore:sizeAfter])
	l.encoder.Reset(encoder2)
	encoder.Buffer.Truncate(sizeBefore)
	if err := l.flushFn(encoder); err != nil {
		l.log.Errorf("flushing metrics error: %v", err)
	}
}

type newMetricListFn func(resolution time.Duration, opts Options) *MetricList

// MetricLists contains all the metric lists
type MetricLists struct {
	sync.RWMutex

	opts            Options
	newMetricListFn newMetricListFn
	closed          bool
	lists           map[time.Duration]*MetricList
}

func newMetricLists(opts Options) *MetricLists {
	return &MetricLists{
		opts:            opts,
		newMetricListFn: newMetricList,
		lists:           make(map[time.Duration]*MetricList),
	}
}

// Len returns the number of lists
func (l *MetricLists) Len() int {
	l.RLock()
	numLists := len(l.lists)
	l.RUnlock()
	return numLists
}

// Close closes the metric lists
func (l *MetricLists) Close() {
	l.Lock()
	if l.closed {
		l.Unlock()
		return
	}
	l.closed = true
	for _, list := range l.lists {
		list.Close()
	}
	l.Unlock()
}

// FindOrCreate looks up a metric list based on a resolution,
// and if not found, creates one
func (l *MetricLists) FindOrCreate(resolution time.Duration) (*MetricList, error) {
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
		list = l.newMetricListFn(resolution, l.opts)
		l.lists[resolution] = list
	}
	l.Unlock()

	return list, nil
}
