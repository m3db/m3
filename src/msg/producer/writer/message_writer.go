// Copyright (c) 2018 Uber Technologies, Inc.
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

package writer

import (
	"container/list"
	"sync"
	"time"

	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

const (
	defaultAckMapSize      = 1024
	defaultToBeRetriedSize = 1024
)

type messageWriter interface {
	// Write writes the data.
	Write(d producer.RefCountedData)

	// Ack acknowledges the metadata.
	Ack(meta metadata)

	// Init initialize the message writer.
	Init()

	// Close closes the writer.
	// It should block until all buffered data have been acknowledged.
	Close()

	// AddConsumerWriter adds a consumer writer for the given address.
	AddConsumerWriter(addr string, cw consumerWriter)

	// RemoveConsumerWriter removes the consumer writer for the given address.
	RemoveConsumerWriter(addr string)

	// ReplicatedShardID returns the replicated shard id.
	ReplicatedShardID() uint64

	// CutoverNanos returns the cutover nanoseconds.
	CutoverNanos() int64

	// SetCutoverNanos sets the cutover nanoseconds.
	SetCutoverNanos(nanos int64)

	// CutoffNanos returns the cutoff nanoseconds.
	CutoffNanos() int64

	// SetCutoffNanos sets the cutoff nanoseconds.
	SetCutoffNanos(nanos int64)
}

type messageWriterMetrics struct {
	writeSuccess           tally.Counter
	oneConsumerWriteError  tally.Counter
	allConsumersWriteError tally.Counter
	writeRetry             tally.Counter
	writeNew               tally.Counter
	writeAfterCutoff       tally.Counter
	writeBeforeCutover     tally.Counter
	writeLatency           tally.Timer
	retryBatchLatency      tally.Timer
	queueSize              tally.Gauge
}

func newMessageWriterMetrics(scope tally.Scope) messageWriterMetrics {
	return messageWriterMetrics{
		writeSuccess: scope.Counter("write-success"),
		oneConsumerWriteError: scope.
			Tagged(map[string]string{"error-type": "one-consumer"}).
			Counter("write-error"),
		allConsumersWriteError: scope.
			Tagged(map[string]string{"error-type": "all-consumers"}).
			Counter("write-error"),
		writeRetry: scope.
			Tagged(map[string]string{"write-type": "retry"}).
			Counter("write"),
		writeNew: scope.
			Tagged(map[string]string{"write-type": "new"}).
			Counter("write"),
		writeAfterCutoff: scope.
			Tagged(map[string]string{"reason": "after-cutoff"}).
			Counter("invalid-write"),
		writeBeforeCutover: scope.
			Tagged(map[string]string{"reason": "before-cutover"}).
			Counter("invalid-write"),
		retryBatchLatency: scope.Timer("retry-batch-latency"),
		writeLatency:      scope.Timer("write-latency"),
		queueSize:         scope.Gauge("message-queue-size"),
	}
}

type messageWriterImpl struct {
	sync.RWMutex

	replicatedShardID uint64
	mPool             messagePool
	opts              Options
	retryOpts         retry.Options

	msgID           uint64
	queue           *list.List
	consumerWriters map[string]consumerWriter
	acks            *acks
	cutOffNanos     int64
	cutOverNanos    int64
	toBeRetried     []*message
	isClosed        bool
	doneCh          chan struct{}
	wg              sync.WaitGroup
	m               messageWriterMetrics

	nowFn clock.NowFn
}

func newMessageWriter(
	replicatedShardID uint64,
	mPool messagePool,
	opts Options,
) messageWriter {
	if opts == nil {
		opts = NewOptions()
	}
	return &messageWriterImpl{
		replicatedShardID: replicatedShardID,
		mPool:             mPool,
		opts:              opts,
		retryOpts:         opts.MessageRetryOptions(),
		msgID:             0,
		queue:             list.New(),
		consumerWriters:   make(map[string]consumerWriter),
		acks:              newAckHelper(defaultAckMapSize),
		cutOffNanos:       0,
		cutOverNanos:      0,
		toBeRetried:       make([]*message, 0, defaultToBeRetriedSize),
		isClosed:          false,
		doneCh:            make(chan struct{}),
		m:                 newMessageWriterMetrics(opts.InstrumentOptions().MetricsScope()),
		nowFn:             time.Now,
	}
}

func (w *messageWriterImpl) Write(rd producer.RefCountedData) {
	now := w.nowFn()
	nowNanos := now.UnixNano()
	w.RLock()
	isValid := w.isValidWriteWithLock(nowNanos)
	w.RUnlock()
	if !isValid {
		return
	}
	rd.IncRef()
	msg := w.mPool.Get()

	w.Lock()
	w.msgID++
	meta := metadata{
		shard: w.replicatedShardID,
		id:    w.msgID,
	}
	msg.Reset(meta, rd)
	w.acks.add(meta, msg)
	w.queue.PushBack(msg)
	consumerWriters := w.consumerWriters
	w.Unlock()
	w.write(consumerWriters, msg, nowNanos)
	w.m.writeNew.Inc(1)
	w.m.writeLatency.Record(w.nowFn().Sub(now))
}

func (w *messageWriterImpl) isValidWriteWithLock(nowNanos int64) bool {
	if w.cutOffNanos > 0 && nowNanos >= w.cutOffNanos {
		w.m.writeAfterCutoff.Inc(1)
		return false
	}
	if w.cutOverNanos > 0 && nowNanos < w.cutOverNanos {
		w.m.writeBeforeCutover.Inc(1)
		return false
	}
	return true
}

func (w *messageWriterImpl) write(
	consumerWriters map[string]consumerWriter,
	m *message,
	nowNanos int64,
) {
	m.IncWriteTimes()
	m.IncReads()
	msg, isValid := m.Marshaler()
	if !isValid {
		m.DecReads()
		return
	}
	written := false
	for _, cw := range consumerWriters {
		if err := cw.Write(msg); err != nil {
			w.m.oneConsumerWriteError.Inc(1)
			continue
		}
		written = true
		w.m.writeSuccess.Inc(1)
		break
	}
	m.DecReads()

	if !written {
		// Could not be written to any consumer, will retry later.
		w.m.allConsumersWriteError.Inc(1)
	}
	m.SetRetryAtNanos(w.nextRetryNanos(m.WriteTimes(), nowNanos))
}

func (w *messageWriterImpl) nextRetryNanos(writeTimes int64, nowNanos int64) int64 {
	backoff := retry.BackoffNanos(
		int(writeTimes),
		w.retryOpts.Jitter(),
		w.retryOpts.BackoffFactor(),
		w.retryOpts.InitialBackoff(),
		w.retryOpts.MaxBackoff(),
	)
	return nowNanos + backoff
}

func (w *messageWriterImpl) Ack(meta metadata) {
	w.acks.ack(meta)
}

func (w *messageWriterImpl) Init() {
	w.wg.Add(1)
	go func() {
		w.retryUnacknowledgedUntilClose()
		w.wg.Done()
	}()
}

func (w *messageWriterImpl) retryUnacknowledgedUntilClose() {
	ticker := time.NewTicker(w.opts.MessageQueueScanInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.retryUnacknowledged()
		case <-w.doneCh:
			return
		}
	}
}

func (w *messageWriterImpl) retryUnacknowledged() {
	w.RLock()
	var (
		l           = w.queue.Len()
		e           = w.queue.Front()
		toBeRetried []*message
	)
	w.RUnlock()
	w.m.queueSize.Update(float64(l))
	for e != nil {
		now := w.nowFn()
		nowNanos := now.UnixNano()
		w.Lock()
		e, toBeRetried = w.retryBatchWithLock(e, nowNanos)
		consumerWriters := w.consumerWriters
		w.Unlock()
		for _, m := range toBeRetried {
			w.write(consumerWriters, m, w.nowFn().UnixNano())
			w.m.writeRetry.Inc(1)
		}
		w.m.retryBatchLatency.Record(w.nowFn().Sub(now))
	}
}

// retryBatchWithLock iterates the message queue with a lock.
// It returns after visited enough items or the first item
// to retry so it holds the lock for less time and allows new writes
// to be less blocked, so that one slow message writer does not
// slow down other message writers too much.
func (w *messageWriterImpl) retryBatchWithLock(
	start *list.Element,
	nowNanos int64,
) (*list.Element, []*message) {
	var (
		iterated int
		next     *list.Element
	)
	w.toBeRetried = w.toBeRetried[:0]
	for e := start; e != nil; e = next {
		iterated++
		if iterated > w.opts.MessageRetryBatchSize() {
			break
		}
		next = e.Next()
		m := e.Value.(*message)
		if m.IsDroppedOrAcked() {
			// Try removing the ack in case the data was dropped rather than acked.
			w.acks.remove(m.Metadata())
			w.queue.Remove(e)
			w.mPool.Put(m)
			continue
		}
		if w.isClosed {
			// Simply ack the messages here to mark them as consumed for this
			// message writer, this is useful when user removes a consumer service
			// during runtime that may be unhealthy to consume the messages.
			// So that the unacked messages for the unhealthy consumer services
			// do not stay in memory forever.
			w.Ack(m.Metadata())
			w.queue.Remove(e)
			w.mPool.Put(m)
			continue
		}
		if m.RetryAtNanos() >= nowNanos {
			continue
		}
		w.toBeRetried = append(w.toBeRetried, m)
	}
	return next, w.toBeRetried
}

func (w *messageWriterImpl) Close() {
	w.Lock()
	if w.isClosed {
		w.Unlock()
		return
	}
	w.isClosed = true
	w.Unlock()
	// NB: Wait until all messages cleaned up then close.
	w.waitUntilAllMessageRemoved()
	close(w.doneCh)
	w.wg.Wait()
}

func (w *messageWriterImpl) waitUntilAllMessageRemoved() {
	// The message writers are being closed sequentially, checking isEmpty()
	// before always waiting for the first tick can speed up Close() a lot.
	if w.isEmpty() {
		return
	}
	ticker := time.NewTicker(w.opts.CloseCheckInterval())
	defer ticker.Stop()

	for range ticker.C {
		if w.isEmpty() {
			return
		}
	}
}

func (w *messageWriterImpl) isEmpty() bool {
	w.RLock()
	l := w.queue.Len()
	w.RUnlock()
	return l == 0
}

func (w *messageWriterImpl) ReplicatedShardID() uint64 {
	return w.replicatedShardID
}

func (w *messageWriterImpl) CutoffNanos() int64 {
	w.RLock()
	res := w.cutOffNanos
	w.RUnlock()
	return res
}

func (w *messageWriterImpl) SetCutoffNanos(nanos int64) {
	w.Lock()
	w.cutOffNanos = nanos
	w.Unlock()
}

func (w *messageWriterImpl) CutoverNanos() int64 {
	w.RLock()
	res := w.cutOverNanos
	w.RUnlock()
	return res
}

func (w *messageWriterImpl) SetCutoverNanos(nanos int64) {
	w.Lock()
	w.cutOverNanos = nanos
	w.Unlock()
}

func (w *messageWriterImpl) AddConsumerWriter(addr string, cw consumerWriter) {
	w.Lock()
	newConsumerWriters := make(map[string]consumerWriter, len(w.consumerWriters)+1)
	for key, cw := range w.consumerWriters {
		newConsumerWriters[key] = cw
	}
	newConsumerWriters[addr] = cw
	w.consumerWriters = newConsumerWriters
	w.Unlock()
}

func (w *messageWriterImpl) RemoveConsumerWriter(addr string) {
	w.Lock()
	newConsumerWriters := make(map[string]consumerWriter, len(w.consumerWriters)-1)
	for key, cw := range w.consumerWriters {
		if key == addr {
			continue
		}
		newConsumerWriters[key] = cw
	}
	w.consumerWriters = newConsumerWriters
	w.Unlock()
}

type acks struct {
	sync.Mutex

	m map[metadata]*message
}

// nolint: unparam
func newAckHelper(size int) *acks {
	return &acks{
		m: make(map[metadata]*message, size),
	}
}

func (h *acks) add(meta metadata, m *message) {
	h.Lock()
	h.m[meta] = m
	h.Unlock()
}

func (h *acks) remove(meta metadata) {
	h.Lock()
	delete(h.m, meta)
	h.Unlock()
}

func (h *acks) ack(meta metadata) {
	h.Lock()
	m, ok := h.m[meta]
	if !ok {
		h.Unlock()
		// Acking a message that is already acked, which is ok.
		return
	}
	delete(h.m, meta)
	h.Unlock()
	m.Ack()
}
