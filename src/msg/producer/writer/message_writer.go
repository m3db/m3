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
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

var (
	errFailAllConsumers = errors.New("could not write to any consumer")
	errNoWriters        = errors.New("no writers")
)

type messageWriter interface {
	// Write writes the message.
	Write(rm *producer.RefCountedMessage)

	// Ack acknowledges the metadata.
	Ack(meta metadata)

	// Init initialize the message writer.
	Init()

	// Close closes the writer.
	// It should block until all buffered messages have been acknowledged.
	Close()

	// AddConsumerWriter adds a consumer writer.
	AddConsumerWriter(cw consumerWriter)

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

	// QueueSize returns the number of messages queued in the writer.
	QueueSize() int
}

type messageWriterMetrics struct {
	writeSuccess           tally.Counter
	oneConsumerWriteError  tally.Counter
	allConsumersWriteError tally.Counter
	noWritersError         tally.Counter
	writeAfterCutoff       tally.Counter
	writeBeforeCutover     tally.Counter
	messageAcked           tally.Counter
	messageClosed          tally.Counter
	messageDropped         tally.Counter
	retryBatchLatency      tally.Timer
	retryTotalLatency      tally.Timer
}

func newMessageWriterMetrics(
	scope tally.Scope,
	samplingRate float64,
) messageWriterMetrics {
	return messageWriterMetrics{
		writeSuccess:          scope.Counter("write-success"),
		oneConsumerWriteError: scope.Counter("write-error-one-consumer"),
		allConsumersWriteError: scope.
			Tagged(map[string]string{"error-type": "all-consumers"}).
			Counter("write-error"),
		noWritersError: scope.
			Tagged(map[string]string{"error-type": "no-writers"}).
			Counter("write-error"),
		writeAfterCutoff: scope.
			Tagged(map[string]string{"reason": "after-cutoff"}).
			Counter("invalid-write"),
		writeBeforeCutover: scope.
			Tagged(map[string]string{"reason": "before-cutover"}).
			Counter("invalid-write"),
		messageAcked:      scope.Counter("message-acked"),
		messageClosed:     scope.Counter("message-closed"),
		messageDropped:    scope.Counter("message-dropped"),
		retryBatchLatency: instrument.MustCreateSampledTimer(scope.Timer("retry-batch-latency"), samplingRate),
		retryTotalLatency: instrument.MustCreateSampledTimer(scope.Timer("retry-total-latency"), samplingRate),
	}
}

type messageWriterImpl struct {
	sync.RWMutex

	replicatedShardID uint64
	mPool             messagePool
	opts              Options
	retryOpts         retry.Options
	r                 *rand.Rand

	msgID           uint64
	queue           *list.List
	consumerWriters []consumerWriter
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
	m messageWriterMetrics,
) messageWriter {
	if opts == nil {
		opts = NewOptions()
	}
	return &messageWriterImpl{
		replicatedShardID: replicatedShardID,
		mPool:             mPool,
		opts:              opts,
		retryOpts:         opts.MessageRetryOptions(),
		r:                 rand.New(rand.NewSource(time.Now().UnixNano())),
		msgID:             0,
		queue:             list.New(),
		acks:              newAckHelper(opts.InitialAckMapSize()),
		cutOffNanos:       0,
		cutOverNanos:      0,
		toBeRetried:       make([]*message, 0, opts.MessageRetryBatchSize()),
		isClosed:          false,
		doneCh:            make(chan struct{}),
		m:                 m,
		nowFn:             time.Now,
	}
}

func (w *messageWriterImpl) Write(rm *producer.RefCountedMessage) {
	var (
		nowNanos = w.nowFn().UnixNano()
		msg      = w.newMessage()
	)
	w.Lock()
	if !w.isValidWriteWithLock(nowNanos) {
		w.Unlock()
		w.close(msg)
		return
	}
	rm.IncRef()
	w.msgID++
	meta := metadata{
		shard: w.replicatedShardID,
		id:    w.msgID,
	}
	msg.Set(meta, rm)
	w.acks.add(meta, msg)
	w.queue.PushBack(msg)
	w.Unlock()
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
	consumerWriters []consumerWriter,
	m *message,
) error {
	m.IncReads()
	msg, isValid := m.Marshaler()
	if !isValid {
		m.DecReads()
		return nil
	}
	var (
		written  = false
		l        = len(consumerWriters)
		nowNanos = w.nowFn().UnixNano()
		start    = int(nowNanos) % l
	)
	for i := start; i < start+l; i++ {
		idx := i % l
		if err := consumerWriters[idx].Write(msg); err != nil {
			w.m.oneConsumerWriteError.Inc(1)
			continue
		}
		written = true
		w.m.writeSuccess.Inc(1)
		break
	}
	m.DecReads()
	if written {
		return nil
	}
	// Could not be written to any consumer, will retry later.
	w.m.allConsumersWriteError.Inc(1)
	return errFailAllConsumers
}

func (w *messageWriterImpl) nextRetryNanos(writeTimes int, nowNanos int64) int64 {
	backoff := retry.BackoffNanos(
		writeTimes,
		w.retryOpts.Jitter(),
		w.retryOpts.BackoffFactor(),
		w.retryOpts.InitialBackoff(),
		w.retryOpts.MaxBackoff(),
		w.r.Int63n,
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
	var (
		interval = w.opts.MessageQueueScanInterval()
		jitter   = time.Duration(rand.Int63n(int64(interval)))
	)
	// NB(cw): Add some jitter before the tick starts to reduce
	// some contention between all the message writers.
	time.Sleep(jitter)
	ticker := time.NewTicker(interval)
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
	e := w.queue.Front()
	w.RUnlock()
	var (
		toBeRetried []*message
		beforeRetry = w.nowFn()
		batchSize   = w.opts.MessageRetryBatchSize()
	)
	for e != nil {
		beforeBatch := w.nowFn()
		beforeBatchNanos := beforeBatch.UnixNano()
		w.Lock()
		e, toBeRetried = w.retryBatchWithLock(e, beforeBatchNanos, batchSize)
		consumerWriters := w.consumerWriters
		w.Unlock()
		err := w.writeBatch(consumerWriters, toBeRetried)
		w.m.retryBatchLatency.Record(w.nowFn().Sub(beforeBatch))
		if err != nil {
			// When we can't write to any consumer writer, skip the tick
			// to avoid meaningless attempts, wait for next tick to retry.
			break
		}
	}
	w.m.retryTotalLatency.Record(w.nowFn().Sub(beforeRetry))
}

func (w *messageWriterImpl) writeBatch(
	consumerWriters []consumerWriter,
	toBeRetried []*message,
) error {
	if len(consumerWriters) == 0 {
		// Not expected in a healthy/valid placement.
		w.m.noWritersError.Inc(int64(len(toBeRetried)))
		return errNoWriters
	}
	for _, m := range toBeRetried {
		if err := w.write(consumerWriters, m); err != nil {
			return err
		}
	}
	return nil
}

// retryBatchWithLock iterates the message queue with a lock.
// It returns after visited enough items or the first item
// to retry so it holds the lock for less time and allows new writes
// to be less blocked, so that one slow message writer does not
// slow down other message writers too much.
func (w *messageWriterImpl) retryBatchWithLock(
	start *list.Element,
	nowNanos int64,
	batchSize int,
) (*list.Element, []*message) {
	var (
		iterated int
		next     *list.Element
	)
	w.toBeRetried = w.toBeRetried[:0]
	for e := start; e != nil; e = next {
		iterated++
		if iterated > batchSize {
			break
		}
		next = e.Next()
		m := e.Value.(*message)
		if w.isClosed {
			// Simply ack the messages here to mark them as consumed for this
			// message writer, this is useful when user removes a consumer service
			// during runtime that may be unhealthy to consume the messages.
			// So that the unacked messages for the unhealthy consumer services
			// do not stay in memory forever.
			// NB: The message must be added to the ack map to be acked here.
			w.Ack(m.Metadata())
			w.removeFromQueueWithLock(e, m)
			w.m.messageClosed.Inc(1)
			continue
		}
		if m.RetryAtNanos() >= nowNanos {
			continue
		}
		if m.IsAcked() {
			w.removeFromQueueWithLock(e, m)
			w.m.messageAcked.Inc(1)
			continue
		}
		if m.IsDroppedOrConsumed() {
			// There is a chance the message could be acked between m.Acked()
			// and m.IsDroppedOrConsumed() check, in which case we should not
			// mark it as dropped, just continue and next tick will remove it
			// as acked.
			if m.IsAcked() {
				continue
			}
			w.acks.remove(m.Metadata())
			w.removeFromQueueWithLock(e, m)
			w.m.messageDropped.Inc(1)
			continue
		}
		m.IncWriteTimes()
		m.SetRetryAtNanos(w.nextRetryNanos(m.WriteTimes(), nowNanos))
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

func (w *messageWriterImpl) AddConsumerWriter(cw consumerWriter) {
	w.Lock()
	newConsumerWriters := make([]consumerWriter, 0, len(w.consumerWriters)+1)
	newConsumerWriters = append(newConsumerWriters, w.consumerWriters...)
	newConsumerWriters = append(newConsumerWriters, cw)
	w.consumerWriters = newConsumerWriters
	w.Unlock()
}

func (w *messageWriterImpl) RemoveConsumerWriter(addr string) {
	w.Lock()
	newConsumerWriters := make([]consumerWriter, 0, len(w.consumerWriters)-1)
	for _, cw := range w.consumerWriters {
		if cw.Address() == addr {
			continue
		}
		newConsumerWriters = append(newConsumerWriters, cw)
	}
	w.consumerWriters = newConsumerWriters
	w.Unlock()
}

func (w *messageWriterImpl) QueueSize() int {
	return w.acks.size()
}

func (w *messageWriterImpl) newMessage() *message {
	if w.mPool != nil {
		return w.mPool.Get()
	}
	return newMessage()
}

func (w *messageWriterImpl) removeFromQueueWithLock(e *list.Element, m *message) {
	w.queue.Remove(e)
	w.close(m)
}

func (w *messageWriterImpl) close(m *message) {
	if w.mPool != nil {
		m.Close()
		w.mPool.Put(m)
	}
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

func (a *acks) add(meta metadata, m *message) {
	a.Lock()
	a.m[meta] = m
	a.Unlock()
}

func (a *acks) remove(meta metadata) {
	a.Lock()
	delete(a.m, meta)
	a.Unlock()
}

func (a *acks) ack(meta metadata) {
	a.Lock()
	m, ok := a.m[meta]
	if !ok {
		a.Unlock()
		// Acking a message that is already acked, which is ok.
		return
	}
	delete(a.m, meta)
	a.Unlock()
	m.Ack()
}

func (a *acks) size() int {
	a.Lock()
	l := len(a.m)
	a.Unlock()
	return l
}
