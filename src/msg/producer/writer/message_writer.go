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
	"github.com/m3db/m3msg/protocol/proto"
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
	// Write writes a message, messages not acknowledged in time will be retried.
	// New messages will be written in order, but retries could be out of order.
	Write(rm *producer.RefCountedMessage)

	// Ack acknowledges the metadata.
	Ack(meta metadata) bool

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

	// MessageTTLNanos returns the message ttl nanoseconds.
	MessageTTLNanos() int64

	// SetMessageTTLNanos sets the message ttl nanoseconds.
	SetMessageTTLNanos(nanos int64)

	// QueueSize returns the number of messages queued in the writer.
	QueueSize() int
}

type messageWriterMetrics struct {
	writeSuccess             tally.Counter
	oneConsumerWriteError    tally.Counter
	allConsumersWriteError   tally.Counter
	noWritersError           tally.Counter
	writeAfterCutoff         tally.Counter
	writeBeforeCutover       tally.Counter
	messageAcked             tally.Counter
	messageClosed            tally.Counter
	messageDroppedBufferFull tally.Counter
	messageDroppedTTLExpire  tally.Counter
	messageRetry             tally.Counter
	messageConsumeLatency    tally.Timer
	messageWriteDelay        tally.Timer
	scanBatchLatency         tally.Timer
	scanTotalLatency         tally.Timer
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
		messageAcked:  scope.Counter("message-acked"),
		messageClosed: scope.Counter("message-closed"),
		messageDroppedBufferFull: scope.Tagged(
			map[string]string{"reason": "buffer-full"},
		).Counter("message-dropped"),
		messageDroppedTTLExpire: scope.Tagged(
			map[string]string{"reason": "ttl-expire"},
		).Counter("message-dropped"),
		messageRetry:          scope.Counter("message-retry"),
		messageConsumeLatency: instrument.MustCreateSampledTimer(scope.Timer("message-consume-latency"), samplingRate),
		messageWriteDelay:     instrument.MustCreateSampledTimer(scope.Timer("message-write-delay"), samplingRate),
		scanBatchLatency:      instrument.MustCreateSampledTimer(scope.Timer("scan-batch-latency"), samplingRate),
		scanTotalLatency:      instrument.MustCreateSampledTimer(scope.Timer("scan-total-latency"), samplingRate),
	}
}

type messageWriterImpl struct {
	sync.RWMutex

	replicatedShardID uint64
	mPool             messagePool
	opts              Options
	retryOpts         retry.Options
	r                 *rand.Rand
	encoder           proto.Encoder

	msgID            uint64
	queue            *list.List
	consumerWriters  []consumerWriter
	iterationIndexes []int
	acks             *acks
	cutOffNanos      int64
	cutOverNanos     int64
	messageTTLNanos  int64
	msgsToWrite      []*message
	isClosed         bool
	doneCh           chan struct{}
	wg               sync.WaitGroup
	m                messageWriterMetrics
	nextFullScan     time.Time
	lastNewWrite     *list.Element

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
	nowFn := time.Now
	return &messageWriterImpl{
		replicatedShardID: replicatedShardID,
		mPool:             mPool,
		opts:              opts,
		retryOpts:         opts.MessageRetryOptions(),
		r:                 rand.New(rand.NewSource(nowFn().UnixNano())),
		encoder:           proto.NewEncoder(opts.EncoderOptions()),
		msgID:             0,
		queue:             list.New(),
		acks:              newAckHelper(opts.InitialAckMapSize()),
		cutOffNanos:       0,
		cutOverNanos:      0,
		msgsToWrite:       make([]*message, 0, opts.MessageQueueScanBatchSize()),
		isClosed:          false,
		doneCh:            make(chan struct{}),
		m:                 m,
		nowFn:             nowFn,
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
	msg.Set(meta, rm, nowNanos)
	w.acks.add(meta, msg)
	// Make sure all the new writes are ordered in queue.
	if w.lastNewWrite != nil {
		w.lastNewWrite = w.queue.InsertAfter(msg, w.lastNewWrite)
	} else {
		w.lastNewWrite = w.queue.PushFront(msg)
	}
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
	iterationIndexes []int,
	consumerWriters []consumerWriter,
	m *message,
) error {
	m.IncReads()
	msg, isValid := m.Marshaler()
	if !isValid {
		m.DecReads()
		return nil
	}
	// The write function is accessed through only one thread,
	// so no lock is required for encoding.
	err := w.encoder.Encode(msg)
	m.DecReads()
	if err != nil {
		return err
	}
	var (
		written = false
	)
	for i := len(iterationIndexes) - 1; i >= 0; i-- {
		if err := consumerWriters[randIndex(iterationIndexes, i)].Write(w.encoder.Bytes()); err != nil {
			w.m.oneConsumerWriteError.Inc(1)
			continue
		}
		written = true
		w.m.writeSuccess.Inc(1)
		break
	}
	if written {
		return nil
	}
	// Could not be written to any consumer, will retry later.
	w.m.allConsumersWriteError.Inc(1)
	return errFailAllConsumers
}

func randIndex(iterationIndexes []int, i int) int {
	j := rand.Intn(i + 1)
	// NB: we should only mutate the order in the iteration indexes and
	// keep the order of consumer writers unchanged to prevent data race.
	iterationIndexes[i], iterationIndexes[j] = iterationIndexes[j], iterationIndexes[i]
	return iterationIndexes[i]
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

func (w *messageWriterImpl) Ack(meta metadata) bool {
	acked, initNanos := w.acks.ack(meta)
	if acked {
		w.m.messageConsumeLatency.Record(time.Duration(w.nowFn().UnixNano() - initNanos))
		w.m.messageAcked.Inc(1)
		return true
	}
	return false
}

func (w *messageWriterImpl) Init() {
	w.wg.Add(1)
	go func() {
		w.scanMessageQueueUntilClose()
		w.wg.Done()
	}()
}

func (w *messageWriterImpl) scanMessageQueueUntilClose() {
	var (
		interval = w.opts.MessageQueueNewWritesScanInterval()
		jitter   = time.Duration(w.r.Int63n(int64(interval)))
	)
	// NB(cw): Add some jitter before the tick starts to reduce
	// some contention between all the message writers.
	time.Sleep(jitter)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.scanMessageQueue()
		case <-w.doneCh:
			return
		}
	}
}

func (w *messageWriterImpl) scanMessageQueue() {
	w.RLock()
	e := w.queue.Front()
	w.lastNewWrite = nil
	isClosed := w.isClosed
	w.RUnlock()
	var (
		msgsToWrite      []*message
		beforeScan       = w.nowFn()
		batchSize        = w.opts.MessageQueueScanBatchSize()
		consumerWriters  []consumerWriter
		iterationIndexes []int
		fullScan         = isClosed || beforeScan.After(w.nextFullScan)
		skipWrites       bool
	)
	for e != nil {
		beforeBatch := w.nowFn()
		beforeBatchNanos := beforeBatch.UnixNano()
		w.Lock()
		e, msgsToWrite = w.scanBatchWithLock(e, beforeBatchNanos, batchSize, fullScan)
		consumerWriters = w.consumerWriters
		iterationIndexes = w.iterationIndexes
		w.Unlock()
		if !fullScan && len(msgsToWrite) == 0 {
			w.m.scanBatchLatency.Record(w.nowFn().Sub(beforeBatch))
			// If this is not a full scan, abort after the iteration batch
			// that no new messages were found.
			break
		}
		if skipWrites {
			w.m.scanBatchLatency.Record(w.nowFn().Sub(beforeBatch))
			continue
		}
		if err := w.writeBatch(iterationIndexes, consumerWriters, msgsToWrite); err != nil {
			// When we can't write to any consumer writer, skip the writes in this scan
			// to avoid meaningless attempts but continue to clean up the queue.
			skipWrites = true
		}
		w.m.scanBatchLatency.Record(w.nowFn().Sub(beforeBatch))
	}
	afterScan := w.nowFn()
	w.m.scanTotalLatency.Record(afterScan.Sub(beforeScan))
	if fullScan {
		w.nextFullScan = afterScan.Add(w.opts.MessageQueueFullScanInterval())
	}
}

func (w *messageWriterImpl) writeBatch(
	iterationIndexes []int,
	consumerWriters []consumerWriter,
	toBeRetried []*message,
) error {
	if len(consumerWriters) == 0 {
		// Not expected in a healthy/valid placement.
		w.m.noWritersError.Inc(int64(len(toBeRetried)))
		return errNoWriters
	}
	for _, m := range toBeRetried {
		if err := w.write(iterationIndexes, consumerWriters, m); err != nil {
			return err
		}
		w.m.messageWriteDelay.Record(time.Duration(w.nowFn().UnixNano() - m.InitNanos()))
	}
	return nil
}

// scanBatchWithLock iterates the message queue with a lock. It returns after
// visited enough elements. So it holds the lock for less time and allows new
// writes to be unblocked.
func (w *messageWriterImpl) scanBatchWithLock(
	start *list.Element,
	nowNanos int64,
	batchSize int,
	fullScan bool,
) (*list.Element, []*message) {
	var (
		iterated int
		next     *list.Element
	)
	w.msgsToWrite = w.msgsToWrite[:0]
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
			w.acks.ack(m.Metadata())
			w.removeFromQueueWithLock(e, m)
			w.m.messageClosed.Inc(1)
			continue
		}
		if m.RetryAtNanos() >= nowNanos {
			if !fullScan {
				// If this is not a full scan, bail after the first element that
				// is not a new write.
				break
			}
			continue
		}
		// If the message exceeded its allowed ttl of the consumer service,
		// remove it from the buffer.
		if w.messageTTLNanos > 0 && m.InitNanos()+w.messageTTLNanos <= nowNanos {
			// There is a chance the message was acked right before the ack is
			// called, in which case just remove it from the queue.
			if acked, _ := w.acks.ack(m.Metadata()); acked {
				w.m.messageDroppedTTLExpire.Inc(1)
			}
			w.removeFromQueueWithLock(e, m)
			continue
		}
		if m.IsAcked() {
			w.removeFromQueueWithLock(e, m)
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
			w.m.messageDroppedBufferFull.Inc(1)
			continue
		}
		m.IncWriteTimes()
		writeTimes := m.WriteTimes()
		m.SetRetryAtNanos(w.nextRetryNanos(writeTimes, nowNanos))
		if writeTimes > 1 {
			w.m.messageRetry.Inc(1)
		}
		w.msgsToWrite = append(w.msgsToWrite, m)
	}
	return next, w.msgsToWrite
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

func (w *messageWriterImpl) MessageTTLNanos() int64 {
	w.RLock()
	res := w.messageTTLNanos
	w.RUnlock()
	return res
}

func (w *messageWriterImpl) SetMessageTTLNanos(nanos int64) {
	w.Lock()
	w.messageTTLNanos = nanos
	w.Unlock()
}

func (w *messageWriterImpl) AddConsumerWriter(cw consumerWriter) {
	w.Lock()
	newConsumerWriters := make([]consumerWriter, 0, len(w.consumerWriters)+1)
	newConsumerWriters = append(newConsumerWriters, w.consumerWriters...)
	newConsumerWriters = append(newConsumerWriters, cw)

	w.iterationIndexes = make([]int, len(newConsumerWriters))
	for i := range w.iterationIndexes {
		w.iterationIndexes[i] = i
	}
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

	w.iterationIndexes = make([]int, len(newConsumerWriters))
	for i := range w.iterationIndexes {
		w.iterationIndexes[i] = i
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

	ackMap map[metadata]*message
}

// nolint: unparam
func newAckHelper(size int) *acks {
	return &acks{
		ackMap: make(map[metadata]*message, size),
	}
}

func (a *acks) add(meta metadata, m *message) {
	a.Lock()
	a.ackMap[meta] = m
	a.Unlock()
}

func (a *acks) remove(meta metadata) {
	a.Lock()
	delete(a.ackMap, meta)
	a.Unlock()
}

func (a *acks) ack(meta metadata) (bool, int64) {
	a.Lock()
	m, ok := a.ackMap[meta]
	if !ok {
		a.Unlock()
		// Acking a message that is already acked, which is ok.
		return false, 0
	}
	delete(a.ackMap, meta)
	a.Unlock()
	initNanos := m.InitNanos()
	m.Ack()
	return true, initNanos
}

func (a *acks) size() int {
	a.Lock()
	l := len(a.ackMap)
	a.Unlock()
	return l
}
