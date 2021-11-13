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
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/unsafe"

	"github.com/uber-go/tally"
)

var (
	errFailAllConsumers = errors.New("could not write to any consumer")
	errNoWriters        = errors.New("no writers")
)

const _recordMessageDelayEvery = 4 // keep it a power of two value to keep modulo fast

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

	// Metrics returns the metrics
	Metrics() messageWriterMetrics

	// SetMetrics sets the metrics
	//
	// This allows changing the labels of the metrics when the downstream consumer instance changes.
	SetMetrics(m messageWriterMetrics)

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
	withoutConsumerScope     bool
	scope                    tally.Scope
	opts                     instrument.TimerOptions
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
	enqueuedMessages         tally.Counter
	dequeuedMessages         tally.Counter
	processedWrite           tally.Counter
	processedClosed          tally.Counter
	processedNotReady        tally.Counter
	processedTTL             tally.Counter
	processedAck             tally.Counter
	processedDrop            tally.Counter
}

func (m messageWriterMetrics) withConsumer(consumer string) messageWriterMetrics {
	if m.withoutConsumerScope {
		return m
	}
	return newMessageWriterMetricsWithConsumer(m.scope, m.opts, consumer, false)
}

func newMessageWriterMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
	withoutConsumerScope bool,
) messageWriterMetrics {
	return newMessageWriterMetricsWithConsumer(scope, opts, "unknown", withoutConsumerScope)
}

func newMessageWriterMetricsWithConsumer(
	scope tally.Scope,
	opts instrument.TimerOptions,
	consumer string,
	withoutConsumerScope bool) messageWriterMetrics {
	consumerScope := scope
	if !withoutConsumerScope {
		consumerScope = scope.Tagged(map[string]string{"consumer": consumer})
	}
	return messageWriterMetrics{
		withoutConsumerScope:  withoutConsumerScope,
		scope:                 scope,
		opts:                  opts,
		writeSuccess:          consumerScope.Counter("write-success"),
		oneConsumerWriteError: scope.Counter("write-error-one-consumer"),
		allConsumersWriteError: consumerScope.
			Tagged(map[string]string{"error-type": "all-consumers"}).
			Counter("write-error"),
		noWritersError: consumerScope.
			Tagged(map[string]string{"error-type": "no-writers"}).
			Counter("write-error"),
		writeAfterCutoff: consumerScope.
			Tagged(map[string]string{"reason": "after-cutoff"}).
			Counter("invalid-write"),
		writeBeforeCutover: consumerScope.
			Tagged(map[string]string{"reason": "before-cutover"}).
			Counter("invalid-write"),
		messageAcked:  consumerScope.Counter("message-acked"),
		messageClosed: consumerScope.Counter("message-closed"),
		messageDroppedBufferFull: consumerScope.Tagged(
			map[string]string{"reason": "buffer-full"},
		).Counter("message-dropped"),
		messageDroppedTTLExpire: consumerScope.Tagged(
			map[string]string{"reason": "ttl-expire"},
		).Counter("message-dropped"),
		messageRetry:          consumerScope.Counter("message-retry"),
		messageConsumeLatency: instrument.NewTimer(consumerScope, "message-consume-latency", opts),
		messageWriteDelay:     instrument.NewTimer(consumerScope, "message-write-delay", opts),
		scanBatchLatency:      instrument.NewTimer(consumerScope, "scan-batch-latency", opts),
		scanTotalLatency:      instrument.NewTimer(consumerScope, "scan-total-latency", opts),
		enqueuedMessages:      consumerScope.Counter("message-enqueue"),
		dequeuedMessages:      consumerScope.Counter("message-dequeue"),
		processedWrite: consumerScope.
			Tagged(map[string]string{"result": "write"}).
			Counter("message-processed"),
		processedClosed: consumerScope.
			Tagged(map[string]string{"result": "closed"}).
			Counter("message-processed"),
		processedNotReady: consumerScope.
			Tagged(map[string]string{"result": "not-ready"}).
			Counter("message-processed"),
		processedTTL: consumerScope.
			Tagged(map[string]string{"result": "ttl"}).
			Counter("message-processed"),
		processedAck: consumerScope.
			Tagged(map[string]string{"result": "ack"}).
			Counter("message-processed"),
		processedDrop: consumerScope.
			Tagged(map[string]string{"result": "drop"}).
			Counter("message-processed"),
	}
}

type messageWriterImpl struct {
	sync.RWMutex

	replicatedShardID   uint64
	mPool               messagePool
	opts                Options
	nextRetryAfterNanos func(int) int64
	encoder             proto.Encoder
	numConnections      int

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
	// metrics can be updated when a consumer instance changes, so must be guarded with RLock
	m            *messageWriterMetrics
	nextFullScan time.Time
	lastNewWrite *list.Element

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
		replicatedShardID:   replicatedShardID,
		mPool:               mPool,
		opts:                opts,
		nextRetryAfterNanos: nextRetryNanosFn(opts.MessageRetryOptions()),
		encoder:             proto.NewEncoder(opts.EncoderOptions()),
		numConnections:      opts.ConnectionOptions().NumConnections(),
		msgID:               0,
		queue:               list.New(),
		acks:                newAckHelper(opts.InitialAckMapSize()),
		cutOffNanos:         0,
		cutOverNanos:        0,
		msgsToWrite:         make([]*message, 0, opts.MessageQueueScanBatchSize()),
		isClosed:            false,
		doneCh:              make(chan struct{}),
		m:                   &m,
		nowFn:               nowFn,
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
		metadataKey: metadataKey{
			shard: w.replicatedShardID,
			id:    w.msgID,
		},
	}
	msg.Set(meta, rm, nowNanos)
	w.acks.add(meta, msg)
	// Make sure all the new writes are ordered in queue.
	w.m.enqueuedMessages.Inc(1)
	if w.lastNewWrite != nil {
		w.lastNewWrite = w.queue.InsertAfter(msg, w.lastNewWrite)
	} else {
		w.lastNewWrite = w.queue.PushFront(msg)
	}
	w.Unlock()
}

func (w *messageWriterImpl) isValidWriteWithLock(nowNanos int64) bool {
	if w.opts.IgnoreCutoffCutover() {
		return true
	}

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
	metrics *messageWriterMetrics,
	m *message,
) error {
	m.IncReads()
	m.SetSentAt(w.nowFn().UnixNano())
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
		// NB(r): Always select the same connection index per shard.
		connIndex   = int(w.replicatedShardID % uint64(w.numConnections))
		writes      int64
		writeErrors int64
	)

	for i := len(iterationIndexes) - 1; i >= 0; i-- {
		consumerWriter := consumerWriters[randIndex(iterationIndexes, i)]
		if err := consumerWriter.Write(connIndex, w.encoder.Bytes()); err != nil {
			writeErrors++
			continue
		}
		writes++
		break
	}

	if writeErrors > 0 {
		metrics.oneConsumerWriteError.Inc(writeErrors)
	}

	if writes > 0 {
		metrics.writeSuccess.Inc(writes)
		return nil
	}
	// Could not be written to any consumer, will retry later.
	metrics.allConsumersWriteError.Inc(1)
	return errFailAllConsumers
}

func randIndex(iterationIndexes []int, i int) int {
	j := int(unsafe.Fastrandn(uint32(i + 1)))
	// NB: we should only mutate the order in the iteration indexes and
	// keep the order of consumer writers unchanged to prevent data race.
	iterationIndexes[i], iterationIndexes[j] = iterationIndexes[j], iterationIndexes[i]
	return iterationIndexes[i]
}

func (w *messageWriterImpl) Ack(meta metadata) bool {
	acked, expectedProcessNanos := w.acks.ack(meta)
	if acked {
		w.RLock()
		defer w.RUnlock()
		w.m.messageConsumeLatency.Record(time.Duration(w.nowFn().UnixNano() - expectedProcessNanos))
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
		jitter   = time.Duration(
			// approx ~40 days of jitter at millisecond precision - more than enough
			unsafe.Fastrandn(uint32(interval.Milliseconds())),
		) * time.Millisecond
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
	m := w.m
	w.RUnlock()
	var (
		nowFn            = w.nowFn
		msgsToWrite      []*message
		beforeScan       = nowFn()
		beforeBatchNanos = beforeScan.UnixNano()
		batchSize        = w.opts.MessageQueueScanBatchSize()
		consumerWriters  []consumerWriter
		iterationIndexes []int
		fullScan         = isClosed || beforeScan.After(w.nextFullScan)
		scanMetrics      scanBatchMetrics
		skipWrites       bool
	)
	defer scanMetrics.record(m)
	for e != nil {
		w.Lock()
		e, msgsToWrite = w.scanBatchWithLock(e, beforeBatchNanos, batchSize, fullScan, &scanMetrics)
		consumerWriters = w.consumerWriters
		iterationIndexes = w.iterationIndexes
		w.Unlock()
		if !fullScan && len(msgsToWrite) == 0 {
			m.scanBatchLatency.Record(time.Duration(nowFn().UnixNano() - beforeBatchNanos))
			// If this is not a full scan, abort after the iteration batch
			// that no new messages were found.
			break
		}
		if skipWrites {
			m.scanBatchLatency.Record(time.Duration(nowFn().UnixNano() - beforeBatchNanos))
			continue
		}
		if err := w.writeBatch(iterationIndexes, consumerWriters, m, msgsToWrite); err != nil {
			// When we can't write to any consumer writer, skip the writes in this scan
			// to avoid meaningless attempts but continue to clean up the queue.
			skipWrites = true
		}
		nowNanos := nowFn().UnixNano()
		m.scanBatchLatency.Record(time.Duration(nowNanos - beforeBatchNanos))
		beforeBatchNanos = nowNanos
	}
	afterScan := nowFn()
	m.scanTotalLatency.Record(afterScan.Sub(beforeScan))
	if fullScan {
		w.nextFullScan = afterScan.Add(w.opts.MessageQueueFullScanInterval())
	}
}

func (w *messageWriterImpl) writeBatch(
	iterationIndexes []int,
	consumerWriters []consumerWriter,
	metrics *messageWriterMetrics,
	messages []*message,
) error {
	if len(consumerWriters) == 0 {
		// Not expected in a healthy/valid placement.
		metrics.noWritersError.Inc(int64(len(messages)))
		return errNoWriters
	}
	delay := metrics.messageWriteDelay
	nowFn := w.nowFn
	for i := range messages {
		if err := w.write(iterationIndexes, consumerWriters, metrics, messages[i]); err != nil {
			return err
		}
		if i%_recordMessageDelayEvery == 0 {
			delay.Record(time.Duration(nowFn().UnixNano() - messages[i].ExpectedProcessAtNanos()))
		}
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
	scanMetrics *scanBatchMetrics,
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
			scanMetrics[_processedClosed]++
			// Simply ack the messages here to mark them as consumed for this
			// message writer, this is useful when user removes a consumer service
			// during runtime that may be unhealthy to consume the messages.
			// So that the unacked messages for the unhealthy consumer services
			// do not stay in memory forever.
			// NB: The message must be added to the ack map to be acked here.
			w.acks.ack(m.Metadata())
			w.removeFromQueueWithLock(e, m)
			scanMetrics[_messageClosed]++
			continue
		}
		if m.RetryAtNanos() >= nowNanos {
			scanMetrics[_processedNotReady]++
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
			scanMetrics[_processedTTL]++
			// There is a chance the message was acked right before the ack is
			// called, in which case just remove it from the queue.
			if acked, _ := w.acks.ack(m.Metadata()); acked {
				scanMetrics[_messageDroppedTTLExpire]++
			}
			w.removeFromQueueWithLock(e, m)
			continue
		}
		if m.IsAcked() {
			scanMetrics[_processedAck]++
			w.removeFromQueueWithLock(e, m)
			continue
		}
		if m.IsDroppedOrConsumed() {
			scanMetrics[_processedDrop]++
			// There is a chance the message could be acked between m.Acked()
			// and m.IsDroppedOrConsumed() check, in which case we should not
			// mark it as dropped, just continue and next tick will remove it
			// as acked.
			if m.IsAcked() {
				continue
			}
			w.acks.remove(m.Metadata())
			w.removeFromQueueWithLock(e, m)
			scanMetrics[_messageDroppedBufferFull]++
			continue
		}
		m.IncWriteTimes()
		writeTimes := m.WriteTimes()
		m.SetRetryAtNanos(w.nextRetryAfterNanos(writeTimes) + nowNanos)
		if writeTimes > 1 {
			scanMetrics[_messageRetry]++
		}
		scanMetrics[_processedWrite]++
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

func (w *messageWriterImpl) Metrics() messageWriterMetrics {
	w.RLock()
	defer w.RUnlock()
	return *w.m
}

func (w *messageWriterImpl) SetMetrics(m messageWriterMetrics) {
	w.Lock()
	w.m = &m
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
	w.m.dequeuedMessages.Inc(1)
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

	ackMap map[metadataKey]*message
}

// nolint: unparam
func newAckHelper(size int) *acks {
	return &acks{
		ackMap: make(map[metadataKey]*message, size),
	}
}

func (a *acks) add(meta metadata, m *message) {
	a.Lock()
	a.ackMap[meta.metadataKey] = m
	a.Unlock()
}

func (a *acks) remove(meta metadata) {
	a.Lock()
	delete(a.ackMap, meta.metadataKey)
	a.Unlock()
}

// ack processes the ack. returns true if the message was not already acked. additionally returns the expected
// processing time for lag calculations.
func (a *acks) ack(meta metadata) (bool, int64) {
	a.Lock()
	m, ok := a.ackMap[meta.metadataKey]
	if !ok {
		a.Unlock()
		// Acking a message that is already acked, which is ok.
		return false, 0
	}
	delete(a.ackMap, meta.metadataKey)
	a.Unlock()
	expectedProcessAtNanos := m.ExpectedProcessAtNanos()
	m.Ack()
	return true, expectedProcessAtNanos
}

func (a *acks) size() int {
	a.Lock()
	l := len(a.ackMap)
	a.Unlock()
	return l
}

type metricIdx byte

const (
	_messageClosed metricIdx = iota
	_messageDroppedBufferFull
	_messageDroppedTTLExpire
	_messageRetry
	_processedAck
	_processedClosed
	_processedDrop
	_processedNotReady
	_processedTTL
	_processedWrite
	_lastMetricIdx
)

type scanBatchMetrics [_lastMetricIdx]int32

func (m *scanBatchMetrics) record(metrics *messageWriterMetrics) {
	m.recordNonzeroCounter(_messageClosed, metrics.messageClosed)
	m.recordNonzeroCounter(_messageDroppedBufferFull, metrics.messageDroppedBufferFull)
	m.recordNonzeroCounter(_messageDroppedTTLExpire, metrics.messageDroppedTTLExpire)
	m.recordNonzeroCounter(_messageRetry, metrics.messageRetry)
	m.recordNonzeroCounter(_processedAck, metrics.processedAck)
	m.recordNonzeroCounter(_processedClosed, metrics.processedClosed)
	m.recordNonzeroCounter(_processedDrop, metrics.processedDrop)
	m.recordNonzeroCounter(_processedNotReady, metrics.processedNotReady)
	m.recordNonzeroCounter(_processedTTL, metrics.processedTTL)
	m.recordNonzeroCounter(_processedWrite, metrics.processedWrite)
}

func (m *scanBatchMetrics) recordNonzeroCounter(idx metricIdx, c tally.Counter) {
	if m[idx] > 0 {
		c.Inc(int64(m[idx]))
	}
}

func nextRetryNanosFn(retryOpts retry.Options) func(int) int64 {
	var (
		jitter              = retryOpts.Jitter()
		backoffFactor       = retryOpts.BackoffFactor()
		initialBackoff      = retryOpts.InitialBackoff()
		maxBackoff          = retryOpts.MaxBackoff()
		initialBackoffFloat = float64(initialBackoff)
	)

	// inlined and specialized retry function that does not have any state that needs to be kept
	// between tries
	return func(writeTimes int) int64 {
		backoff := initialBackoff.Nanoseconds()
		if writeTimes >= 1 {
			backoffFloat64 := initialBackoffFloat * math.Pow(backoffFactor, float64(writeTimes-1))
			backoff = int64(backoffFloat64)
		}
		// Validate the value of backoff to make sure Fastrandn() does not panic and
		// check for overflow from the exponentiation op - unlikely, but prevents weird side effects.
		halfInMicros := (backoff / 2) / int64(time.Microsecond)
		if jitter && backoff >= 2 && halfInMicros < math.MaxUint32 {
			// Jitter can be only up to ~1 hour in microseconds, but it's not a limitation here
			jitterInMicros := unsafe.Fastrandn(uint32(halfInMicros))
			jitterInNanos := time.Duration(jitterInMicros) * time.Microsecond
			halfInNanos := time.Duration(halfInMicros) * time.Microsecond
			backoff = int64(halfInNanos + jitterInNanos)
		}
		// Clamp backoff to maxBackoff
		if maxBackoff := maxBackoff.Nanoseconds(); backoff > maxBackoff {
			backoff = maxBackoff
		}
		return backoff
	}
}
