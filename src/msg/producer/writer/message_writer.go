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
	"errors"
	"math"
	"sync"
	"time"
	stdunsafe "unsafe"

	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/unsafe"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

// MessageRetryNanosFn returns the message backoff time for retry in nanoseconds.
type MessageRetryNanosFn func(writeTimes int) int64

var (
	errInvalidBackoffDuration = errors.New("invalid backoff duration")
	errFailAllConsumers       = errors.New("could not write to any consumer")
	errNoWriters              = errors.New("no writers")
)

const (
	_recordMessageDelayEvery  = 4   // keep it a power of two value to keep modulo fast
	_resizeBuffersProbability = 250 // 1/250th chance
)

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

func (m *messageWriterMetrics) withConsumer(consumer string) *messageWriterMetrics {
	if m.withoutConsumerScope {
		return m
	}
	return newMessageWriterMetricsWithConsumer(m.scope, m.opts, consumer, false)
}

func newMessageWriterMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
	withoutConsumerScope bool,
) *messageWriterMetrics {
	return newMessageWriterMetricsWithConsumer(scope, opts, "unknown", withoutConsumerScope)
}

func newMessageWriterMetricsWithConsumer(
	scope tally.Scope,
	opts instrument.TimerOptions,
	consumer string,
	withoutConsumerScope bool,
) *messageWriterMetrics {
	consumerScope := scope
	if !withoutConsumerScope {
		consumerScope = scope.Tagged(map[string]string{"consumer": consumer})
	}
	return &messageWriterMetrics{
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

type messageWriter struct {
	sync.RWMutex

	replicatedShardID   uint64
	mPool               *messagePool
	opts                Options
	nextRetryAfterNanos MessageRetryNanosFn
	encoder             proto.Encoder
	numConnections      int
	msgID               uint64
	consumerWriters     []consumerWriter
	iterationIndexes    []int
	acks                *acks
	cutOffNanos         int64
	cutOverNanos        int64
	messageTTLNanos     int64
	isClosed            atomic.Bool
	doneCh              chan struct{}
	wg                  sync.WaitGroup
	// metrics can be updated when a consumer instance changes, so must be guarded with RLock
	metrics        atomic.UnsafePointer //  *messageWriterMetrics
	batchBuf       []*message
	scanBuf        []*message
	scanMtx        sync.RWMutex // scanMtx is held during any queue scan, but not acquired in Write()
	queueMtx       sync.Mutex   // queueMtx applies only to new writes to incomingMsgs
	incomingMsgs   []*message
	processingMsgs []*message
	nowFn          clock.NowFn
	ignoreCutoffs  bool
}

func newMessageWriter(
	replicatedShardID uint64,
	mPool *messagePool,
	opts Options,
	m *messageWriterMetrics,
) *messageWriter {
	if opts == nil {
		opts = NewOptions()
	}
	nowFn := time.Now
	mw := &messageWriter{
		replicatedShardID:   replicatedShardID,
		mPool:               mPool,
		opts:                opts,
		nextRetryAfterNanos: opts.MessageRetryNanosFn(),
		encoder:             proto.NewEncoder(opts.EncoderOptions()),
		numConnections:      opts.ConnectionOptions().NumConnections(),
		acks:                newAckHelper(opts.InitialAckMapSize()),
		batchBuf:            make([]*message, 0, opts.MessageQueueScanBatchSize()),
		doneCh:              make(chan struct{}),
		nowFn:               nowFn,
		ignoreCutoffs:       opts.IgnoreCutoffCutover(),
	}
	mw.metrics.Store(stdunsafe.Pointer(m))
	return mw
}

// Write writes a message, messages not acknowledged in time will be retried.
// New messages will be written in order, but retries could be out of order.
func (w *messageWriter) Write(rm *producer.RefCountedMessage) {
	var (
		nowNanos = w.nowFn().UnixNano()
		msg      = w.newMessage()
		metrics  = w.Metrics()
	)
	w.Lock()
	if !w.isValidWriteWithLock(nowNanos, metrics) {
		w.Unlock()
		w.close(msg)
		return
	}

	rm.IncRef()
	w.msgID++
	msgID := w.msgID
	w.Unlock()

	meta := metadata{
		metadataKey: metadataKey{
			shard: w.replicatedShardID,
			id:    msgID,
		},
	}
	msg.Set(meta, rm, nowNanos)
	w.acks.add(meta, msg)

	w.queueMtx.Lock()
	w.incomingMsgs = append(w.incomingMsgs, msg)
	w.queueMtx.Unlock()

	metrics.enqueuedMessages.Inc(1)
}

func (w *messageWriter) isValidWriteWithLock(nowNanos int64, metrics *messageWriterMetrics) bool {
	if w.ignoreCutoffs {
		return true
	}

	if w.cutOffNanos > 0 && nowNanos >= w.cutOffNanos {
		metrics.writeAfterCutoff.Inc(1)
		return false
	}
	if w.cutOverNanos > 0 && nowNanos < w.cutOverNanos {
		metrics.writeBeforeCutover.Inc(1)
		return false
	}

	return true
}

func (w *messageWriter) write(
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

// Ack acknowledges the metadata.
func (w *messageWriter) Ack(meta metadata) bool {
	if acked, expectedProcessNanos := w.acks.ack(meta); acked {
		m := w.Metrics()
		m.messageConsumeLatency.Record(time.Duration(w.nowFn().UnixNano() - expectedProcessNanos))
		m.messageAcked.Inc(1)
		return true
	}
	return false
}

// Init initialize the message writer.
func (w *messageWriter) Init() {
	w.wg.Add(1)
	go func() {
		w.scanMessageQueueUntilClose()
		w.wg.Done()
	}()
}

func (w *messageWriter) scanMessageQueueUntilClose() {
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

	var nextFullScan = w.nowFn().Add(w.opts.MessageQueueFullScanInterval())
	for {
		select {
		case <-ticker.C:
			var fullScan = w.isClosed.Load() || w.nowFn().After(nextFullScan)
			w.scanMessageQueue(fullScan)
			if fullScan {
				nextFullScan = w.nowFn().Add(w.opts.MessageQueueFullScanInterval())
			}
		case <-w.doneCh:
			return
		}
	}
}

// scanMessageQueue is part of core processing loop and is called from the main ticker at NewWritesScanInterval.
//
// New write scans only consider incoming messages since last cycle, and does not perform retries on
// previously scanned messages.
// If the cycle is not a full scan:
// - incomingMsgs are copied into a buffer and incomingMsgs queue is reset, releasing the lock for new writes
// - only messages that came in to incomingMsgs queue since last NewWritesScanInterval are considered.
// - all valid messages are moved into `processingMsgs` queue and `incomingMsg` queue is reset
// If there's enough time passed since last full scan, the iteration will consider all messages instead.
// On full scan:
// - both incomingMsgs and processingMsgs are copied into a single buffer, and new writes are unblocked
// - messages that are still pending (nextRetry > now) will be skipped
// - messages that are due for a retry (nextRetry < now) will be retried
// - messages that are dropped, acked or
// - all remaining messages are copied back again into processingMsgs queue
// In both cases, all valid (not dropped or expired between enqueue and scan) messages
// are moved to `processingMsgs` queue.

// This ensures write semantics are FIFO (except for retries), and that new messages are processed
// without blocking new writes.
// If the messages are dropped from the queue in Buffer, they'll be skipped over
// and not written back into processingMsgs queue.
func (w *messageWriter) scanMessageQueue(fullScan bool) {
	w.scanMtx.Lock()
	defer w.scanMtx.Unlock()

	var realloc = unsafe.Fastrandn(_resizeBuffersProbability) == 0

	w.queueMtx.Lock()
	// copy new messages into scratch buffer
	w.scanBuf = append(w.scanBuf, w.incomingMsgs...)

	if realloc {
		w.incomingMsgs = make([]*message, 0, cap(w.incomingMsgs)/2)
	} else {
		w.incomingMsgs = zeroMsgBuf(w.incomingMsgs)
	}
	w.queueMtx.Unlock()

	if fullScan {
		w.scanBuf = append(w.scanBuf, w.processingMsgs...)
		w.processingMsgs = zeroMsgBuf(w.processingMsgs)

		w.scanMessageQueueInner(w.scanBuf)
		if realloc {
			// shrink all scratch buffers as well
			w.processingMsgs = make([]*message, 0, cap(w.processingMsgs)/2)
			w.scanBuf = make([]*message, 0, cap(w.scanBuf)/2)
		}
		return
	}

	w.scanMessageQueueInner(w.scanBuf)
	w.scanBuf = zeroMsgBuf(w.scanBuf)
}

func (w *messageWriter) scanMessageQueueInner(queue []*message) {
	var (
		nowFn            = w.nowFn
		beforeBatchNanos = nowFn().UnixNano()
		batchSize        = w.opts.MessageQueueScanBatchSize()
		toWrite          = batchSize
		metrics          = w.Metrics()
		consumerWriters  []consumerWriter
		iterationIndexes []int
		scanMetrics      scanBatchMetrics
		skipWrites       bool
	)

	defer scanMetrics.record(metrics)
	defer metrics.scanTotalLatency.Start().Stop()

	for len(queue) > 0 {
		if toWrite > len(queue) {
			toWrite = len(queue)
		}

		batch := w.scanBatch(queue[:toWrite], beforeBatchNanos, metrics, &scanMetrics)
		if len(batch) == 0 || skipWrites {
			// nothing to write, go straight to cleanup
			goto next
		}

		w.RLock()
		consumerWriters = w.consumerWriters
		iterationIndexes = w.iterationIndexes
		w.RUnlock()

		if err := w.writeBatch(iterationIndexes, consumerWriters, metrics, batch); err != nil {
			// When we can't write to any consumer writer, skip the writes in this scan
			// to avoid meaningless attempts but continue to clean up the queue.
			skipWrites = true
		}
		metrics.scanBatchLatency.Record(time.Duration(nowFn().UnixNano() - beforeBatchNanos))

	next:
		beforeBatchNanos = nowFn().UnixNano()
		queue = queue[toWrite:]
	}
}

func (w *messageWriter) writeBatch(
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

func (w *messageWriter) scanBatch(
	queue []*message,
	nowNanos int64,
	metrics *messageWriterMetrics,
	scanMetrics *scanBatchMetrics,
) []*message {
	var (
		// check these values only once per batch
		ttlNanos = w.MessageTTLNanos()
		closed   = w.isClosed.Load()
		dequed   int
	)

	w.batchBuf = w.batchBuf[:0]
	for _, m := range queue {
		if closed {
			scanMetrics[_processedClosed]++
			// Simply ack the messages here to mark them as consumed for this
			// message writer, this is useful when user removes a consumer service
			// during runtime that may be unhealthy to consume the messages.
			// So that the unacked messages for the unhealthy consumer services
			// do not stay in memory forever.
			// NB: The message must be added to the ack map to be acked here.
			w.acks.ack(m.Metadata())
			w.close(m)
			scanMetrics[_messageClosed]++
			continue
		}

		if m.RetryAtNanos() >= nowNanos {
			w.processingMsgs = append(w.processingMsgs, m)
			scanMetrics[_processedNotReady]++
			continue
		}
		// If the message exceeded its allowed ttl of the consumer service,
		// remove it from the buffer.
		if ttlNanos > 0 && m.InitNanos()+ttlNanos <= nowNanos {
			scanMetrics[_processedTTL]++
			// There is a chance the message was acked right before the ack is
			// called, in which case just remove it from the queue.
			// NB: message must always be acked before closing.
			if acked, _ := w.acks.ack(m.Metadata()); acked {
				scanMetrics[_messageDroppedTTLExpire]++
			}
			w.close(m)
			dequed++
			continue
		}

		if m.IsAcked() {
			w.close(m)
			dequed++
			scanMetrics[_processedAck]++
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
			w.close(m)
			dequed++
			scanMetrics[_messageDroppedBufferFull]++
			continue
		}

		writeTimes := m.IncWriteTimes()
		m.SetRetryAtNanos(w.nextRetryAfterNanos(writeTimes) + nowNanos)
		if writeTimes > 1 {
			scanMetrics[_messageRetry]++
		}
		scanMetrics[_processedWrite]++

		w.batchBuf = append(w.batchBuf, m)
		w.processingMsgs = append(w.processingMsgs, m)
	}

	if dequed > 0 {
		metrics.dequeuedMessages.Inc(int64(dequed))
	}

	return w.batchBuf
}

// Close closes the writer.
// It should block until all buffered messages have been acknowledged.
func (w *messageWriter) Close() {
	if !w.isClosed.CAS(false, true) {
		return
	}
	// NB: Wait until all messages cleaned up then close.
	w.waitUntilAllMessageRemoved()
	close(w.doneCh)
	w.wg.Wait()
}

func (w *messageWriter) waitUntilAllMessageRemoved() {
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

func (w *messageWriter) isEmpty() bool {
	return w.QueueSize() == 0
}

// ReplicatedShardID returns the replicated shard id.
func (w *messageWriter) ReplicatedShardID() uint64 {
	return w.replicatedShardID
}

func (w *messageWriter) CutoffNanos() int64 {
	w.RLock()
	res := w.cutOffNanos
	w.RUnlock()
	return res
}

func (w *messageWriter) SetCutoffNanos(nanos int64) {
	w.Lock()
	w.cutOffNanos = nanos
	w.Unlock()
}

func (w *messageWriter) CutoverNanos() int64 {
	w.RLock()
	res := w.cutOverNanos
	w.RUnlock()
	return res
}

func (w *messageWriter) SetCutoverNanos(nanos int64) {
	w.Lock()
	w.cutOverNanos = nanos
	w.Unlock()
}

func (w *messageWriter) MessageTTLNanos() int64 {
	w.RLock()
	res := w.messageTTLNanos
	w.RUnlock()
	return res
}

func (w *messageWriter) SetMessageTTLNanos(nanos int64) {
	w.Lock()
	w.messageTTLNanos = nanos
	w.Unlock()
}

// AddConsumerWriter adds a consumer writer.
func (w *messageWriter) AddConsumerWriter(cw consumerWriter) {
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

// RemoveConsumerWriter removes the consumer writer for the given address.
func (w *messageWriter) RemoveConsumerWriter(addr string) {
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

// Metrics returns the metrics. These are dynamic and change if downstream consumer instance changes.
func (w *messageWriter) Metrics() *messageWriterMetrics {
	return (*messageWriterMetrics)(w.metrics.Load())
}

// SetMetrics sets the metrics
//
// This allows changing the labels of the metrics when the downstream consumer instance changes.
func (w *messageWriter) SetMetrics(m *messageWriterMetrics) {
	w.metrics.Store(stdunsafe.Pointer(m))
}

// QueueSize returns the number of messages already enqueued in the writer.
func (w *messageWriter) QueueSize() int {
	return w.acks.size()
}

// BufferSize returns the number of messages being processed in the writer.
func (w *messageWriter) BufferSize() int {
	w.scanMtx.RLock()
	defer w.scanMtx.RUnlock()

	return len(w.incomingMsgs) + len(w.processingMsgs)
}
func (w *messageWriter) newMessage() *message {
	return w.mPool.Get()
}

func (w *messageWriter) close(m *message) {
	m.Close()
	w.mPool.Put(m)
}

type acks struct {
	mtx  sync.Mutex
	acks map[uint64]*message
}

// nolint: unparam
func newAckHelper(size int) *acks {
	return &acks{
		acks: make(map[uint64]*message, size),
	}
}

func (a *acks) add(meta metadata, m *message) {
	a.mtx.Lock()
	a.acks[meta.metadataKey.id] = m
	a.mtx.Unlock()
}

func (a *acks) remove(meta metadata) {
	a.mtx.Lock()
	delete(a.acks, meta.metadataKey.id)
	a.mtx.Unlock()
}

// ack processes the ack. returns true if the message was not already acked. additionally returns the expected
// processing time for lag calculations.
func (a *acks) ack(meta metadata) (bool, int64) {
	a.mtx.Lock()
	m, ok := a.acks[meta.metadataKey.id]
	if !ok {
		a.mtx.Unlock()
		// Acking a message that is already acked, which is ok.
		return false, 0
	}

	delete(a.acks, meta.metadataKey.id)
	a.mtx.Unlock()

	expectedProcessAtNanos := m.ExpectedProcessAtNanos()
	m.Ack()

	return true, expectedProcessAtNanos
}

func (a *acks) size() int {
	a.mtx.Lock()
	l := len(a.acks)
	a.mtx.Unlock()
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

// NextRetryNanosFn creates a MessageRetryNanosFn based on the retry options.
func NextRetryNanosFn(retryOpts retry.Options) func(int) int64 {
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

// StaticRetryNanosFn creates a MessageRetryNanosFn based on static config.
func StaticRetryNanosFn(backoffDurations []time.Duration) (MessageRetryNanosFn, error) {
	if len(backoffDurations) == 0 {
		return nil, errInvalidBackoffDuration
	}
	backoffInt64s := make([]int64, 0, len(backoffDurations))
	for _, b := range backoffDurations {
		backoffInt64s = append(backoffInt64s, int64(b))
	}
	return func(writeTimes int) int64 {
		retry := writeTimes - 1
		l := len(backoffInt64s)
		if retry < l {
			return backoffInt64s[retry]
		}
		return backoffInt64s[l-1]
	}, nil
}

func randIndex(iterationIndexes []int, i int) int {
	j := int(unsafe.Fastrandn(uint32(i + 1)))
	// NB: we should only mutate the order in the iteration indexes and
	// keep the order of consumer writers unchanged to prevent data race.
	iterationIndexes[i], iterationIndexes[j] = iterationIndexes[j], iterationIndexes[i]
	return iterationIndexes[i]
}

func zeroMsgBuf(buf []*message) []*message {
	for i := 0; i < len(buf); i++ {
		buf[i] = nil
	}

	return buf[:0]
}
