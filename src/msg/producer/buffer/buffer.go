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

package buffer

import (
	"container/list"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

var (
	emptyStruct = struct{}{}

	// ErrBufferFull is returned when the buffer is full.
	ErrBufferFull = errors.New("buffer full")

	errBufferClosed      = errors.New("buffer closed")
	errMessageTooLarge   = errors.New("message size larger than allowed")
	errCleanupNoProgress = errors.New("buffer cleanup no progress")
)

type bufferMetrics struct {
	messageDropped    counterPerNumRefBuckets
	byteDropped       counterPerNumRefBuckets
	messageTooLarge   tally.Counter
	cleanupNoProgress tally.Counter
	dropOldestSync    tally.Counter
	dropOldestAsync   tally.Counter
	messageBuffered   tally.Gauge
	byteBuffered      tally.Gauge
	bufferScanBatch   tally.Timer
	bytesAdded        tally.Counter
	bytesRemoved      tally.Counter
}

type counterPerNumRefBuckets struct {
	buckets       []counterPerNumRefBucket
	unknownBucket tally.Counter
}

type counterPerNumRefBucket struct {
	// numRef is the counter for the number of references at time of count
	// of the ref counted message.
	numRef int
	// counter is the actual counter for this bucket.
	counter tally.Counter
}

func newCounterPerNumRefBuckets(
	scope tally.Scope,
	name string,
	n int,
) counterPerNumRefBuckets {
	buckets := make([]counterPerNumRefBucket, 0, n)
	for i := 0; i < n; i++ {
		buckets = append(buckets, counterPerNumRefBucket{
			numRef: i,
			counter: scope.Tagged(map[string]string{
				"num-replicas": strconv.Itoa(i),
			}).Counter(name),
		})
	}
	return counterPerNumRefBuckets{
		buckets: buckets,
		unknownBucket: scope.Tagged(map[string]string{
			"num-replicas": "unknown",
		}).Counter(name),
	}
}

func (c counterPerNumRefBuckets) Inc(numRef int32, delta int64) {
	for _, b := range c.buckets {
		if b.numRef == int(numRef) {
			b.counter.Inc(delta)
			return
		}
	}
	c.unknownBucket.Inc(delta)
}

func newBufferMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) bufferMetrics {
	return bufferMetrics{
		messageDropped:    newCounterPerNumRefBuckets(scope, "buffer-message-dropped", 10),
		byteDropped:       newCounterPerNumRefBuckets(scope, "buffer-byte-dropped", 10),
		messageTooLarge:   scope.Counter("message-too-large"),
		cleanupNoProgress: scope.Counter("cleanup-no-progress"),
		dropOldestSync:    scope.Counter("drop-oldest-sync"),
		dropOldestAsync:   scope.Counter("drop-oldest-async"),
		messageBuffered:   scope.Gauge("message-buffered"),
		byteBuffered:      scope.Gauge("byte-buffered"),
		bufferScanBatch:   instrument.NewTimer(scope, "buffer-scan-batch", opts),
		bytesAdded:        scope.Counter("buffer-bytes-added"),
		bytesRemoved:      scope.Counter("buffer-bytes-removed"),
	}
}

// nolint: maligned
type buffer struct {
	sync.RWMutex

	listLock         sync.RWMutex
	bufferList       *list.List
	opts             Options
	maxBufferSize    uint64
	maxSpilloverSize uint64
	maxMessageSize   int
	onFinalizeFn     producer.OnFinalizeFn
	retrier          retry.Retrier
	m                bufferMetrics

	size         *atomic.Uint64
	isClosed     bool
	dropOldestCh chan struct{}
	doneCh       chan struct{}
	forceDrop    bool
	wg           sync.WaitGroup
}

// NewBuffer returns a new buffer.
func NewBuffer(opts Options) (producer.Buffer, error) {
	if opts == nil {
		opts = NewOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	maxBufferSize := uint64(opts.MaxBufferSize())
	allowedSpillover := float64(maxBufferSize) * opts.AllowedSpilloverRatio()
	b := &buffer{
		bufferList:       list.New(),
		maxBufferSize:    maxBufferSize,
		maxSpilloverSize: uint64(allowedSpillover) + maxBufferSize,
		maxMessageSize:   opts.MaxMessageSize(),
		opts:             opts,
		retrier:          retry.NewRetrier(opts.CleanupRetryOptions()),
		m: newBufferMetrics(
			opts.InstrumentOptions().MetricsScope(),
			opts.InstrumentOptions().TimerOptions(),
		),
		size:         atomic.NewUint64(0),
		isClosed:     false,
		dropOldestCh: make(chan struct{}, 1),
		doneCh:       make(chan struct{}),
	}
	b.onFinalizeFn = b.subSize
	return b, nil
}

func (b *buffer) Add(m producer.Message) (*producer.RefCountedMessage, error) {
	s := m.Size()
	b.m.bytesAdded.Inc(int64(s))
	if s > b.maxMessageSize {
		b.m.messageTooLarge.Inc(1)
		return nil, errMessageTooLarge
	}
	b.RLock()
	if b.isClosed {
		b.RUnlock()
		return nil, errBufferClosed
	}
	messageSize := uint64(s)
	newBufferSize := b.size.Add(messageSize)
	if newBufferSize > b.maxBufferSize {
		if err := b.produceOnFull(newBufferSize, messageSize); err != nil {
			b.RUnlock()
			return nil, err
		}
	}
	rm := producer.NewRefCountedMessage(m, b.onFinalizeFn)
	b.listLock.Lock()
	b.bufferList.PushBack(rm)
	b.listLock.Unlock()
	b.RUnlock()
	return rm, nil
}

func (b *buffer) produceOnFull(newBufferSize uint64, messageSize uint64) error {
	switch b.opts.OnFullStrategy() {
	case ReturnError:
		b.size.Sub(messageSize)
		return ErrBufferFull
	case DropOldest:
		if newBufferSize >= b.maxSpilloverSize {
			// The size after the write reached max allowed spill over size.
			// We have to clean up the buffer synchronizely to make room for
			// the new write.
			b.dropOldestUntilTarget(b.maxBufferSize)
			b.m.dropOldestSync.Inc(1)
			return nil
		}
		// The new message is within the allowed spill over range, clean up
		// the buffer asynchronizely.
		select {
		case b.dropOldestCh <- emptyStruct:
		default:
		}
		b.m.dropOldestAsync.Inc(1)
	}
	return nil
}

func (b *buffer) Init() {
	b.wg.Add(1)
	go func() {
		b.cleanupUntilClose()
		b.wg.Done()
	}()

	if b.opts.OnFullStrategy() != DropOldest {
		return
	}
	b.wg.Add(1)
	go func() {
		b.dropOldestUntilClose()
		b.wg.Done()
	}()
}

func (b *buffer) cleanupUntilClose() {
	ticker := time.NewTicker(
		b.opts.CleanupRetryOptions().InitialBackoff(),
	)
	defer ticker.Stop()

	continueFn := func(int) bool {
		select {
		case <-b.doneCh:
			return false
		default:
			return true
		}
	}
	for {
		select {
		case <-ticker.C:
			b.retrier.AttemptWhile(
				continueFn,
				b.cleanup,
			)
		case <-b.doneCh:
			return
		}
	}
}

func (b *buffer) cleanup() error {
	b.listLock.RLock()
	e := b.bufferList.Front()
	b.listLock.RUnlock()
	b.RLock()
	forceDrop := b.forceDrop
	b.RUnlock()
	var (
		batchSize    = b.opts.ScanBatchSize()
		totalRemoved int
		batchRemoved int
	)
	for e != nil {
		beforeBatch := time.Now()
		// NB: There is a chance the start element could be removed by another
		// thread since the lock will be released between scan batch.
		// For example when the there is a slow/dead consumer that is not
		// consuming anything and caused buffer to be full, a new write could
		// trigger dropOldest and remove elements from the front of the list.
		// In this case, the batch starting from the removed element will do
		// nothing and will finish the tick, which is good as this avoids the
		// tick repeatedly scanning and doing nothing because nothing is being
		// consumed.
		b.listLock.Lock()
		e, batchRemoved = b.cleanupBatchWithListLock(e, batchSize, forceDrop)
		b.listLock.Unlock()
		b.m.bufferScanBatch.Record(time.Since(beforeBatch))
		totalRemoved += batchRemoved
	}
	b.m.messageBuffered.Update(float64(b.bufferLen()))
	b.m.byteBuffered.Update(float64(b.size.Load()))
	if totalRemoved == 0 {
		b.m.cleanupNoProgress.Inc(1)
		return errCleanupNoProgress
	}
	return nil
}

func (b *buffer) cleanupBatchWithListLock(
	start *list.Element,
	batchSize int,
	forceDrop bool,
) (*list.Element, int) {
	var (
		iterated int
		next     *list.Element
		removed  int
	)
	for e := start; e != nil; e = next {
		iterated++
		if iterated > batchSize {
			break
		}
		next = e.Next()
		rm := e.Value.(*producer.RefCountedMessage)
		if rm.IsDroppedOrConsumed() {
			b.bufferList.Remove(e)
			removed++
			continue
		}
		if !forceDrop {
			continue
		}
		// There is a chance that the message is consumed right before
		// the drop call which will lead drop to return false.
		if rm.Drop() {
			b.bufferList.Remove(e)
			removed++

			numRef := rm.NumRef()
			b.m.messageDropped.Inc(numRef, 1)
			b.m.byteDropped.Inc(numRef, int64(rm.Size()))
		}
	}
	return next, removed
}

func (b *buffer) dropOldestUntilClose() {
	ticker := time.NewTicker(b.opts.DropOldestInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case <-b.dropOldestCh:
			default:
				continue
			}
			b.dropOldestUntilTarget(b.maxBufferSize)
		case <-b.doneCh:
			return
		}
	}
}

func (b *buffer) dropOldestUntilTarget(targetSize uint64) {
	shouldContinue := true
	for shouldContinue {
		b.listLock.Lock()
		shouldContinue = b.dropOldestBatchUntilTargetWithListLock(targetSize, b.opts.ScanBatchSize())
		b.listLock.Unlock()
	}
}

func (b *buffer) dropOldestBatchUntilTargetWithListLock(
	targetSize uint64,
	batchSize int,
) bool {
	var (
		iterated int
		e        = b.bufferList.Front()
	)
	for e != nil && b.size.Load() > targetSize {
		iterated++
		if iterated > batchSize {
			return true
		}
		next := e.Next()
		rm := e.Value.(*producer.RefCountedMessage)
		b.bufferList.Remove(e)
		e = next
		if rm.IsDroppedOrConsumed() {
			continue
		}
		// There is a chance that the message is consumed right before
		// the drop call which will lead drop to return false.
		if rm.Drop() {
			numRef := rm.NumRef()
			b.m.messageDropped.Inc(numRef, 1)
			b.m.byteDropped.Inc(numRef, int64(rm.Size()))
		}
	}
	return false
}

func (b *buffer) Close(ct producer.CloseType) {
	// Stop taking writes right away.
	b.Lock()
	if b.isClosed {
		b.Unlock()
		return
	}
	b.isClosed = true
	if ct == producer.DropEverything {
		b.forceDrop = true
	}
	b.Unlock()
	b.waitUntilAllDataConsumed()
	close(b.doneCh)
	close(b.dropOldestCh)
	b.wg.Wait()
}

func (b *buffer) waitUntilAllDataConsumed() {
	if b.bufferLen() == 0 {
		return
	}
	ticker := time.NewTicker(b.opts.CloseCheckInterval())
	defer ticker.Stop()

	for range ticker.C {
		if b.bufferLen() == 0 {
			return
		}
	}
}

func (b *buffer) bufferLen() int {
	b.listLock.RLock()
	l := b.bufferList.Len()
	b.listLock.RUnlock()
	return l
}

func (b *buffer) subSize(rm *producer.RefCountedMessage) {
	b.m.bytesRemoved.Inc(int64(rm.Size()))
	b.size.Sub(rm.Size())
}
