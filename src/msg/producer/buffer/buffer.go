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
	"sync"
	"time"

	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

var (
	errBufferFull                     = errors.New("buffer full")
	errBufferClosed                   = errors.New("buffer closed")
	errMessageTooLarge                = errors.New("message size larger than allowed")
	errMessageLargerThanMaxBufferSize = errors.New("message size larger than max buffer size")
)

type bufferMetrics struct {
	messageDropped  tally.Counter
	byteDropped     tally.Counter
	messageTooLarge tally.Counter
	messageBuffered tally.Gauge
	byteBuffered    tally.Gauge
	bufferScanBatch tally.Timer
}

func newBufferMetrics(
	scope tally.Scope,
	samplingRate float64,
) bufferMetrics {
	return bufferMetrics{
		messageDropped:  scope.Counter("buffer-message-dropped"),
		byteDropped:     scope.Counter("buffer-byte-dropped"),
		messageTooLarge: scope.Counter("message-too-large"),
		messageBuffered: scope.Gauge("message-buffered"),
		byteBuffered:    scope.Gauge("byte-buffered"),
		bufferScanBatch: instrument.MustCreateSampledTimer(scope.Timer("buffer-scan-batch"), samplingRate),
	}
}

type buffer struct {
	sync.RWMutex

	buffers        *list.List
	opts           Options
	maxBufferSize  uint64
	maxMessageSize uint32
	onFinalizeFn   producer.OnFinalizeFn
	m              bufferMetrics

	size      *atomic.Uint64
	isClosed  bool
	doneCh    chan struct{}
	forceDrop bool
	wg        sync.WaitGroup
}

// NewBuffer returns a new buffer.
func NewBuffer(opts Options) producer.Buffer {
	if opts == nil {
		opts = NewOptions()
	}
	b := &buffer{
		buffers:        list.New(),
		maxBufferSize:  uint64(opts.MaxBufferSize()),
		maxMessageSize: uint32(opts.MaxMessageSize()),
		opts:           opts,
		m: newBufferMetrics(
			opts.InstrumentOptions().MetricsScope(),
			opts.InstrumentOptions().MetricsSamplingRate(),
		),
		size:     atomic.NewUint64(0),
		doneCh:   make(chan struct{}),
		isClosed: false,
	}
	b.onFinalizeFn = b.subSize
	return b
}

func (b *buffer) Add(m producer.Message) (*producer.RefCountedMessage, error) {
	b.Lock()
	if b.isClosed {
		b.Unlock()
		return nil, errBufferClosed
	}
	s := m.Size()
	if s > b.maxMessageSize {
		b.Unlock()
		b.m.messageTooLarge.Inc(1)
		return nil, errMessageTooLarge
	}
	dataSize := uint64(s)
	if dataSize > b.maxBufferSize {
		b.Unlock()
		b.m.messageTooLarge.Inc(1)
		return nil, errMessageLargerThanMaxBufferSize
	}
	targetBufferSize := b.maxBufferSize - dataSize
	if b.size.Load() > targetBufferSize {
		if err := b.produceOnFullWithLock(targetBufferSize); err != nil {
			b.Unlock()
			return nil, err
		}
	}
	b.size.Add(dataSize)
	rm := producer.NewRefCountedMessage(m, b.onFinalizeFn)
	b.buffers.PushBack(rm)
	b.Unlock()
	return rm, nil
}

func (b *buffer) produceOnFullWithLock(targetSize uint64) error {
	switch b.opts.OnFullStrategy() {
	case ReturnError:
		return errBufferFull
	case DropEarliest:
		b.dropEarliestUntilTargetWithLock(targetSize)
	}
	return nil
}

func (b *buffer) dropEarliestUntilTargetWithLock(targetSize uint64) {
	var next *list.Element
	for e := b.buffers.Front(); e != nil && b.size.Load() > targetSize; e = next {
		next = e.Next()
		rm := e.Value.(*producer.RefCountedMessage)
		b.buffers.Remove(e)
		if rm.IsDroppedOrConsumed() {
			continue
		}
		// There is a chance that the message is consumed right before
		// the drop call which will lead drop to return false.
		if rm.Drop() {
			b.m.messageDropped.Inc(1)
			b.m.byteDropped.Inc(int64(rm.Size()))
		}
	}
}

func (b *buffer) Init() {
	b.wg.Add(1)
	go func() {
		b.cleanupUntilClose()
		b.wg.Done()
	}()
}

func (b *buffer) cleanupUntilClose() {
	ticker := time.NewTicker(b.opts.CleanupInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.cleanup()
		case <-b.doneCh:
			return
		}
	}
}

func (b *buffer) cleanup() {
	b.RLock()
	e := b.buffers.Front()
	b.RUnlock()
	batchSize := b.opts.ScanBatchSize()
	for e != nil {
		beforeBatch := time.Now()
		// NB: There is a chance the start element could be removed by another
		// thread since the lock will be released between scan batch.
		// For example when the there is a slow/dead consumer that is not
		// consuming anything and caused buffer to be full, a new write could
		// trigger dropEarliest and remove elements from the front of the list.
		// In this case, the batch starting from the removed element will do
		// nothing and will finish the tick, which is good as this avoids the
		// tick repeatedly scanning and doing nothing because nothing is being
		// consumed.
		b.Lock()
		e = b.cleanupBatchWithLock(e, batchSize)
		b.Unlock()
		b.m.bufferScanBatch.Record(time.Since(beforeBatch))
	}
	b.m.messageBuffered.Update(float64(b.bufferLen()))
	b.m.byteBuffered.Update(float64(b.size.Load()))
}

func (b *buffer) cleanupBatchWithLock(
	start *list.Element,
	batchSize int,
) *list.Element {
	var (
		iterated int
		next     *list.Element
	)
	for e := start; e != nil; e = next {
		iterated++
		if iterated > batchSize {
			break
		}
		next = e.Next()
		rm := e.Value.(*producer.RefCountedMessage)
		if rm.IsDroppedOrConsumed() {
			b.buffers.Remove(e)
			continue
		}
		if !b.forceDrop {
			continue
		}
		// There is a chance that the message is consumed right before
		// the drop call which will lead drop to return false.
		if rm.Drop() {
			b.m.messageDropped.Inc(1)
			b.m.byteDropped.Inc(int64(rm.Size()))
			b.buffers.Remove(e)
		}
	}
	return next
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
	b.RLock()
	l := b.buffers.Len()
	b.RUnlock()
	return l
}

func (b *buffer) subSize(rm *producer.RefCountedMessage) {
	b.size.Sub(rm.Size())
}
