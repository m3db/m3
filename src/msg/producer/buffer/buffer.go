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
	"github.com/m3db/m3msg/producer/msg"

	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
)

var (
	errBufferFull                     = errors.New("buffer full")
	errBufferClosed                   = errors.New("buffer closed")
	errMessageTooLarge                = errors.New("message size larger than allowed")
	errMessageLargerThanMaxBufferSize = errors.New("message size larger than max buffer size")
)

type bufferMetrics struct {
	messageDropped  tally.Counter
	bytesDropped    tally.Counter
	messageTooLarge tally.Counter
	messageBuffered tally.Gauge
	bytesBuffered   tally.Gauge
}

func newBufferMetrics(scope tally.Scope) bufferMetrics {
	return bufferMetrics{
		messageDropped:  scope.Counter("message-dropped"),
		bytesDropped:    scope.Counter("bytes-dropped"),
		messageTooLarge: scope.Counter("message-too-large"),
		messageBuffered: scope.Gauge("message-buffered"),
		bytesBuffered:   scope.Gauge("bytes-buffered"),
	}
}

type buffer struct {
	sync.RWMutex

	buffers        *list.List
	opts           Options
	maxBufferSize  uint64
	maxMessageSize uint32
	onFinalizeFn   msg.OnFinalizeFn
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
		m:              newBufferMetrics(opts.InstrumentOptions().MetricsScope()),
		size:           atomic.NewUint64(0),
		doneCh:         make(chan struct{}),
		isClosed:       false,
	}
	b.onFinalizeFn = b.subSize
	return b
}

func (b *buffer) Add(m producer.Message) (producer.RefCountedMessage, error) {
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
	rm := msg.NewRefCountedMessage(m, b.onFinalizeFn)
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
		rm := e.Value.(producer.RefCountedMessage)
		b.buffers.Remove(e)
		if rm.IsDroppedOrConsumed() {
			continue
		}
		// There is a chance that the message is consumed right before
		// the drop call which will lead drop to return false.
		if rm.Drop() {
			b.m.messageDropped.Inc(1)
			b.m.bytesDropped.Inc(int64(rm.Size()))
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
			b.Lock()
			b.cleanupWithLock()
			l := b.buffers.Len()
			b.Unlock()
			b.m.messageBuffered.Update(float64(l))
			b.m.bytesBuffered.Update(float64(b.size.Load()))
		case <-b.doneCh:
			return
		}
	}
}

func (b *buffer) cleanupWithLock() {
	var next *list.Element
	for e := b.buffers.Front(); e != nil; e = next {
		next = e.Next()
		rm := e.Value.(producer.RefCountedMessage)
		if rm.IsDroppedOrConsumed() {
			b.buffers.Remove(e)
			continue
		}
		if !b.forceDrop {
			continue
		}
		if rm.Drop() {
			b.m.messageDropped.Inc(1)
			b.m.bytesDropped.Inc(int64(rm.Size()))
			b.buffers.Remove(e)
		}
	}
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
	if b.isBufferEmpty() {
		return
	}
	ticker := time.NewTicker(b.opts.CloseCheckInterval())
	defer ticker.Stop()

	for range ticker.C {
		if b.isBufferEmpty() {
			return
		}
	}
}

func (b *buffer) isBufferEmpty() bool {
	b.RLock()
	l := b.buffers.Len()
	b.RUnlock()
	return l == 0
}

func (b *buffer) subSize(rm producer.RefCountedMessage) {
	b.size.Sub(rm.Size())
}
