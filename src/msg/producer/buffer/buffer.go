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
	"github.com/m3db/m3msg/producer/data"

	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
)

var (
	errBufferFull                         = errors.New("buffer full")
	errBufferClosed                       = errors.New("buffer closed")
	errInvalidDataLargerThanMaxBufferSize = errors.New("invalid data, larger than max buffer size")
)

type bufferMetrics struct {
	messageDropped  tally.Counter
	bytesDropped    tally.Counter
	dataTooLarge    tally.Counter
	messageBuffered tally.Gauge
	bytesBuffered   tally.Gauge
}

func newBufferMetrics(scope tally.Scope) bufferMetrics {
	return bufferMetrics{
		messageDropped:  scope.Counter("message-dropped"),
		bytesDropped:    scope.Counter("bytes-dropped"),
		dataTooLarge:    scope.Counter("data-too-large"),
		messageBuffered: scope.Gauge("message-buffered"),
		bytesBuffered:   scope.Gauge("bytes-buffered"),
	}
}

type buffer struct {
	sync.RWMutex

	buffers       *list.List
	opts          Options
	maxBufferSize uint64
	onFinalizeFn  data.OnFinalizeFn
	m             bufferMetrics

	size     *atomic.Uint64
	isClosed bool
	doneCh   chan struct{}
	closeWG  sync.WaitGroup
}

// NewBuffer returns a new buffer.
func NewBuffer(opts Options) producer.Buffer {
	if opts == nil {
		opts = NewBufferOptions()
	}
	b := &buffer{
		buffers:       list.New(),
		maxBufferSize: uint64(opts.MaxBufferSize()),
		opts:          opts,
		m:             newBufferMetrics(opts.InstrumentOptions().MetricsScope()),
		size:          atomic.NewUint64(0),
		doneCh:        make(chan struct{}),
		isClosed:      false,
	}
	b.onFinalizeFn = b.subSize
	return b
}

func (b *buffer) Add(d producer.Data) (producer.RefCountedData, error) {
	b.Lock()
	if b.isClosed {
		b.Unlock()
		return nil, errBufferClosed
	}
	var (
		dataSize      = uint64(d.Size())
		maxBufferSize = b.maxBufferSize
	)
	if dataSize > maxBufferSize {
		b.Unlock()
		b.m.dataTooLarge.Inc(1)
		return nil, errInvalidDataLargerThanMaxBufferSize
	}
	targetBufferSize := maxBufferSize - dataSize
	if b.size.Load() > targetBufferSize {
		if err := b.produceOnFullWithLock(targetBufferSize); err != nil {
			b.Unlock()
			return nil, err
		}
	}
	b.size.Add(dataSize)
	rd := data.NewRefCountedData(d, b.onFinalizeFn)
	b.buffers.PushBack(rd)
	b.Unlock()
	return rd, nil
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
		d := e.Value.(producer.RefCountedData)
		b.buffers.Remove(e)
		if d.IsDroppedOrConsumed() {
			continue
		}
		// There is a chance that the data is consumed right before
		// the drop call which will lead drop to return false.
		if d.Drop() {
			b.m.messageDropped.Inc(1)
			b.m.bytesDropped.Inc(int64(d.Size()))
		}
	}
}

func (b *buffer) Init() {
	b.closeWG.Add(1)
	go b.cleanupForever()
}

func (b *buffer) cleanupForever() {
	defer b.closeWG.Done()

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
		d := e.Value.(producer.RefCountedData)
		if d.IsDroppedOrConsumed() {
			b.buffers.Remove(e)
		}
	}
}

func (b *buffer) Close() {
	// Stop taking writes right away.
	b.Lock()
	if b.isClosed {
		b.Unlock()
		return
	}
	b.isClosed = true
	b.Unlock()

	b.waitUntilAllDataConsumed()
	close(b.doneCh)
	b.closeWG.Wait()
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

func (b *buffer) subSize(d producer.RefCountedData) {
	b.size.Sub(d.Size())
}
