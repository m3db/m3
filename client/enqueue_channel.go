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

package client

import (
	"sync/atomic"
	"time"
)

type enqueueChannel struct {
	enqueued        uint64
	processed       uint64
	peersMetadataCh chan []*blocksMetadata
	closed          int64
	metrics         *streamFromPeersMetrics
}

func newEnqueueChannel(m *streamFromPeersMetrics) *enqueueChannel {
	c := &enqueueChannel{
		peersMetadataCh: make(chan []*blocksMetadata, 4096),
		closed:          0,
		metrics:         m,
	}
	go func() {
		for atomic.LoadInt64(&c.closed) == 0 {
			m.blocksEnqueueChannel.Update(int64(len(c.peersMetadataCh)))
			time.Sleep(gaugeReportInterval)
		}
	}()
	return c
}

func (c *enqueueChannel) enqueue(peersMetadata []*blocksMetadata) {
	atomic.AddUint64(&c.enqueued, 1)
	c.peersMetadataCh <- peersMetadata
}

func (c *enqueueChannel) enqueueDelayed() func([]*blocksMetadata) {
	atomic.AddUint64(&c.enqueued, 1)
	return func(peersMetadata []*blocksMetadata) {
		c.peersMetadataCh <- peersMetadata
	}
}

func (c *enqueueChannel) get() <-chan []*blocksMetadata {
	return c.peersMetadataCh
}

func (c *enqueueChannel) trackProcessed(amount int) {
	atomic.AddUint64(&c.processed, uint64(amount))
}

func (c *enqueueChannel) unprocessedLen() int {
	return int(atomic.LoadUint64(&c.enqueued) - atomic.LoadUint64(&c.processed))
}

func (c *enqueueChannel) closeOnAllProcessed() {
	defer func() {
		atomic.StoreInt64(&c.closed, 1)
	}()
	for {
		if c.unprocessedLen() == 0 {
			// Will only ever be zero after all is processed if called
			// after enqueueing the desired set of entries as long as
			// the guarentee that reattempts are enqueued before the
			// failed attempt is marked as processed is upheld
			close(c.peersMetadataCh)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
