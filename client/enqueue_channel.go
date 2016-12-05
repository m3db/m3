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

	"github.com/uber-go/tally"
)

type enqueueChannel struct {
	enqueued        int32
	peersMetadataCh chan []*blocksMetadata
	closed          int32
	qGauge          tally.Gauge
}

func newEnqueueChannel(m *streamFromPeersMetrics) *enqueueChannel {
	c := &enqueueChannel{
		peersMetadataCh: make(chan []*blocksMetadata, 2*4096),
		// capacity for 2x the total number of peers avoids deadlock on re-enqueue
		qGauge: m.blocksEnqueueChannel,
	}
	go c.updateGauge()

	return c
}

func (c *enqueueChannel) updateGauge() {
	for atomic.LoadInt32(&c.closed) == 0 {
		c.qGauge.Update(int64(len(c.peersMetadataCh)))
		time.Sleep(gaugeReportInterval)
	}
}

func (c *enqueueChannel) enqueue(peersMetadata []*blocksMetadata) {
	atomic.AddInt32(&c.enqueued, 1)
	c.peersMetadataCh <- peersMetadata
}

func (c *enqueueChannel) get() <-chan []*blocksMetadata {
	return c.peersMetadataCh
}

func (c *enqueueChannel) maybeDestruct() {
	// todo@bl: temporary! make a deterministic destructor

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&c.enqueued) == 0 && atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		close(c.peersMetadataCh)
		c.qGauge.Update(0)
	}
}

func (c *enqueueChannel) done() {
	if atomic.AddInt32(&c.enqueued, -1) == 0 {
		c.maybeDestruct()
	}
}
