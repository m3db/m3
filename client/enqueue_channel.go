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
	enqueued        int32
	peersMetadataCh chan []*blocksMetadata
	closed          int32
	metrics         *streamFromPeersMetrics
}

func newEnqueueChannel(m *streamFromPeersMetrics) *enqueueChannel {
	c := &enqueueChannel{
		peersMetadataCh: make(chan []*blocksMetadata, 4096),
		metrics:         m,
	}
	go func() {
		for atomic.LoadInt32(&c.closed) == 0 {
			m.blocksEnqueueChannel.Update(int64(len(c.peersMetadataCh)))
			time.Sleep(gaugeReportInterval)
		}
		m.blocksEnqueueChannel.Update(0)
	}()
	return c
}

func (c *enqueueChannel) enqueue(peersMetadata []*blocksMetadata) {
	atomic.AddInt32(&c.enqueued, 1)
	c.peersMetadataCh <- peersMetadata
}

func (c *enqueueChannel) enqueueDelayed() func([]*blocksMetadata) {
	atomic.AddInt32(&c.enqueued, 1)
	return func(peersMetadata []*blocksMetadata) {
		c.peersMetadataCh <- peersMetadata
	}
}

func (c *enqueueChannel) get() <-chan []*blocksMetadata {
	return c.peersMetadataCh
}

func (c *enqueueChannel) trackProcessed() {
	if atomic.AddInt32(&c.enqueued, -1) == 0 {
		close(c.peersMetadataCh)
		atomic.StoreInt32(&c.closed, 1)
	}
}
