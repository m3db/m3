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
	"fmt"
	"sync"
)

type ackRouter interface {
	// Ack acks the metadata.
	Ack(ack metadata) error

	// Register registers a message writer.
	Register(replicatedShardID uint64, mw *messageWriter)

	// Unregister removes a message writer.
	Unregister(replicatedShardID uint64)
}

var _ ackRouter = (*router)(nil)

type router struct {
	sync.RWMutex

	messageWriters map[uint64]*messageWriter
}

func newAckRouter(size int) *router {
	return &router{
		messageWriters: make(map[uint64]*messageWriter, size),
	}
}

func (r *router) Ack(meta metadata) error {
	r.RLock()
	mw, ok := r.messageWriters[meta.shard]
	r.RUnlock()
	if !ok {
		// Unexpected.
		return fmt.Errorf("can't find shard %v", meta.shard)
	}
	mw.Ack(meta)
	return nil
}

func (r *router) Register(replicatedShardID uint64, mw *messageWriter) {
	r.Lock()
	r.messageWriters[replicatedShardID] = mw
	r.Unlock()
}

func (r *router) Unregister(replicatedShardID uint64) {
	r.Lock()
	delete(r.messageWriters, replicatedShardID)
	r.Unlock()
}
