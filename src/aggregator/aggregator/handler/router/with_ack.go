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

package router

import (
	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3msg/producer"
)

type withAckRouter struct {
	p producer.Producer
}

// NewWithAckRouter creates a new router that routes buffer and waits for acknowledgements.
func NewWithAckRouter(p producer.Producer) Router {
	return withAckRouter{p: p}
}

func (r withAckRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	return r.p.Produce(newMessage(shard, buffer))
}

func (r withAckRouter) Close() {
	r.p.Close(producer.WaitForConsumption)
}

type message struct {
	shard  uint32
	buffer *common.RefCountedBuffer
}

// TODO(cw): Pool the messages if needed.
func newMessage(shard uint32, buffer *common.RefCountedBuffer) producer.Message {
	return message{shard: shard, buffer: buffer}
}

func (d message) Shard() uint32 {
	return d.shard
}

func (d message) Bytes() []byte {
	return d.buffer.Buffer().Bytes()
}

func (d message) Size() int {
	// Use the cap of the underlying byte slice in the buffer instead of
	// the length of the byte encoded to avoid "memory leak", for example
	// when the underlying buffer is 2KB, and it only encoded 300B, if we
	// use 300 as the size, then a producer with a buffer of 3GB could be
	// actually buffering 20GB in total for the underlying buffers.
	return cap(d.Bytes())
}

func (d message) Finalize(producer.FinalizeReason) {
	d.buffer.DecRef()
}
