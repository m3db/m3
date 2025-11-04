// Copyright (c) 2025 Uber Technologies, Inc.
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
	murmur3 "github.com/m3db/stackmurmur3/v2"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	xerrors "github.com/m3db/m3/src/x/errors"
)

// queueRouter selects a child queue for an enqueued buffer.
type queueRouter interface {
	Select(buf protobuf.Buffer, num int) int
}

type hashRouter struct{}

func (hashRouter) Select(buf protobuf.Buffer, num int) int {
	if num <= 1 {
		return 0
	}
	return int(murmur3.Sum32(buf.Bytes()) % uint32(num))
}

// multiQueue fans out enqueues across multiple underlying instance queues.
// Each child queue maintains its own connection and buffers.
type multiQueue struct {
	queues []instanceQueue
	router queueRouter
}

// newInstanceMultiQueue creates a multi-queue with N independent queues/connections.
func newInstanceMultiQueue(instance placement.Instance, opts Options) instanceQueue {
	num := opts.ConnectionsPerInstance()
	if num < 1 {
		num = 1
	}

	queues := make([]instanceQueue, 0, num)
	for i := 0; i < num; i++ {
		queues = append(queues, newSingleInstanceQueue(instance, opts))
	}

	return &multiQueue{queues: queues, router: hashRouter{}}
}

func (mq *multiQueue) Enqueue(buf protobuf.Buffer) error {
	idx := mq.router.Select(buf, len(mq.queues))
	return mq.queues[idx].Enqueue(buf)
}

func (mq *multiQueue) Size() int {
	total := 0
	for _, q := range mq.queues {
		total += q.Size()
	}
	return total
}

func (mq *multiQueue) Close() error {
	multiErr := xerrors.NewMultiError()
	for _, q := range mq.queues {
		if err := q.Close(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (mq *multiQueue) Flush() {
	for _, q := range mq.queues {
		q.Flush()
	}
}
