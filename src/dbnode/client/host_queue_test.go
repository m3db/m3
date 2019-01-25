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

import "github.com/m3db/m3x/pool"

var (
	smallPoolOptions            = pool.NewObjectPoolOptions().SetSize(1)
	testWriteBatchRawPool       writeBatchRawRequestPool
	testWriteArrayPool          writeBatchRawRequestElementArrayPool
	testWriteTaggedBatchRawPool writeTaggedBatchRawRequestPool
	testWriteTaggedArrayPool    writeTaggedBatchRawRequestElementArrayPool
)

func init() {
	testWriteBatchRawPool = newWriteBatchRawRequestPool(smallPoolOptions)
	testWriteBatchRawPool.Init()
	testWriteArrayPool = newWriteBatchRawRequestElementArrayPool(smallPoolOptions, 0)
	testWriteArrayPool.Init()
	testWriteTaggedBatchRawPool = newWriteTaggedBatchRawRequestPool(smallPoolOptions)
	testWriteTaggedBatchRawPool.Init()
	testWriteTaggedArrayPool = newWriteTaggedBatchRawRequestElementArrayPool(smallPoolOptions, 0)
	testWriteTaggedArrayPool.Init()
}

type hostQueueResult struct {
	result interface{}
	err    error
}

func newHostQueueTestOptions() Options {
	return newSessionTestOptions().
		SetHostQueueOpsFlushSize(4).
		SetHostQueueOpsArrayPoolSize(4).
		SetWriteBatchSize(4).
		SetFetchBatchSize(4).
		SetHostQueueOpsFlushInterval(0)
}
