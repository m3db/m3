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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/uber/tchannel-go/thrift"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var (
	testWriteBatchPool writeBatchRequestPool
	testWriteArrayPool writeRequestArrayPool
)

func init() {
	testWriteBatchPool = newWriteBatchRequestPool(0)
	testWriteBatchPool.Init()
	testWriteArrayPool = newWriteRequestArrayPool(0, 0)
	testWriteArrayPool.Init()
}

type hostQueueResult struct {
	result interface{}
	err    error
}

func newHostQueueTestOptions() m3db.ClientOptions {
	return NewOptions().
		HostQueueOpsFlushSize(4).
		HostQueueOpsArrayPoolSize(4).
		WriteBatchSize(4).
		FetchBatchSize(4).
		HostQueueOpsFlushInterval(0)
}

func TestHostQueueWriteErrorBeforeOpen(t *testing.T) {
	opts := newHostQueueTestOptions()
	queue := newHostQueue(h, testWriteBatchPool, testWriteArrayPool, opts)

	err := queue.Enqueue(&writeOp{})
	assert.Error(t, err)
	assert.Equal(t, err, errQueueNotOpen)
}

func TestHostQueueWriteErrorAfterClose(t *testing.T) {
	opts := newHostQueueTestOptions()
	queue := newHostQueue(h, testWriteBatchPool, testWriteArrayPool, opts)

	queue.Open()
	queue.Close()

	err := queue.Enqueue(&writeOp{})
	assert.Error(t, err)
	assert.Equal(t, err, errQueueNotOpen)
}

func TestHostQueueWriteBatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := mocks.NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	queue := newHostQueue(h, testWriteBatchPool, testWriteArrayPool, opts).(*queue)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, stateOpen, queue.state)

	// Prepare callback for writes
	var (
		results []hostQueueResult
		wg      sync.WaitGroup
	)
	callback := func(r interface{}, err error) {
		results = append(results, hostQueueResult{r, err})
		wg.Done()
	}

	// Prepare writes
	writes := []*writeOp{
		testWriteOp("foo", 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback),
		testWriteOp("bar", 2.0, 2000, rpc.TimeType_UNIX_SECONDS, callback),
		testWriteOp("baz", 3.0, 3000, rpc.TimeType_UNIX_SECONDS, callback),
		testWriteOp("qux", 4.0, 4000, rpc.TimeType_UNIX_SECONDS, callback),
	}
	wg.Add(len(writes))

	for i, write := range writes[:3] {
		assert.NoError(t, queue.Enqueue(write))
		assert.Equal(t, i+1, queue.Len())

		// Sleep some so that we can ensure flushing is not happening until queue is full
		time.Sleep(20 * time.Millisecond)
	}

	// Prepare mocks for flush
	mockClient := mocks.NewMockTChanNode(ctrl)
	writeBatch := func(ctx thrift.Context, req *rpc.WriteBatchRequest) {
		for i, write := range writes {
			assert.Equal(t, *req.Elements[i], write.request)
		}
	}
	mockClient.EXPECT().WriteBatch(gomock.Any(), gomock.Any()).Do(writeBatch).Return(nil)

	mockConnPool.EXPECT().NextClient().Return(mockClient, nil)

	// Final write will flush
	assert.NoError(t, queue.Enqueue(writes[3]))
	assert.Equal(t, 0, queue.Len())

	// Wait for all writes
	wg.Wait()

	// Assert writes successful
	success := []hostQueueResult{{nil, nil}, {nil, nil}, {nil, nil}, {nil, nil}}
	assert.Equal(t, success, results)

	// Close
	var closeWg sync.WaitGroup
	closeWg.Add(1)
	mockConnPool.EXPECT().Close().Do(func() {
		closeWg.Done()
	})
	queue.Close()
	closeWg.Wait()
}

func TestHostQueueWriteBatchesNoClientAvailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := mocks.NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	opts = opts.HostQueueOpsFlushInterval(time.Millisecond)
	queue := newHostQueue(h, testWriteBatchPool, testWriteArrayPool, opts).(*queue)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, stateOpen, queue.state)

	// Prepare mocks for flush
	nextClientErr := fmt.Errorf("an error")
	mockConnPool.EXPECT().NextClient().Return(nil, nextClientErr)

	// Write
	var wg sync.WaitGroup
	wg.Add(1)
	callback := func(r interface{}, err error) {
		assert.Error(t, err)
		assert.Equal(t, nextClientErr, err)
		wg.Done()
	}
	assert.NoError(t, queue.Enqueue(testWriteOp("foo", 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback)))

	// Wait for background flush
	wg.Wait()

	// Close
	var closeWg sync.WaitGroup
	closeWg.Add(1)
	mockConnPool.EXPECT().Close().Do(func() {
		closeWg.Done()
	})
	queue.Close()
	closeWg.Wait()
}

func TestHostQueueWriteBatchesPartialBatchErrs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := mocks.NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	opts = opts.HostQueueOpsFlushSize(2)
	queue := newHostQueue(h, testWriteBatchPool, testWriteArrayPool, opts).(*queue)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, stateOpen, queue.state)

	// Prepare writes
	var wg sync.WaitGroup
	writeErr := "a write error"
	writes := []*writeOp{
		testWriteOp("foo", 1.0, 1000, rpc.TimeType_UNIX_SECONDS, func(r interface{}, err error) {
			assert.Error(t, err)
			assert.Equal(t, fmt.Sprintf("%v", err), writeErr)
			wg.Done()
		}),
		testWriteOp("bar", 2.0, 2000, rpc.TimeType_UNIX_SECONDS, func(r interface{}, err error) {
			assert.NoError(t, err)
			wg.Done()
		}),
	}
	wg.Add(len(writes))

	// Prepare mocks for flush
	mockClient := mocks.NewMockTChanNode(ctrl)
	writeBatch := func(ctx thrift.Context, req *rpc.WriteBatchRequest) {
		for i, write := range writes {
			assert.Equal(t, *req.Elements[i], write.request)
		}
	}
	batchErrs := &rpc.WriteBatchErrors{Errors: []*rpc.WriteBatchError{
		&rpc.WriteBatchError{ElementErrorIndex: 0, Error: &rpc.WriteError{Message: writeErr}},
	}}
	mockClient.EXPECT().WriteBatch(gomock.Any(), gomock.Any()).Do(writeBatch).Return(batchErrs)
	mockConnPool.EXPECT().NextClient().Return(mockClient, nil)

	// Perform writes
	for _, write := range writes {
		assert.NoError(t, queue.Enqueue(write))
	}

	// Wait for flush
	wg.Wait()

	// Close
	var closeWg sync.WaitGroup
	closeWg.Add(1)
	mockConnPool.EXPECT().Close().Do(func() {
		closeWg.Done()
	})
	queue.Close()
	closeWg.Wait()
}

func TestHostQueueWriteBatchesEntireBatchErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := mocks.NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	opts = opts.HostQueueOpsFlushSize(2)
	queue := newHostQueue(h, testWriteBatchPool, testWriteArrayPool, opts).(*queue)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, stateOpen, queue.state)

	// Prepare writes
	var wg sync.WaitGroup
	writeErr := fmt.Errorf("an error")
	callback := func(r interface{}, err error) {
		assert.Error(t, err)
		assert.Equal(t, writeErr, err)
		wg.Done()
	}
	writes := []*writeOp{
		testWriteOp("foo", 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback),
		testWriteOp("bar", 2.0, 2000, rpc.TimeType_UNIX_SECONDS, callback),
	}
	wg.Add(len(writes))

	// Prepare mocks for flush
	mockClient := mocks.NewMockTChanNode(ctrl)
	writeBatch := func(ctx thrift.Context, req *rpc.WriteBatchRequest) {
		for i, write := range writes {
			assert.Equal(t, *req.Elements[i], write.request)
		}
	}
	mockClient.EXPECT().WriteBatch(gomock.Any(), gomock.Any()).Do(writeBatch).Return(writeErr)
	mockConnPool.EXPECT().NextClient().Return(mockClient, nil)

	// Perform writes
	for _, write := range writes {
		assert.NoError(t, queue.Enqueue(write))
	}

	// Wait for flush
	wg.Wait()

	// Close
	var closeWg sync.WaitGroup
	closeWg.Add(1)
	mockConnPool.EXPECT().Close().Do(func() {
		closeWg.Done()
	})
	queue.Close()
	closeWg.Wait()
}

func TestHostQueueFetchBatches(t *testing.T) {
	ids := []string{"foo", "bar", "baz", "qux"}
	result := &rpc.FetchRawBatchResult_{}
	for _ = range ids {
		result.Elements = append(result.Elements, &rpc.FetchRawResult_{Segments: []*rpc.Segments{}})
	}
	var expected []hostQueueResult
	for i := range ids {
		expected = append(expected, hostQueueResult{result.Elements[i].Segments, nil})
	}
	testHostQueueFetchBatches(t, ids, result, expected, nil, func(results []hostQueueResult) {
		assert.Equal(t, expected, results)
	})
}

func TestHostQueueFetchBatchesErrorOnNextClientUnavailable(t *testing.T) {
	ids := []string{"foo", "bar", "baz", "qux"}
	expectedErr := fmt.Errorf("an error")
	var expected []hostQueueResult
	for _ = range ids {
		expected = append(expected, hostQueueResult{nil, expectedErr})
	}
	opts := &testHostQueueFetchBatchesOptions{
		nextClientErr: expectedErr,
	}
	testHostQueueFetchBatches(t, ids, nil, expected, opts, func(results []hostQueueResult) {
		assert.Equal(t, expected, results)
	})
}

func TestHostQueueFetchBatchesErrorOnFetchRawBatchError(t *testing.T) {
	ids := []string{"foo", "bar", "baz", "qux"}
	expectedErr := fmt.Errorf("an error")
	var expected []hostQueueResult
	for _ = range ids {
		expected = append(expected, hostQueueResult{nil, expectedErr})
	}
	opts := &testHostQueueFetchBatchesOptions{
		fetchRawBatchErr: expectedErr,
	}
	testHostQueueFetchBatches(t, ids, nil, expected, opts, func(results []hostQueueResult) {
		assert.Equal(t, expected, results)
	})
}

func TestHostQueueFetchBatchesErrorOnFetchNoResponse(t *testing.T) {
	ids := []string{"foo", "bar", "baz", "qux"}
	result := &rpc.FetchRawBatchResult_{}
	for _ = range ids[:len(ids)-1] {
		result.Elements = append(result.Elements, &rpc.FetchRawResult_{Segments: []*rpc.Segments{}})
	}
	var expected []hostQueueResult
	for i := range ids[:len(ids)-1] {
		expected = append(expected, hostQueueResult{result.Elements[i].Segments, nil})
	}
	expected = append(expected, hostQueueResult{nil, errQueueFetchNoResponse})
	testHostQueueFetchBatches(t, ids, result, expected, nil, func(results []hostQueueResult) {
		assert.Equal(t, expected, results)
	})
}

func TestHostQueueFetchBatchesErrorOnResultError(t *testing.T) {
	ids := []string{"foo", "bar", "baz", "qux"}
	anError := &rpc.Error{Message: "an error"}
	result := &rpc.FetchRawBatchResult_{}
	for _ = range ids[:len(ids)-1] {
		result.Elements = append(result.Elements, &rpc.FetchRawResult_{Segments: []*rpc.Segments{}})
	}
	result.Elements = append(result.Elements, &rpc.FetchRawResult_{Err: anError})
	var expected []hostQueueResult
	for i := range ids[:len(ids)-1] {
		expected = append(expected, hostQueueResult{result.Elements[i].Segments, nil})
	}
	testHostQueueFetchBatches(t, ids, result, expected, nil, func(results []hostQueueResult) {
		assert.Equal(t, expected, results[:len(results)-1])
		assert.Equal(t, anError.Message, results[len(results)-1].err.Error())
	})
}

type testHostQueueFetchBatchesOptions struct {
	nextClientErr    error
	fetchRawBatchErr error
}

func testHostQueueFetchBatches(
	t *testing.T,
	ids []string,
	result *rpc.FetchRawBatchResult_,
	expected []hostQueueResult,
	testOpts *testHostQueueFetchBatchesOptions,
	assertion func(results []hostQueueResult),
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := mocks.NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	queue := newHostQueue(h, testWriteBatchPool, testWriteArrayPool, opts).(*queue)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, stateOpen, queue.state)

	// Prepare callback for fetches
	var (
		results []hostQueueResult
		wg      sync.WaitGroup
	)
	callback := func(r interface{}, err error) {
		results = append(results, hostQueueResult{r, err})
		wg.Done()
	}

	// Prepare fetch batch op
	fetchBatch := &fetchBatchOp{
		request: rpc.FetchRawBatchRequest{
			RangeStart: 0,
			RangeEnd:   1,
			Ids:        ids,
		},
	}
	for _ = range fetchBatch.request.Ids {
		fetchBatch.completionFns = append(fetchBatch.completionFns, callback)
	}
	wg.Add(len(fetchBatch.request.Ids))

	// Prepare mocks for flush
	mockClient := mocks.NewMockTChanNode(ctrl)
	if testOpts != nil && testOpts.nextClientErr != nil {
		mockConnPool.EXPECT().NextClient().Return(nil, testOpts.nextClientErr)
	} else if testOpts != nil && testOpts.fetchRawBatchErr != nil {
		fetchRawBatch := func(ctx thrift.Context, req *rpc.FetchRawBatchRequest) {
			assert.Equal(t, &fetchBatch.request, req)
		}
		mockClient.EXPECT().
			FetchRawBatch(gomock.Any(), gomock.Any()).
			Do(fetchRawBatch).
			Return(nil, testOpts.fetchRawBatchErr)

		mockConnPool.EXPECT().NextClient().Return(mockClient, nil)
	} else {
		fetchRawBatch := func(ctx thrift.Context, req *rpc.FetchRawBatchRequest) {
			assert.Equal(t, &fetchBatch.request, req)
		}
		mockClient.EXPECT().
			FetchRawBatch(gomock.Any(), gomock.Any()).
			Do(fetchRawBatch).
			Return(result, nil)

		mockConnPool.EXPECT().NextClient().Return(mockClient, nil)
	}

	// Fetch
	assert.NoError(t, queue.Enqueue(fetchBatch))

	// Wait for fetch to complete
	wg.Wait()

	// Assert results match expected
	assertion(results)

	// Close
	var closeWg sync.WaitGroup
	closeWg.Add(1)
	mockConnPool.EXPECT().Close().Do(func() {
		closeWg.Done()
	})
	queue.Close()
	closeWg.Wait()
}

func testWriteOp(
	id string,
	value float64,
	timestamp int64,
	timeType rpc.TimeType,
	completionFn m3db.CompletionFn,
) *writeOp {
	w := &writeOp{}
	w.reset()
	w.request.ID = id
	w.datapoint = rpc.Datapoint{
		Value:         value,
		Timestamp:     timestamp,
		TimestampType: timeType,
	}
	w.completionFn = completionFn
	return w
}
