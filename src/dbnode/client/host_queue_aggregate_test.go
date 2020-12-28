// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/thrift"
)

func TestHostQueueDrainOnCloseAggregate(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	mockConnPool := NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	queue := newTestHostQueue(opts)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, statusOpen, queue.status)

	// Prepare callback for writes
	var (
		results []hostQueueResult
		wg      sync.WaitGroup
	)
	callback := func(r interface{}, err error) {
		results = append(results, hostQueueResult{r, err})
		wg.Done()
	}

	// Prepare aggregates
	aggregate := testAggregateOp("testNs", callback)
	wg.Add(1)
	assert.NoError(t, queue.Enqueue(aggregate))
	assert.Equal(t, 1, queue.Len())
	// Sleep some so that we can ensure flushing is not happening until queue is full
	time.Sleep(20 * time.Millisecond)

	mockClient := rpc.NewMockTChanNode(ctrl)
	aggregateExec := func(ctx thrift.Context, req *rpc.AggregateQueryRawRequest) {
		assert.Equal(t, aggregate.request.NameSpace, req.NameSpace)
	}
	mockClient.EXPECT().AggregateRaw(gomock.Any(), gomock.Any()).Do(aggregateExec).Return(nil, nil)
	mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil)
	mockConnPool.EXPECT().Close().AnyTimes()

	// Close the queue should cause all writes to be flushed
	queue.Close()
	closeCh := make(chan struct{})
	go func() {
		// Wait for all writes
		wg.Wait()
		close(closeCh)
	}()

	select {
	case <-closeCh:
	case <-time.After(time.Minute):
		assert.Fail(t, "Not flushing writes")
	}

	// Assert aggregate successful
	assert.Equal(t, 1, len(results))
	for _, result := range results {
		assert.Nil(t, result.err)
	}
}

func TestHostQueueAggregate(t *testing.T) {
	namespace := "testNs"
	res := &rpc.AggregateQueryRawResult_{
		Results: []*rpc.AggregateQueryRawResultTagNameElement{
			&rpc.AggregateQueryRawResultTagNameElement{
				TagName: []byte("tagName"),
			},
		},
		Exhaustive: true,
	}
	expectedResults := []hostQueueResult{
		hostQueueResult{
			result: aggregateResultAccumulatorOpts{
				response: res,
				host:     h,
			},
		},
	}
	testHostQueueAggregate(t, namespace, res, expectedResults, nil, func(results []hostQueueResult) {
		assert.Equal(t, expectedResults, results)
	})
}

func TestHostQueueAggregateErrorOnNextClientUnavailable(t *testing.T) {
	namespace := "testNs"
	expectedErr := fmt.Errorf("an error")
	expectedResults := []hostQueueResult{
		hostQueueResult{
			result: aggregateResultAccumulatorOpts{
				host: h,
			},
			err: expectedErr,
		},
	}
	opts := &testHostQueueAggregateOptions{
		nextClientErr: expectedErr,
	}
	testHostQueueAggregate(t, namespace, nil, expectedResults, opts, func(results []hostQueueResult) {
		assert.Equal(t, expectedResults, results)
	})
}

func TestHostQueueAggregateErrorOnAggregateError(t *testing.T) {
	namespace := "testNs"
	expectedErr := fmt.Errorf("an error")
	expectedResults := []hostQueueResult{
		hostQueueResult{
			result: aggregateResultAccumulatorOpts{host: h},
			err:    expectedErr,
		},
	}
	opts := &testHostQueueAggregateOptions{
		aggregateErr: expectedErr,
	}
	testHostQueueAggregate(t, namespace, nil, expectedResults, opts, func(results []hostQueueResult) {
		assert.Equal(t, expectedResults, results)
	})
}

type testHostQueueAggregateOptions struct {
	nextClientErr error
	aggregateErr  error
}

func testHostQueueAggregate(
	t *testing.T,
	namespace string,
	result *rpc.AggregateQueryRawResult_,
	expected []hostQueueResult,
	testOpts *testHostQueueAggregateOptions,
	assertion func(results []hostQueueResult),
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions().
		SetHostQueueOpsFlushInterval(time.Millisecond)
	queue := newTestHostQueue(opts)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, statusOpen, queue.status)

	// Prepare callback for aggregates
	var (
		results []hostQueueResult
		wg      sync.WaitGroup
	)
	callback := func(r interface{}, err error) {
		results = append(results, hostQueueResult{r, err})
		wg.Done()
	}

	// Prepare aggregate op
	aggregateOp := testAggregateOp("testNs", callback)
	wg.Add(1)

	// Prepare mocks for flush
	mockClient := rpc.NewMockTChanNode(ctrl)
	if testOpts != nil && testOpts.nextClientErr != nil {
		mockConnPool.EXPECT().NextClient().Return(nil, nil, testOpts.nextClientErr)
	} else if testOpts != nil && testOpts.aggregateErr != nil {
		aggregateExec := func(ctx thrift.Context, req *rpc.AggregateQueryRawRequest) {
			require.NotNil(t, req)
			assert.Equal(t, aggregateOp.request, *req)
		}
		mockClient.EXPECT().
			AggregateRaw(gomock.Any(), gomock.Any()).
			Do(aggregateExec).
			Return(nil, testOpts.aggregateErr)

		mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil)
	} else {
		aggregateExec := func(ctx thrift.Context, req *rpc.AggregateQueryRawRequest) {
			require.NotNil(t, req)
			assert.Equal(t, aggregateOp.request, *req)
		}
		mockClient.EXPECT().
			AggregateRaw(gomock.Any(), gomock.Any()).
			Do(aggregateExec).
			Return(result, nil)

		mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil)
	}

	// Fetch
	assert.NoError(t, queue.Enqueue(aggregateOp))

	// Wait for aggregate to complete
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

func testAggregateOp(
	namespace string,
	completionFn completionFn,
) *aggregateOp {
	f := newAggregateOp(nil)
	f.incRef()
	f.request = rpc.AggregateQueryRawRequest{
		NameSpace: []byte(namespace),
	}
	f.completionFn = completionFn
	return f
}
