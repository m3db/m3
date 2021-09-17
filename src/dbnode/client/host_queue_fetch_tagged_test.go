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

package client

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/thrift"
)

func TestHostQueueDrainOnCloseFetchTagged(t *testing.T) {
	ctrl := gomock.NewController(t)
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

	// Prepare fetches
	fetch := testFetchTaggedOp("testNs", callback)

	mockClient := rpc.NewMockTChanNode(ctrl)
	fetchTagged := func(ctx thrift.Context, req *rpc.FetchTaggedRequest) {
		assert.Equal(t, fetch.request.NameSpace, req.NameSpace)
	}
	mockClient.EXPECT().FetchTagged(gomock.Any(), gomock.Any()).Do(fetchTagged).Return(nil, nil)
	mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, true, nil)
	mockConnPool.EXPECT().Close().AnyTimes()

	// Execute fetch
	wg.Add(1)
	assert.NoError(t, queue.Enqueue(fetch))

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

	// Assert fetches successful
	assert.Equal(t, 1, len(results))
	for _, result := range results {
		assert.Nil(t, result.err)
	}
}

func TestHostQueueFetchTagged(t *testing.T) {
	namespace := "testNs"
	res := &rpc.FetchTaggedResult_{
		Elements: []*rpc.FetchTaggedIDResult_{
			{
				NameSpace: []byte(namespace),
				ID:        []byte("abc"),
			},
		},
		Exhaustive: true,
	}
	expectedResults := []hostQueueResult{
		{
			result: fetchTaggedResultAccumulatorOpts{
				response: res,
				host:     h,
			},
		},
	}
	testHostQueueFetchTagged(t, res, nil, func(results []hostQueueResult) {
		assert.Equal(t, expectedResults, results)
	})
}

func TestHostQueueFetchTaggedErrorOnNextClientNotBootstrapped(t *testing.T) {
	expectedResults := []hostQueueResult{
		{
			result: fetchTaggedResultAccumulatorOpts{
				host: h,
			},
			err: errNodeNotBootstrapped,
		},
	}
	opts := &testHostQueueFetchTaggedOptions{
		bootstrapped: false,
	}
	testHostQueueFetchTagged(t, nil, opts, func(results []hostQueueResult) {
		assert.Equal(t, expectedResults, results)
	})
}

func TestHostQueueFetchTaggedErrorOnNextClientUnavailable(t *testing.T) {
	expectedErr := fmt.Errorf("an error")
	expectedResults := []hostQueueResult{
		{
			result: fetchTaggedResultAccumulatorOpts{
				host: h,
			},
			err: expectedErr,
		},
	}
	opts := &testHostQueueFetchTaggedOptions{
		nextClientErr: expectedErr,
	}
	testHostQueueFetchTagged(t, nil, opts, func(results []hostQueueResult) {
		assert.Equal(t, expectedResults, results)
	})
}

func TestHostQueueFetchTaggedErrorOnFetchTaggedError(t *testing.T) {
	expectedErr := fmt.Errorf("an error")
	expectedResults := []hostQueueResult{
		{
			result: fetchTaggedResultAccumulatorOpts{host: h},
			err:    expectedErr,
		},
	}
	opts := &testHostQueueFetchTaggedOptions{
		fetchTaggedErr: expectedErr,
	}
	testHostQueueFetchTagged(t, nil, opts, func(results []hostQueueResult) {
		assert.Equal(t, expectedResults, results)
	})
}

type testHostQueueFetchTaggedOptions struct {
	nextClientErr  error
	fetchTaggedErr error
	bootstrapped   bool
}

func testHostQueueFetchTagged(
	t *testing.T,
	result *rpc.FetchTaggedResult_,
	testOpts *testHostQueueFetchTaggedOptions,
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
	fetchTagged := testFetchTaggedOp("testNs", callback)
	wg.Add(1)

	// Prepare mocks for flush
	mockClient := rpc.NewMockTChanNode(ctrl)
	if testOpts != nil && testOpts.nextClientErr != nil {
		mockConnPool.EXPECT().NextClient().Return(nil, nil, false, testOpts.nextClientErr)
	} else if testOpts != nil && testOpts.fetchTaggedErr != nil {
		fetchTaggedExec := func(ctx thrift.Context, req *rpc.FetchTaggedRequest) {
			require.NotNil(t, req)
			assert.Equal(t, fetchTagged.request, *req)
		}
		mockClient.EXPECT().
			FetchTagged(gomock.Any(), gomock.Any()).
			Do(fetchTaggedExec).
			Return(nil, testOpts.fetchTaggedErr)

		mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, true, nil)
	} else if testOpts != nil && !testOpts.bootstrapped {
		mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, false, nil)
	} else {
		fetchTaggedExec := func(ctx thrift.Context, req *rpc.FetchTaggedRequest) {
			require.NotNil(t, req)
			assert.Equal(t, fetchTagged.request, *req)
		}
		mockClient.EXPECT().
			FetchTagged(gomock.Any(), gomock.Any()).
			Do(fetchTaggedExec).
			Return(result, nil)

		mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, true, nil)
	}

	// Fetch
	assert.NoError(t, queue.Enqueue(fetchTagged))

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

func testFetchTaggedOp(
	namespace string,
	completionFn completionFn,
) *fetchTaggedOp {
	f := newFetchTaggedOp(nil)
	f.incRef()
	f.context = testContext()
	f.request = rpc.FetchTaggedRequest{
		NameSpace: []byte(namespace),
	}
	f.completionFn = completionFn
	return f
}
