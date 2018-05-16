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

	"github.com/m3db/m3db/src/dbnode/generated/thrift/rpc"
	"github.com/uber/tchannel-go/thrift"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestHostQueueFetchBatches(t *testing.T) {
	namespace := "testNs"
	ids := []string{"foo", "bar", "baz", "qux"}
	result := &rpc.FetchBatchRawResult_{}
	for range ids {
		result.Elements = append(result.Elements, &rpc.FetchRawResult_{Segments: []*rpc.Segments{}})
	}
	var expected []hostQueueResult
	for i := range ids {
		expected = append(expected, hostQueueResult{result.Elements[i].Segments, nil})
	}
	testHostQueueFetchBatches(t, namespace, ids, result, expected, nil, func(results []hostQueueResult) {
		assert.Equal(t, expected, results)
	})
}

func TestHostQueueFetchBatchesErrorOnNextClientUnavailable(t *testing.T) {
	namespace := "testNs"
	ids := []string{"foo", "bar", "baz", "qux"}
	expectedErr := fmt.Errorf("an error")
	var expected []hostQueueResult
	for range ids {
		expected = append(expected, hostQueueResult{nil, expectedErr})
	}
	opts := &testHostQueueFetchBatchesOptions{
		nextClientErr: expectedErr,
	}
	testHostQueueFetchBatches(t, namespace, ids, nil, expected, opts, func(results []hostQueueResult) {
		assert.Equal(t, expected, results)
	})
}

func TestHostQueueFetchBatchesErrorOnFetchRawBatchError(t *testing.T) {
	namespace := "testNs"
	ids := []string{"foo", "bar", "baz", "qux"}
	expectedErr := fmt.Errorf("an error")
	var expected []hostQueueResult
	for range ids {
		expected = append(expected, hostQueueResult{nil, expectedErr})
	}
	opts := &testHostQueueFetchBatchesOptions{
		fetchRawBatchErr: expectedErr,
	}
	testHostQueueFetchBatches(t, namespace, ids, nil, expected, opts, func(results []hostQueueResult) {
		assert.Equal(t, expected, results)
	})
}

func TestHostQueueFetchBatchesErrorOnFetchNoResponse(t *testing.T) {
	namespace := "testNs"
	ids := []string{"foo", "bar", "baz", "qux"}
	result := &rpc.FetchBatchRawResult_{}
	for range ids[:len(ids)-1] {
		result.Elements = append(result.Elements, &rpc.FetchRawResult_{Segments: []*rpc.Segments{}})
	}
	var expected []hostQueueResult
	for i := range ids[:len(ids)-1] {
		expected = append(expected, hostQueueResult{result.Elements[i].Segments, nil})
	}

	testHostQueueFetchBatches(t, namespace, ids, result, expected, nil, func(results []hostQueueResult) {
		assert.Equal(t, expected, results[:len(results)-1])
		lastResult := results[len(results)-1]
		assert.Nil(t, lastResult.result)
		assert.IsType(t, errQueueFetchNoResponse(""), lastResult.err)
	})
}

func TestHostQueueFetchBatchesErrorOnResultError(t *testing.T) {
	namespace := "testNs"
	ids := []string{"foo", "bar", "baz", "qux"}
	anError := &rpc.Error{Type: rpc.ErrorType_INTERNAL_ERROR, Message: "an error"}
	result := &rpc.FetchBatchRawResult_{}
	for range ids[:len(ids)-1] {
		result.Elements = append(result.Elements, &rpc.FetchRawResult_{Segments: []*rpc.Segments{}})
	}
	result.Elements = append(result.Elements, &rpc.FetchRawResult_{Err: anError})
	var expected []hostQueueResult
	for i := range ids[:len(ids)-1] {
		expected = append(expected, hostQueueResult{result.Elements[i].Segments, nil})
	}
	testHostQueueFetchBatches(t, namespace, ids, result, expected, nil, func(results []hostQueueResult) {
		assert.Equal(t, expected, results[:len(results)-1])
		rpcErr, ok := results[len(results)-1].err.(*rpc.Error)
		assert.True(t, ok)
		assert.Equal(t, anError.Type, rpcErr.Type)
		assert.Equal(t, anError.Message, rpcErr.Message)
	})
}

type testHostQueueFetchBatchesOptions struct {
	nextClientErr    error
	fetchRawBatchErr error
}

func testHostQueueFetchBatches(
	t *testing.T,
	namespace string,
	ids []string,
	result *rpc.FetchBatchRawResult_,
	expected []hostQueueResult,
	testOpts *testHostQueueFetchBatchesOptions,
	assertion func(results []hostQueueResult),
) {
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

	// Prepare callback for fetches
	var (
		results []hostQueueResult
		wg      sync.WaitGroup
	)
	callback := func(r interface{}, err error) {
		results = append(results, hostQueueResult{r, err})
		wg.Done()
	}

	rawIDs := make([][]byte, len(ids))

	for i, id := range ids {
		rawIDs[i] = []byte(id)
	}

	// Prepare fetch batch op
	fetchBatch := &fetchBatchOp{
		request: rpc.FetchBatchRawRequest{
			RangeStart: 0,
			RangeEnd:   1,
			NameSpace:  []byte(namespace),
			Ids:        rawIDs,
		},
	}
	for range fetchBatch.request.Ids {
		fetchBatch.completionFns = append(fetchBatch.completionFns, callback)
	}
	wg.Add(len(fetchBatch.request.Ids))

	// Prepare mocks for flush
	mockClient := rpc.NewMockTChanNode(ctrl)
	if testOpts != nil && testOpts.nextClientErr != nil {
		mockConnPool.EXPECT().NextClient().Return(nil, testOpts.nextClientErr)
	} else if testOpts != nil && testOpts.fetchRawBatchErr != nil {
		fetchBatchRaw := func(ctx thrift.Context, req *rpc.FetchBatchRawRequest) {
			assert.Equal(t, &fetchBatch.request, req)
		}
		mockClient.EXPECT().
			FetchBatchRaw(gomock.Any(), gomock.Any()).
			Do(fetchBatchRaw).
			Return(nil, testOpts.fetchRawBatchErr)

		mockConnPool.EXPECT().NextClient().Return(mockClient, nil)
	} else {
		fetchBatchRaw := func(ctx thrift.Context, req *rpc.FetchBatchRawRequest) {
			assert.Equal(t, &fetchBatch.request, req)
		}
		mockClient.EXPECT().
			FetchBatchRaw(gomock.Any(), gomock.Any()).
			Do(fetchBatchRaw).
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
