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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go/thrift"
)

func TestHostQueueWriteTaggedErrorBeforeOpen(t *testing.T) {
	opts := newHostQueueTestOptions()
	queue := newTestHostQueue(opts)
	assert.Error(t, queue.Enqueue(&writeTaggedOperation{}))
}

func TestHostQueueWriteTaggedErrorAfterClose(t *testing.T) {
	opts := newHostQueueTestOptions()
	queue := newTestHostQueue(opts)
	queue.Open()
	queue.Close()

	assert.Error(t, queue.Enqueue(&writeTaggedOperation{}))
}

func TestHostQueueWriteTaggedBatches(t *testing.T) {
	for _, opts := range []Options{
		newHostQueueTestOptions().SetUseV2BatchAPIs(false),
		newHostQueueTestOptions().SetUseV2BatchAPIs(true),
	} {
		t.Run(fmt.Sprintf("useV2: %v", opts.UseV2BatchAPIs()), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockConnPool := NewMockconnectionPool(ctrl)

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

			// Prepare writes
			writes := []*writeTaggedOperation{
				testWriteTaggedOp("testNs", "foo", map[string]string{
					"tag": "value",
					"sup": "holmes",
				}, 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback),
				testWriteTaggedOp("testNs", "bar", map[string]string{
					"and": "one",
				}, 2.0, 2000, rpc.TimeType_UNIX_SECONDS, callback),
				testWriteTaggedOp("testNs", "baz", map[string]string{
					"cmon": "dawg",
					"mmm":  "kay",
				}, 3.0, 3000, rpc.TimeType_UNIX_SECONDS, callback),
				testWriteTaggedOp("testNs", "qux", map[string]string{
					"mas": "ter",
					"sal": "vo",
				}, 4.0, 4000, rpc.TimeType_UNIX_SECONDS, callback),
			}
			wg.Add(len(writes))

			for i, write := range writes[:3] {
				assert.NoError(t, queue.Enqueue(write))
				assert.Equal(t, i+1, queue.Len())

				// Sleep some so that we can ensure flushing is not happening until queue is full
				time.Sleep(20 * time.Millisecond)
			}

			// Prepare mocks for flush
			mockClient := rpc.NewMockTChanNode(ctrl)
			if opts.UseV2BatchAPIs() {
				writeBatch := func(ctx thrift.Context, req *rpc.WriteTaggedBatchRawV2Request) {
					for i, write := range writes {
						assert.Equal(t, req.Elements[i].NameSpace, int64(0))
						assert.Equal(t, req.Elements[i].ID, write.request.ID)
						assert.Equal(t, req.Elements[i].Datapoint, write.request.Datapoint)
						assert.Equal(t, req.Elements[i].EncodedTags, write.request.EncodedTags)
					}
				}
				mockClient.EXPECT().WriteTaggedBatchRawV2(gomock.Any(), gomock.Any()).Do(writeBatch).Return(nil)
			} else {
				writeBatch := func(ctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) {
					for i, write := range writes {
						assert.Equal(t, req.Elements[i].ID, write.request.ID)
						assert.Equal(t, req.Elements[i].Datapoint, write.request.Datapoint)
						assert.Equal(t, req.Elements[i].EncodedTags, write.request.EncodedTags)
					}
				}
				mockClient.EXPECT().WriteTaggedBatchRaw(gomock.Any(), gomock.Any()).Do(writeBatch).Return(nil)
			}
			mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil)

			// Final write will flush
			assert.NoError(t, queue.Enqueue(writes[3]))
			assert.Equal(t, 0, queue.Len())

			// Wait for all writes
			wg.Wait()

			// Assert writes successful
			assert.Equal(t, len(writes), len(results))
			for _, result := range results {
				assert.Nil(t, result.err)
			}

			// Close
			var closeWg sync.WaitGroup
			closeWg.Add(1)
			mockConnPool.EXPECT().Close().Do(func() {
				closeWg.Done()
			})
			queue.Close()
			closeWg.Wait()
		})
	}
}

func TestHostQueueWriteTaggedBatchesDifferentNamespaces(t *testing.T) {
	for _, opts := range []Options{
		newHostQueueTestOptions().SetUseV2BatchAPIs(false),
		newHostQueueTestOptions().SetUseV2BatchAPIs(true),
	} {
		t.Run(fmt.Sprintf("useV2: %v", opts.UseV2BatchAPIs()), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockConnPool := NewMockconnectionPool(ctrl)

			queue := newTestHostQueue(opts)
			queue.connPool = mockConnPool

			// Open
			mockConnPool.EXPECT().Open()
			queue.Open()
			assert.Equal(t, statusOpen, queue.status)

			// Prepare callback for writes
			var (
				results     []hostQueueResult
				resultsLock sync.Mutex
				wg          sync.WaitGroup
			)
			callback := func(r interface{}, err error) {
				resultsLock.Lock()
				results = append(results, hostQueueResult{r, err})
				resultsLock.Unlock()
				wg.Done()
			}

			// Prepare writes
			writes := []*writeTaggedOperation{
				testWriteTaggedOp("testNs1", "foo", map[string]string{
					"tag": "value",
					"sup": "holmes",
				}, 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback),
				testWriteTaggedOp("testNs1", "bar", map[string]string{
					"and": "one",
				}, 2.0, 2000, rpc.TimeType_UNIX_SECONDS, callback),
				testWriteTaggedOp("testNs1", "baz", map[string]string{
					"cmon": "dawg",
					"mmm":  "kay",
				}, 3.0, 3000, rpc.TimeType_UNIX_SECONDS, callback),
				testWriteTaggedOp("testNs2", "qux", map[string]string{
					"mas": "ter",
					"sal": "vo",
				}, 4.0, 4000, rpc.TimeType_UNIX_SECONDS, callback),
			}
			wg.Add(len(writes))

			// Prepare mocks for flush
			mockClient := rpc.NewMockTChanNode(ctrl)
			if opts.UseV2BatchAPIs() {
				writeBatch := func(ctx thrift.Context, req *rpc.WriteTaggedBatchRawV2Request) {
					for i, write := range writes {
						if i < 3 {
							assert.Equal(t, req.Elements[i].NameSpace, int64(0))
						} else {
							assert.Equal(t, req.Elements[i].NameSpace, int64(1))
						}
						assert.Equal(t, req.Elements[i].ID, write.request.ID)
						assert.Equal(t, req.Elements[i].Datapoint, write.request.Datapoint)
						assert.Equal(t, req.Elements[i].EncodedTags, write.request.EncodedTags)
					}
				}
				// Assert the writes will be handled in two batches.
				mockClient.EXPECT().WriteTaggedBatchRawV2(gomock.Any(), gomock.Any()).Do(writeBatch).Return(nil).Times(1)
				mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil).Times(1)
			} else {
				writeBatch := func(ctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) {
					var writesForNamespace []*writeTaggedOperation
					if string(req.NameSpace) == "testNs1" {
						writesForNamespace = writes[:3]
					} else {
						writesForNamespace = writes[3:]
					}
					assert.Equal(t, len(writesForNamespace), len(req.Elements))
					for i, write := range writesForNamespace {
						assert.Equal(t, req.Elements[i].ID, write.request.ID)
						assert.Equal(t, req.Elements[i].Datapoint, write.request.Datapoint)
						assert.Equal(t, req.Elements[i].EncodedTags, write.request.EncodedTags)
					}
				}
				// Assert the writes will be handled in two batches.
				mockClient.EXPECT().WriteTaggedBatchRaw(gomock.Any(), gomock.Any()).Do(writeBatch).Return(nil).Times(2)
				mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil).Times(2)
			}
			for _, write := range writes {
				assert.NoError(t, queue.Enqueue(write))
			}

			// Wait for all writes
			wg.Wait()

			// Assert writes successful
			assert.Equal(t, len(writes), len(results))
			for _, result := range results {
				assert.Nil(t, result.err)
			}

			// Close
			var closeWg sync.WaitGroup
			closeWg.Add(1)
			mockConnPool.EXPECT().Close().Do(func() {
				closeWg.Done()
			})
			queue.Close()
			closeWg.Wait()
		})
	}
}

func TestHostQueueWriteTaggedBatchesNoClientAvailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	opts = opts.SetHostQueueOpsFlushInterval(time.Millisecond)
	queue := newTestHostQueue(opts)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, statusOpen, queue.status)

	// Prepare mocks for flush
	nextClientErr := fmt.Errorf("an error")
	mockConnPool.EXPECT().NextClient().Return(nil, nil, nextClientErr)

	// Write
	var wg sync.WaitGroup
	wg.Add(1)
	callback := func(r interface{}, err error) {
		assert.Error(t, err)
		assert.Equal(t, nextClientErr, err)
		wg.Done()
	}
	assert.NoError(t, queue.Enqueue(testWriteTaggedOp("testNs", "foo", nil, 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback)))

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

func TestHostQueueWriteTaggedBatchesPartialBatchErrs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	opts = opts.SetHostQueueOpsFlushSize(2)
	queue := newTestHostQueue(opts)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, statusOpen, queue.status)

	// Prepare writes
	var wg sync.WaitGroup
	writeErr := "a write error"
	writes := []*writeTaggedOperation{
		testWriteTaggedOp("testNs", "foo", map[string]string{
			"mmm": "kay",
		}, 1.0, 1000, rpc.TimeType_UNIX_SECONDS, func(r interface{}, err error) {
			assert.Error(t, err)
			rpcErr, ok := err.(*rpc.Error)
			assert.True(t, ok)
			assert.Equal(t, rpc.ErrorType_INTERNAL_ERROR, rpcErr.Type)
			assert.Equal(t, writeErr, rpcErr.Message)
			wg.Done()
		}),
		testWriteTaggedOp("testNs", "bar", map[string]string{
			"who": "dat",
		}, 2.0, 2000, rpc.TimeType_UNIX_SECONDS, func(r interface{}, err error) {
			assert.NoError(t, err)
			wg.Done()
		}),
	}
	wg.Add(len(writes))

	// Prepare mocks for flush
	mockClient := rpc.NewMockTChanNode(ctrl)
	writeBatch := func(ctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) {
		for i, write := range writes {
			assert.Equal(t, req.Elements[i].ID, write.request.ID)
			assert.Equal(t, req.Elements[i].Datapoint, write.request.Datapoint)
			assert.Equal(t, req.Elements[i].EncodedTags, write.request.EncodedTags)
		}
	}
	batchErrs := &rpc.WriteBatchRawErrors{Errors: []*rpc.WriteBatchRawError{
		&rpc.WriteBatchRawError{Index: 0, Err: &rpc.Error{
			Type:    rpc.ErrorType_INTERNAL_ERROR,
			Message: writeErr,
		}},
	}}
	mockClient.EXPECT().WriteTaggedBatchRaw(gomock.Any(), gomock.Any()).Do(writeBatch).Return(batchErrs)
	mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil)

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

func TestHostQueueWriteTaggedBatchesEntireBatchErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnPool := NewMockconnectionPool(ctrl)

	opts := newHostQueueTestOptions()
	opts = opts.SetHostQueueOpsFlushSize(2)
	queue := newTestHostQueue(opts)
	queue.connPool = mockConnPool

	// Open
	mockConnPool.EXPECT().Open()
	queue.Open()
	assert.Equal(t, statusOpen, queue.status)

	// Prepare writes
	var wg sync.WaitGroup
	writeErr := fmt.Errorf("an error")
	callback := func(r interface{}, err error) {
		assert.Error(t, err)
		assert.Equal(t, writeErr, err)
		wg.Done()
	}
	writes := []*writeTaggedOperation{
		testWriteTaggedOp("testNs", "foo", map[string]string{"abc": "def"}, 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback),
		testWriteTaggedOp("testNs", "bar", map[string]string{"ghi": "klm"}, 2.0, 2000, rpc.TimeType_UNIX_SECONDS, callback),
	}
	wg.Add(len(writes))

	// Prepare mocks for flush
	mockClient := rpc.NewMockTChanNode(ctrl)
	writeBatch := func(ctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) {
		for i, write := range writes {
			assert.Equal(t, req.Elements[i].ID, write.request.ID)
			assert.Equal(t, req.Elements[i].Datapoint, write.request.Datapoint)
			assert.Equal(t, req.Elements[i].EncodedTags, write.request.EncodedTags)
		}
	}
	mockClient.EXPECT().WriteTaggedBatchRaw(gomock.Any(), gomock.Any()).Do(writeBatch).Return(writeErr)
	mockConnPool.EXPECT().NextClient().Return(mockClient, &noopPooledChannel{}, nil)

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

func TestHostQueueDrainOnCloseTaggedWrite(t *testing.T) {
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

	// Prepare writes
	writes := []*writeTaggedOperation{
		testWriteTaggedOp("testNs", "foo", map[string]string{"a": "b"}, 1.0, 1000, rpc.TimeType_UNIX_SECONDS, callback),
		testWriteTaggedOp("testNs", "bar", map[string]string{"k": "l"}, 2.0, 2000, rpc.TimeType_UNIX_SECONDS, callback),
		testWriteTaggedOp("testNs", "baz", map[string]string{"e": "f"}, 3.0, 3000, rpc.TimeType_UNIX_SECONDS, callback),
	}

	for i, write := range writes {
		wg.Add(1)
		assert.NoError(t, queue.Enqueue(write))
		assert.Equal(t, i+1, queue.Len())

		// Sleep some so that we can ensure flushing is not happening until queue is full
		time.Sleep(20 * time.Millisecond)
	}

	mockClient := rpc.NewMockTChanNode(ctrl)
	writeBatch := func(ctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) {
		for i, write := range writes {
			assert.Equal(t, req.Elements[i].ID, write.request.ID)
			assert.Equal(t, req.Elements[i].Datapoint, write.request.Datapoint)
			assert.Equal(t, req.Elements[i].EncodedTags, write.request.EncodedTags)
		}
	}
	mockClient.EXPECT().WriteTaggedBatchRaw(gomock.Any(), gomock.Any()).Do(writeBatch).Return(nil)

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

	// Assert writes successful
	assert.Equal(t, len(writes), len(results))
	for _, result := range results {
		assert.Nil(t, result.err)
	}
}

func testWriteTaggedOp(
	namespace string,
	id string,
	tags map[string]string,
	value float64,
	timestamp int64,
	timeType rpc.TimeType,
	completionFn completionFn,
) *writeTaggedOperation {
	w := &writeTaggedOperation{}
	w.reset()
	w.namespace = ident.StringID(namespace)
	w.request.ID = []byte(id)
	w.request.Datapoint = &rpc.Datapoint{
		Value:             value,
		Timestamp:         timestamp,
		TimestampTimeType: timeType,
	}
	w.request.EncodedTags = testEncode(tags)
	w.completionFn = completionFn
	w.requestV2.ID = w.request.ID
	w.requestV2.EncodedTags = w.request.EncodedTags
	w.requestV2.Datapoint = w.request.Datapoint
	return w
}

func testEncode(tags map[string]string) []byte {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b []byte
	for _, k := range keys {
		b = append(b, []byte(k)...)
		b = append(b, []byte("=")...)
		b = append(b, []byte(tags[k])...)
		b = append(b, []byte("|")...)
	}
	return b
}
