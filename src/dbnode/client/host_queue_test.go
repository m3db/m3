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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/middleware"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	smallPoolOptions = pool.NewObjectPoolOptions().SetSize(1)

	testWriteBatchRawPool   writeBatchRawRequestPool
	testWriteBatchRawV2Pool writeBatchRawV2RequestPool

	testWriteArrayPool   writeBatchRawRequestElementArrayPool
	testWriteV2ArrayPool writeBatchRawV2RequestElementArrayPool

	testWriteTaggedBatchRawPool   writeTaggedBatchRawRequestPool
	testWriteTaggedBatchRawV2Pool writeTaggedBatchRawV2RequestPool

	testWriteTaggedArrayPool   writeTaggedBatchRawRequestElementArrayPool
	testWriteTaggedV2ArrayPool writeTaggedBatchRawV2RequestElementArrayPool

	testFetchBatchRawV2Pool      fetchBatchRawV2RequestPool
	testFetchBatchRawV2ArrayPool fetchBatchRawV2RequestElementArrayPool
)

func init() {
	testWriteBatchRawPool = newWriteBatchRawRequestPool(smallPoolOptions)
	testWriteBatchRawPool.Init()
	testWriteBatchRawV2Pool = newWriteBatchRawV2RequestPool(smallPoolOptions)
	testWriteBatchRawV2Pool.Init()

	testWriteArrayPool = newWriteBatchRawRequestElementArrayPool(smallPoolOptions, 0)
	testWriteArrayPool.Init()
	testWriteV2ArrayPool = newWriteBatchRawV2RequestElementArrayPool(smallPoolOptions, 0)
	testWriteV2ArrayPool.Init()

	testWriteTaggedBatchRawPool = newWriteTaggedBatchRawRequestPool(smallPoolOptions)
	testWriteTaggedBatchRawPool.Init()
	testWriteTaggedBatchRawV2Pool = newWriteTaggedBatchRawV2RequestPool(smallPoolOptions)
	testWriteTaggedBatchRawV2Pool.Init()

	testWriteTaggedArrayPool = newWriteTaggedBatchRawRequestElementArrayPool(smallPoolOptions, 0)
	testWriteTaggedArrayPool.Init()
	testWriteTaggedV2ArrayPool = newWriteTaggedBatchRawV2RequestElementArrayPool(smallPoolOptions, 0)
	testWriteTaggedV2ArrayPool.Init()

	testFetchBatchRawV2Pool = newFetchBatchRawV2RequestPool(smallPoolOptions)
	testFetchBatchRawV2Pool.Init()
	testFetchBatchRawV2ArrayPool = newFetchBatchRawV2RequestElementArrayPool(smallPoolOptions, 0)
	testFetchBatchRawV2ArrayPool.Init()
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

func TestNewHostQueueWithCircuitBreaker(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name           string
		enableProvider *testEnableProvider
		expectError    bool
	}{
		{
			name:           "enabled middleware",
			enableProvider: &testEnableProvider{enabled: true},
			expectError:    false,
		},
		{
			name:           "disabled middleware",
			enableProvider: &testEnableProvider{enabled: false},
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewMockOptions(ctrl)
			opts.EXPECT().MiddlewareEnableProvider().Return(tt.enableProvider).AnyTimes()
			opts.EXPECT().InstrumentOptions().Return(instrument.NewOptions()).AnyTimes()
			opts.EXPECT().ChannelOptions().Return(nil).AnyTimes()

			// Test middleware initialization
			cbMiddleware, err := middleware.New(
				opts.MiddlewareCircuitbreakerConfig(),
				opts.InstrumentOptions().Logger(),
				opts.InstrumentOptions().MetricsScope(),
				"test-host",
				tt.enableProvider,
			)

			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Test connection wrapping
			wrappedNewConnFn := func(channelName string, address string, clientOpts Options) (Channel, rpc.TChanNode, error) {
				channel, client, err := defaultNewConnectionFn(channelName, address, clientOpts)
				if err != nil {
					return nil, nil, err
				}
				return channel, cbMiddleware(client), nil
			}

			// Test the wrapped function
			channel, client, err := wrappedNewConnFn("test-channel", "test-address", opts)
			assert.NoError(t, err)
			assert.NotNil(t, channel)
			assert.NotNil(t, client)
			assert.Implements(t, (*rpc.TChanNode)(nil), client)
		})
	}
}
