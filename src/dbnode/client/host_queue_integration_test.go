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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/middleware"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostQueueCircuitBreakerIntegration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name                 string
		circuitBreakerConfig middleware.Config
		failCalls            bool
		expectedError        bool
	}{
		{
			name:                 "circuit_breaker_disabled_-_successful_calls",
			circuitBreakerConfig: middleware.Config{},
			failCalls:            false,
			expectedError:        false,
		},
		{
			name:                 "circuit_breaker_disabled_-_failed_calls",
			circuitBreakerConfig: middleware.Config{},
			failCalls:            true,
			expectedError:        true,
		},
		{
			name: "circuit_breaker_enabled_-_successful_calls",
			circuitBreakerConfig: middleware.Config{
				Enabled:    true,
				ShadowMode: false,
				CircuitBreakerConfig: circuitbreaker.Config{
					WindowSize:           15,
					BucketDuration:       time.Second,
					RecoveryTime:         time.Second * 2,
					Jitter:               time.Second,
					MaxProbeTime:         time.Second * 25,
					MinimumRequests:      100,
					FailureRatio:         0.5,
					ProbeRatios:          []float64{0.05, 0.15, 0.30, 0.50, 0.75},
					MinimumProbeRequests: 25,
				},
			},
			failCalls:     false,
			expectedError: false,
		},
		{
			name: "circuit_breaker_enabled_-_failed_calls",
			circuitBreakerConfig: middleware.Config{
				Enabled:    true,
				ShadowMode: false,
				CircuitBreakerConfig: circuitbreaker.Config{
					WindowSize:           15,
					BucketDuration:       time.Second,
					RecoveryTime:         time.Second * 2,
					Jitter:               time.Second,
					MaxProbeTime:         time.Second * 25,
					MinimumRequests:      100,
					FailureRatio:         0.5,
					ProbeRatios:          []float64{0.05, 0.15, 0.30, 0.50, 0.75},
					MinimumProbeRequests: 25,
				},
			},
			failCalls:     true,
			expectedError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockNode := rpc.NewMockTChanNode(ctrl)
			if test.failCalls {
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(errors.New("mock write error"))
			} else {
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(nil)
			}

			opts := newHostQueueTestOptions()
			opts = opts.SetHostQueueOpsFlushSize(1).
				SetHostQueueOpsArrayPoolSize(1).
				SetWriteBatchSize(1).
				SetMiddlewareCircuitbreakerConfig(test.circuitBreakerConfig)

			// Create a wrapped connection function that uses our mock node
			wrappedConnFn := func(channelName string, address string, clientOpts Options) (Channel, rpc.TChanNode, error) {
				return nil, mockNode, nil
			}
			opts = opts.SetNewConnectionFn(wrappedConnFn)

			hostQueue := newTestHostQueue(opts)
			mockConnPool := NewMockconnectionPool(ctrl)
			mockConnPool.EXPECT().NextClient().Return(mockNode, nil, nil)
			hostQueue.connPool = mockConnPool

			mockConnPool.EXPECT().Open()
			mockConnPool.EXPECT().Close().AnyTimes()
			// Open the host queue
			hostQueue.Open()
			defer hostQueue.Close()

			// Create and enqueue the write operation
			var wg sync.WaitGroup
			wg.Add(1)
			var actualErr error

			writeOp := testWriteOp(
				"testnamespace",
				"testid",
				42.0,
				1234567890,
				rpc.TimeType_UNIX_SECONDS,
				func(result interface{}, err error) {
					actualErr = err
					wg.Done()
				},
			)

			err := hostQueue.Enqueue(writeOp)
			require.NoError(t, err)

			// Wait for the write operation to complete
			wg.Wait()

			// Check the result
			if test.expectedError {
				assert.Error(t, actualErr)
			} else {
				assert.NoError(t, actualErr)
			}
		})
	}
}
