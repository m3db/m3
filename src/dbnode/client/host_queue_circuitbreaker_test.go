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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/middleware"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
)

const (
	circuitBreakerRejectMessage = "request rejected by circuit breaker of outbound-service"
)

func hasCircuitBreakerError(errs []error) bool {
	for _, err := range errs {
		if err != nil && strings.Contains(err.Error(), circuitBreakerRejectMessage) {
			return true
		}
	}
	return false
}

func TestHostQueueCircuitBreakerIntegration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name                 string
		circuitBreakerConfig middleware.Config
		failCalls            bool
		expectedError        bool
		numCalls             int
	}{
		{
			name:                 "circuit_breaker_disabled_-_successful_calls",
			circuitBreakerConfig: middleware.Config{},
			failCalls:            false,
			expectedError:        false,
			numCalls:             5,
		},
		{
			name:                 "circuit_breaker_disabled_-_failed_calls",
			circuitBreakerConfig: middleware.Config{},
			failCalls:            true,
			expectedError:        true,
			numCalls:             5,
		},
		{
			name: "circuit_breaker_enabled_-_successful_calls",
			circuitBreakerConfig: middleware.Config{
				Enabled:    true,
				ShadowMode: false,
			},
			failCalls:     false,
			expectedError: false,
			numCalls:      5,
		},
		{
			name: "circuit_breaker_enabled_-_failed_calls",
			circuitBreakerConfig: middleware.Config{
				Enabled:    true,
				ShadowMode: false,
				CircuitBreakerConfig: circuitbreaker.Config{
					MinimumRequests:      1,
					FailureRatio:         0.1,
					MinimumProbeRequests: 0,
					WindowSize:           1,
					BucketDuration:       time.Millisecond,
				},
			},
			failCalls:     true,
			expectedError: true,
			numCalls:      5,
		},
		{
			name: "circuit_breaker_enabled_shadow_mode_-_failed_calls",
			circuitBreakerConfig: middleware.Config{
				Enabled:    true,
				ShadowMode: true,
				CircuitBreakerConfig: circuitbreaker.Config{
					MinimumRequests:      1,
					FailureRatio:         0.1,
					MinimumProbeRequests: 0,
					WindowSize:           1,
					BucketDuration:       time.Millisecond,
				},
			},
			failCalls:     true,
			expectedError: true,
			numCalls:      5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockNode := rpc.NewMockTChanNode(ctrl)

			opts := newHostQueueTestOptions()
			opts = opts.SetHostQueueOpsFlushSize(1).
				SetHostQueueOpsArrayPoolSize(1).
				SetWriteBatchSize(test.numCalls).
				SetMiddlewareCircuitbreakerConfig(test.circuitBreakerConfig)

			hostQueue := newTestHostQueue(opts)
			mockConnPool := NewMockconnectionPool(ctrl)

			// Create middleware and wrap the mock node
			middlewareFn, err := middleware.New(middleware.Params{
				Config: test.circuitBreakerConfig,
				Logger: opts.InstrumentOptions().Logger(),
				Scope:  opts.InstrumentOptions().MetricsScope(),
				Host:   "test-host",
			})
			require.NoError(t, err)

			wrappedNode := middlewareFn(mockNode)

			// Set up expectations on the mock node
			if test.failCalls {
				// For failed calls, we expect the circuit breaker to eventually reject requests
				// So we only expect a few calls to reach the mock node before circuit opens
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).
					Return(errors.New("mock write error")).AnyTimes()
			} else {
				// For successful calls, we expect all calls to reach the mock node
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).
					Return(nil).
					Times(test.numCalls) // Expect all successes
			}

			mockConnPool.EXPECT().NextClient().Return(wrappedNode, nil, nil).Times(test.numCalls)
			hostQueue.connPool = mockConnPool

			mockConnPool.EXPECT().Open()
			mockConnPool.EXPECT().Close().AnyTimes()
			// Open the host queue
			hostQueue.Open()
			defer hostQueue.Close()

			// Create and enqueue the write operations
			var wg sync.WaitGroup
			wg.Add(test.numCalls)
			var actualErrs []error
			var mu sync.Mutex // Add mutex to protect actualErrs

			for i := 0; i < test.numCalls; i++ {
				writeOp := testWriteOp(
					"testnamespace",
					"testid",
					42.0,
					1234567890,
					rpc.TimeType_UNIX_SECONDS,
					func(result interface{}, err error) {
						mu.Lock()
						actualErrs = append(actualErrs, err)
						mu.Unlock()
						wg.Done()
					},
				)

				err := hostQueue.Enqueue(writeOp)
				require.NoError(t, err)
			}

			// Wait for all write operations to complete
			wg.Wait()

			// Check the result
			if test.expectedError {
				if test.circuitBreakerConfig.Enabled {
					if test.circuitBreakerConfig.ShadowMode {
						// In shadow mode, we should get normal errors, not circuit breaker errors
						assert.False(t, hasCircuitBreakerError(actualErrs), "Should not get circuit breaker errors in shadow mode")
						for _, err := range actualErrs {
							assert.Error(t, err)
							assert.Contains(t, err.Error(), "mock write error")
						}
					} else {
						assert.True(t, hasCircuitBreakerError(actualErrs), "Expected at least one circuit breaker rejection error")
					}
				}
			} else {
				for _, err := range actualErrs {
					assert.NoError(t, err)
				}
			}
		})
	}
}
