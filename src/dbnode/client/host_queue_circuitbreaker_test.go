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
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/middleware"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
)

const (
	circuitBreakerRejectMessage = "request rejected by circuit breaker of outbound-service"
)

type testEnableProvider struct {
	enabled    bool
	shadowMode bool
}

func (p *testEnableProvider) IsEnabled() bool {
	return p.enabled
}

func (p *testEnableProvider) WatchConfig(store kv.Store, logger *zap.Logger) error {
	return nil
}

func (p *testEnableProvider) IsShadowMode() bool {
	return p.shadowMode
}

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
		enableProvider       *testEnableProvider
		failCalls            bool
		expectedError        bool
		numCalls             int
	}{
		{
			name:                 "circuit_breaker_disabled_-_successful_calls",
			circuitBreakerConfig: middleware.Config{},
			enableProvider:       &testEnableProvider{enabled: false, shadowMode: false},
			failCalls:            false,
			expectedError:        false,
			numCalls:             5,
		},
		{
			name:                 "circuit_breaker_disabled_-_failed_calls",
			circuitBreakerConfig: middleware.Config{},
			enableProvider:       &testEnableProvider{enabled: false, shadowMode: false},
			failCalls:            true,
			expectedError:        true,
			numCalls:             5,
		},
		{
			name:                 "circuit_breaker_enabled_-_successful_calls",
			circuitBreakerConfig: middleware.Config{},
			enableProvider:       &testEnableProvider{enabled: true, shadowMode: false},
			failCalls:            false,
			expectedError:        false,
			numCalls:             5,
		},
		{
			name: "circuit_breaker_enabled_-_failed_calls",
			circuitBreakerConfig: middleware.Config{
				CircuitBreakerConfig: circuitbreaker.Config{
					MinimumRequests:      1,
					FailureRatio:         0.1,
					MinimumProbeRequests: 0,
					WindowSize:           1,
					BucketDuration:       time.Millisecond,
				},
			},
			enableProvider: &testEnableProvider{enabled: true, shadowMode: false},
			failCalls:      true,
			expectedError:  true,
			numCalls:       5,
		},
		{
			name: "circuit_breaker_enabled_shadow_mode_-_failed_calls",
			circuitBreakerConfig: middleware.Config{
				CircuitBreakerConfig: circuitbreaker.Config{
					MinimumRequests:      1,
					FailureRatio:         0.1,
					MinimumProbeRequests: 0,
					WindowSize:           1,
					BucketDuration:       time.Millisecond,
				},
			},
			enableProvider: &testEnableProvider{enabled: true, shadowMode: true},
			failCalls:      true,
			expectedError:  true,
			numCalls:       5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockNode := rpc.NewMockTChanNode(ctrl)

			opts := newHostQueueTestOptions()
			opts = opts.SetHostQueueOpsFlushSize(1).
				SetHostQueueOpsArrayPoolSize(1).
				SetWriteBatchSize(test.numCalls).
				SetMiddlewareCircuitbreakerConfig(test.circuitBreakerConfig).
				SetMiddlewareEnableProvider(test.enableProvider)

			hostQueue := newTestHostQueue(opts)
			mockConnPool := NewMockconnectionPool(ctrl)

			// Create middleware and wrap the mock node
			middlewareFn, err := middleware.New(middleware.Params{
				Config:         test.circuitBreakerConfig,
				Logger:         opts.InstrumentOptions().Logger(),
				Scope:          opts.InstrumentOptions().MetricsScope(),
				Host:           "test-host",
				EnableProvider: test.enableProvider,
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
				if test.enableProvider.IsEnabled() {
					if test.enableProvider.IsShadowMode() {
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

func TestHostQueueCircuitBreakerErrorFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cbConfig := middleware.Config{
		CircuitBreakerConfig: circuitbreaker.Config{
			MinimumRequests:      1,
			FailureRatio:         0.1,
			MinimumProbeRequests: 0,
			WindowSize:           1,
			BucketDuration:       time.Millisecond,
		},
	}

	tests := []struct {
		name                    string
		mockError               error
		expectCBRejectOnSecond  bool
		expectFirstWriteError   bool
	}{
		{
			name:                   "timeout error trips circuit breaker - second request rejected",
			mockError:              tchannel.ErrTimeout,
			expectCBRejectOnSecond: true,
			expectFirstWriteError:  true,
		},
		{
			name:                   "non-timeout error does NOT trip circuit breaker - second request passes",
			mockError:              errors.New("bad request error"),
			expectCBRejectOnSecond: false,
			expectFirstWriteError:  true,
		},
		{
			name:                   "successful write - second request passes",
			mockError:              nil,
			expectCBRejectOnSecond: false,
			expectFirstWriteError:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockNode := rpc.NewMockTChanNode(ctrl)

			enableProvider := &testEnableProvider{enabled: true, shadowMode: false}

			// Create middleware with IsTimeoutError as the error filter
			middlewareFn, err := middleware.New(middleware.Params{
				Config:         cbConfig,
				Logger:         zap.NewNop(),
				Scope:          tally.NoopScope,
				Host:           "test-host",
				EnableProvider: enableProvider,
				ErrorFilter:    IsTimeoutError,
			})
			require.NoError(t, err)

			// First call returns the configured error
			mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(test.mockError)
			if !test.expectCBRejectOnSecond {
				// If CB should NOT trip, second call should also reach the mock
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(nil)
			}

			wrappedNode := middlewareFn(mockNode)
			ctx, cancel := thrift.NewContext(time.Second)
			defer cancel()

			node := wrappedNode.(rpc.TChanNode)

			// First request
			err = node.WriteBatchRaw(ctx, &rpc.WriteBatchRawRequest{})
			if test.expectFirstWriteError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Second request — verifies whether CB tripped or not
			err = node.WriteBatchRaw(ctx, &rpc.WriteBatchRawRequest{})
			if test.expectCBRejectOnSecond {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), circuitBreakerRejectMessage),
					"expected circuit breaker rejection, got: %v", err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
