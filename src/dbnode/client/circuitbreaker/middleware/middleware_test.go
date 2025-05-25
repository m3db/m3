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

package middleware

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
)

// newTestConfig creates a common test configuration for middleware tests
func newTestConfig(enabled, shadowMode bool) Config {
	return Config{
		Enabled:    enabled,
		ShadowMode: shadowMode,
		CircuitBreakerConfig: circuitbreaker.Config{
			MinimumRequests:      1,
			FailureRatio:         0.1,
			MinimumProbeRequests: 0,
			WindowSize:           1,
			BucketDuration:       time.Millisecond,
		},
	}
}

func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name        string
		params      Params
		expectError bool
	}{
		{
			name: "valid params",
			params: Params{
				Config: Config{
					Enabled: true,
					CircuitBreakerConfig: circuitbreaker.Config{
						MinimumRequests:      1,
						FailureRatio:         0.1,
						MinimumProbeRequests: 0,
						WindowSize:           1,
						BucketDuration:       time.Millisecond,
					},
				},
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			expectError: false,
		},
		{
			name: "invalid circuit breaker config",
			params: Params{
				Config: Config{
					Enabled: true,
					CircuitBreakerConfig: circuitbreaker.Config{
						MinimumRequests:      -1, // Invalid config
						FailureRatio:         0.1,
						MinimumProbeRequests: 0,
						WindowSize:           1,
						BucketDuration:       time.Millisecond,
					},
				},
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middlewareFn, err := New(tt.params)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Test that the middleware function returns a client
			mockNode := rpc.NewMockTChanNode(ctrl)
			client := middlewareFn(mockNode)
			assert.NotNil(t, client)
			assert.Implements(t, (*rpc.TChanNode)(nil), client)
		})
	}
}

func TestClient_WriteBatchRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name          string
		params        Params
		mockBehavior  func(*rpc.MockTChanNode)
		verifyState   func(*testing.T, *client)
		expectedError bool
	}{
		{
			name: "circuit breaker enabled - successful write",
			params: Params{
				Config: newTestConfig(true, false),
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			mockBehavior: func(mockNode *rpc.MockTChanNode) {
				// Expect two successful writes
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(nil).Times(2)
			},
			verifyState: func(t *testing.T, c *client) {
				assert.True(t, c.enabled)
				assert.NotNil(t, c.circuit)
				assert.Equal(t, circuitbreaker.Healthy, c.circuit.Status().State())
			},
			expectedError: false,
		},
		{
			name: "circuit breaker enabled - failed write transitions to unhealthy",
			params: Params{
				Config: newTestConfig(true, false),
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			mockBehavior: func(mockNode *rpc.MockTChanNode) {
				// First request fails
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(errors.New("write error"))
				// Second request should be rejected by circuit breaker
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Times(0)
			},
			verifyState: func(t *testing.T, c *client) {
				assert.True(t, c.enabled)
				assert.NotNil(t, c.circuit)
				assert.Equal(t, circuitbreaker.Unhealthy, c.circuit.Status().State())
			},
			expectedError: true,
		},
		{
			name: "circuit breaker disabled - requests pass through",
			params: Params{
				Config: newTestConfig(false, false),
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			mockBehavior: func(mockNode *rpc.MockTChanNode) {
				// Both requests should go through when disabled
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(nil).Times(2)
			},
			verifyState: func(t *testing.T, c *client) {
				assert.False(t, c.enabled)
				assert.NotNil(t, c.circuit)
				assert.Equal(t, circuitbreaker.Healthy, c.circuit.Status().State())
			},
			expectedError: false,
		},
		{
			name: "circuit breaker enabled - unhealthy state rejects requests",
			params: Params{
				Config: Config{
					Enabled:    true,
					ShadowMode: false,
					CircuitBreakerConfig: circuitbreaker.Config{
						MinimumRequests: 1,
						FailureRatio:    0.1,
						WindowSize:      1,
						BucketDuration:  time.Millisecond,
					},
				},
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			mockBehavior: func(mockNode *rpc.MockTChanNode) {
				// First request fails to trigger unhealthy state
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(errors.New("write error"))
				// Second request should be rejected by circuit breaker
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Times(0)
			},
			verifyState: func(t *testing.T, c *client) {
				assert.True(t, c.enabled)
				assert.NotNil(t, c.circuit)
				assert.Equal(t, circuitbreaker.Unhealthy, c.circuit.Status().State())
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middlewareFn, err := New(tt.params)
			require.NoError(t, err)

			mockNode := rpc.NewMockTChanNode(ctrl)
			tt.mockBehavior(mockNode)

			clientInterface := middlewareFn(mockNode)
			ctx, cancel := thrift.NewContext(time.Second)
			defer cancel()

			// First request to potentially trigger unhealthy state
			node, ok := clientInterface.(rpc.TChanNode)
			require.True(t, ok, "Client must implement rpc.TChanNode")
			err = node.WriteBatchRaw(ctx, &rpc.WriteBatchRawRequest{})
			if tt.expectedError {
				assert.Error(t, err)
				if err != nil {
					assert.Contains(t, err.Error(), "write error")
				}
			} else {
				assert.NoError(t, err)
			}

			// Verify circuit breaker state
			clientImpl, ok := clientInterface.(*client)
			require.True(t, ok, "Client must be of type *client")
			tt.verifyState(t, clientImpl)

			// Second request to test circuit breaker behavior
			err = node.WriteBatchRaw(ctx, &rpc.WriteBatchRawRequest{})
			if tt.expectedError {
				assert.Error(t, err)
				if err != nil {
					if clientImpl.circuit.Status().State() == circuitbreaker.Unhealthy {
						assert.Contains(t, err.Error(), "circuit breaker")
					} else {
						assert.Contains(t, err.Error(), "write error")
					}
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestClient_ShadowMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name          string
		params        Params
		mockBehavior  func(*rpc.MockTChanNode)
		expectedError bool
	}{
		{
			name: "shadow mode enabled - request goes through",
			params: Params{
				Config: newTestConfig(true, true),
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			mockBehavior: func(mockNode *rpc.MockTChanNode) {
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(nil)
			},
			expectedError: false,
		},
		{
			name: "shadow mode enabled - request fails",
			params: Params{
				Config: newTestConfig(true, true),
				Logger: zap.NewNop(),
				Scope:  tally.NoopScope,
				Host:   "test-host",
			},
			mockBehavior: func(mockNode *rpc.MockTChanNode) {
				mockNode.EXPECT().WriteBatchRaw(gomock.Any(), gomock.Any()).Return(errors.New("write error"))
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middlewareFn, err := New(tt.params)
			require.NoError(t, err)

			mockNode := rpc.NewMockTChanNode(ctrl)
			tt.mockBehavior(mockNode)

			client := middlewareFn(mockNode)
			ctx, cancel := thrift.NewContext(time.Second)
			defer cancel()

			err = client.WriteBatchRaw(ctx, &rpc.WriteBatchRawRequest{})
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
