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

	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"
)

type mockNode struct {
	rpc.TChanNode
	failNextCall bool
}

func (m *mockNode) WriteBatchRaw(ctx thrift.Context, req *rpc.WriteBatchRawRequest) error {
	if m.failNextCall {
		return errors.New("mock error")
	}
	return nil
}

func TestCircuitBreakerWriteBatchRawIntegration(t *testing.T) {
	logger := zap.NewNop()
	scope := tally.NoopScope

	tests := []struct {
		name           string
		config         Config
		failCalls      int
		expectedClosed bool
	}{
		{
			name: "circuit breaker opens after failures",
			config: Config{
				Enabled: true,
				CircuitBreakerConfig: circuitbreaker.Config{
					WindowSize:      10,
					BucketDuration:  time.Second,
					FailureRatio:    0.1,
					MinimumRequests: 1,
				},
			},
			failCalls:      3,
			expectedClosed: false,
		},
		{
			name: "circuit breaker stays closed with successful calls",
			config: Config{
				Enabled: true,
				CircuitBreakerConfig: circuitbreaker.Config{
					WindowSize:      10,
					BucketDuration:  time.Second,
					FailureRatio:    0.1,
					MinimumRequests: 1,
				},
			},
			failCalls:      0,
			expectedClosed: true,
		},
		{
			name: "circuit breaker disabled",
			config: Config{
				Enabled: false,
				CircuitBreakerConfig: circuitbreaker.Config{
					WindowSize:      10,
					BucketDuration:  time.Second,
					FailureRatio:    0.1,
					MinimumRequests: 1,
				},
			},
			failCalls:      3,
			expectedClosed: true,
		},
		{
			name: "circuit breaker in shadow mode",
			config: Config{
				Enabled:    true,
				ShadowMode: true,
				CircuitBreakerConfig: circuitbreaker.Config{
					WindowSize:      10,
					BucketDuration:  time.Second,
					FailureRatio:    0.1,
					MinimumRequests: 1,
				},
			},
			failCalls:      3,
			expectedClosed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create middleware
			cbMiddleware, err := New(tt.config, logger, scope, "test-host")
			require.NoError(t, err)

			// Create mock node that will fail calls
			mockNode := &mockNode{}
			wrappedNode := cbMiddleware(mockNode).(rpc.TChanNode)

			// Create a test request
			req := &rpc.WriteBatchRawRequest{
				NameSpace: []byte("test-ns"),
				Elements: []*rpc.WriteBatchRawRequestElement{
					{
						ID: []byte("test-id"),
						Datapoint: &rpc.Datapoint{
							Timestamp:         time.Now().Unix(),
							TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
							Value:             1.0,
						},
					},
				},
			}

			// Make calls that will fail
			for i := 0; i < tt.failCalls; i++ {
				mockNode.failNextCall = true
				ctx, cancel := thrift.NewContext(time.Second)
				err := wrappedNode.WriteBatchRaw(ctx, req)
				cancel()
				assert.Error(t, err)
			}

			// Make a successful call
			mockNode.failNextCall = false
			ctx, cancel := thrift.NewContext(time.Second)
			err = wrappedNode.WriteBatchRaw(ctx, req)
			cancel()

			// Verify circuit breaker state
			if tt.expectedClosed {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}

			// Wait for circuit breaker to potentially close
			time.Sleep(100 * time.Millisecond)

			// Make another call to verify state
			ctx, cancel = thrift.NewContext(time.Second)
			err = wrappedNode.WriteBatchRaw(ctx, req)
			cancel()
			if tt.expectedClosed {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
