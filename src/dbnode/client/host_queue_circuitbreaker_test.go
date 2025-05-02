package client

import (
	"errors"
	"strings"
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
			cbMiddleware, err := middleware.New(
				test.circuitBreakerConfig,
				opts.InstrumentOptions().Logger(),
				opts.InstrumentOptions().MetricsScope(),
				"test-host",
			)
			require.NoError(t, err)

			wrappedNode := cbMiddleware(mockNode)

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

			for i := 0; i < test.numCalls; i++ {
				writeOp := testWriteOp(
					"testnamespace",
					"testid",
					42.0,
					1234567890,
					rpc.TimeType_UNIX_SECONDS,
					func(result interface{}, err error) {
						actualErrs = append(actualErrs, err)
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
					assert.True(t, hasCircuitBreakerError(actualErrs), "Expected at least one circuit breaker rejection error")
				}
			} else {
				for _, err := range actualErrs {
					assert.NoError(t, err)
				}
			}
		})
	}
}
