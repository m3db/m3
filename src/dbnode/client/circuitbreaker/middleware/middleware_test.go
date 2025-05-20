package middleware

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
)

// newTestConfig creates a common test configuration for middleware tests
func newTestConfig(enabled bool, shadowMode bool) Config {
	return Config{
		Enabled:    enabled,
		ShadowMode: shadowMode,
		CircuitBreakerConfig: circuitbreaker.Config{
			WindowSize:      15,
			BucketDuration:  time.Second,
			FailureRatio:    0.1,
			MinimumRequests: 1,
		},
	}
}

func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("invalid_config", func(t *testing.T) {
		config := Config{
			Enabled: true,
			CircuitBreakerConfig: circuitbreaker.Config{
				WindowSize: -1,
			},
		}
		middleware, err := New(config, logger, scope, host)
		require.Error(t, err)
		require.Nil(t, middleware)
	})

	t.Run("valid_config", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)
		require.NotNil(t, middleware)

		client := middleware(mockNode)
		require.NotNil(t, client)
	})
}

func TestWriteBatchRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("circuit_breaker_disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		ctx, cancel := thrift.NewContext(time.Second)
		defer cancel()
		req := &rpc.WriteBatchRawRequest{}

		mockNode.EXPECT().WriteBatchRaw(ctx, req).Return(nil)
		err = client.WriteBatchRaw(ctx, req)
		require.NoError(t, err)
	})

	t.Run("circuit_breaker_rejected_not_in_shadow_mode", func(t *testing.T) {
		config := Config{
			Enabled:    true,
			ShadowMode: false,
			CircuitBreakerConfig: circuitbreaker.Config{
				WindowSize:      15,
				BucketDuration:  time.Second,
				FailureRatio:    0.1,
				MinimumRequests: 1,
			},
		}
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		ctx, cancel := thrift.NewContext(time.Second)
		defer cancel()
		req := &rpc.WriteBatchRawRequest{}

		// First request should fail and trigger circuit breaker
		mockNode.EXPECT().WriteBatchRaw(ctx, req).Return(errors.New("test error"))
		err = client.WriteBatchRaw(ctx, req)
		require.Error(t, err)

		// Second request should be rejected by circuit breaker
		err = client.WriteBatchRaw(ctx, req)
		require.Error(t, err)
	})

	t.Run("circuit_breaker_rejected_in_shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		ctx, cancel := thrift.NewContext(time.Second)
		defer cancel()
		req := &rpc.WriteBatchRawRequest{}

		// First request should fail and trigger circuit breaker
		mockNode.EXPECT().WriteBatchRaw(ctx, req).Return(errors.New("test error"))
		err = client.WriteBatchRaw(ctx, req)
		require.Error(t, err)

		// Second request should still go through in shadow mode
		mockNode.EXPECT().WriteBatchRaw(ctx, req).Return(nil)
		err = client.WriteBatchRaw(ctx, req)
		require.NoError(t, err)
	})

	t.Run("circuit_breaker_success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		ctx, cancel := thrift.NewContext(time.Second)
		defer cancel()
		req := &rpc.WriteBatchRawRequest{}

		mockNode.EXPECT().WriteBatchRaw(ctx, req).Return(nil)
		err = client.WriteBatchRaw(ctx, req)
		require.NoError(t, err)
	})
}
