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

	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/internal/circuitbreaker"
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
		config := Config{
			Enabled: false,
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
		config := Config{
			Enabled:    true,
			ShadowMode: true,
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

		// Second request should still go through in shadow mode
		mockNode.EXPECT().WriteBatchRaw(ctx, req).Return(nil)
		err = client.WriteBatchRaw(ctx, req)
		require.NoError(t, err)
	})

	t.Run("circuit_breaker_success", func(t *testing.T) {
		config := Config{
			Enabled: true,
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

		mockNode.EXPECT().WriteBatchRaw(ctx, req).Return(nil)
		err = client.WriteBatchRaw(ctx, req)
		require.NoError(t, err)
	})
}

func TestFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchRequest{}
		resp := &rpc.FetchResult_{}

		mockNode.EXPECT().Fetch(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Fetch(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchRequest{}
		resp := &rpc.FetchResult_{}

		mockNode.EXPECT().Fetch(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Fetch(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchRequest{}
		resp := &rpc.FetchResult_{}

		mockNode.EXPECT().Fetch(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Fetch(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().Fetch(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.Fetch(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}

func TestFetchTagged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchTaggedRequest{}
		resp := &rpc.FetchTaggedResult_{}

		mockNode.EXPECT().FetchTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.FetchTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchTaggedRequest{}
		resp := &rpc.FetchTaggedResult_{}

		mockNode.EXPECT().FetchTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.FetchTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchTaggedRequest{}
		resp := &rpc.FetchTaggedResult_{}

		mockNode.EXPECT().FetchTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.FetchTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.FetchTaggedRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().FetchTagged(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.FetchTagged(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}

func TestWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteRequest{}
		resp := &rpc.WriteResult_{}

		mockNode.EXPECT().Write(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Write(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteRequest{}
		resp := &rpc.WriteResult_{}

		mockNode.EXPECT().Write(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Write(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteRequest{}
		resp := &rpc.WriteResult_{}

		mockNode.EXPECT().Write(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Write(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().Write(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.Write(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}

func TestWriteTagged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteTaggedRequest{}
		resp := &rpc.WriteTaggedResult_{}

		mockNode.EXPECT().WriteTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.WriteTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteTaggedRequest{}
		resp := &rpc.WriteTaggedResult_{}

		mockNode.EXPECT().WriteTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.WriteTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteTaggedRequest{}
		resp := &rpc.WriteTaggedResult_{}

		mockNode.EXPECT().WriteTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.WriteTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.WriteTaggedRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().WriteTagged(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.WriteTagged(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}

func TestAggregate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateRequest{}
		resp := &rpc.AggregateResult_{}

		mockNode.EXPECT().Aggregate(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Aggregate(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateRequest{}
		resp := &rpc.AggregateResult_{}

		mockNode.EXPECT().Aggregate(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Aggregate(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateRequest{}
		resp := &rpc.AggregateResult_{}

		mockNode.EXPECT().Aggregate(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Aggregate(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().Aggregate(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.Aggregate(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}

func TestAggregateTagged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateTaggedRequest{}
		resp := &rpc.AggregateTaggedResult_{}

		mockNode.EXPECT().AggregateTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.AggregateTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateTaggedRequest{}
		resp := &rpc.AggregateTaggedResult_{}

		mockNode.EXPECT().AggregateTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.AggregateTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateTaggedRequest{}
		resp := &rpc.AggregateTaggedResult_{}

		mockNode.EXPECT().AggregateTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.AggregateTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.AggregateTaggedRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().AggregateTagged(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.AggregateTagged(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}

func TestQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryRequest{}
		resp := &rpc.QueryResult_{}

		mockNode.EXPECT().Query(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Query(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryRequest{}
		resp := &rpc.QueryResult_{}

		mockNode.EXPECT().Query(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Query(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryRequest{}
		resp := &rpc.QueryResult_{}

		mockNode.EXPECT().Query(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.Query(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().Query(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.Query(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}

func TestQueryTagged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNode := rpc.NewMockTChanNode(ctrl)
	logger := zap.NewNop()
	scope := tally.NewTestScope("", nil)
	host := "test-host"

	t.Run("disabled", func(t *testing.T) {
		config := newTestConfig(false, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryTaggedRequest{}
		resp := &rpc.QueryTaggedResult_{}

		mockNode.EXPECT().QueryTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.QueryTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("shadow_mode", func(t *testing.T) {
		config := newTestConfig(true, true)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryTaggedRequest{}
		resp := &rpc.QueryTaggedResult_{}

		mockNode.EXPECT().QueryTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.QueryTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("success", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryTaggedRequest{}
		resp := &rpc.QueryTaggedResult_{}

		mockNode.EXPECT().QueryTagged(gomock.Any(), req).Return(resp, nil)

		actualResp, err := client.QueryTagged(ctx, req)
		require.NoError(t, err)
		require.Equal(t, resp, actualResp)
	})

	t.Run("error", func(t *testing.T) {
		config := newTestConfig(true, false)
		middleware, err := New(config, logger, scope, host)
		require.NoError(t, err)

		client := middleware(mockNode)
		require.NotNil(t, client)

		ctx := thrift.NewContext(time.Second)
		req := &rpc.QueryTaggedRequest{}
		expectedErr := errors.New("test error")

		mockNode.EXPECT().QueryTagged(gomock.Any(), req).Return(nil, expectedErr)

		actualResp, err := client.QueryTagged(ctx, req)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, actualResp)
	})
}
