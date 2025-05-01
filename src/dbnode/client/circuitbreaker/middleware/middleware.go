package middleware

import (
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/circuitbreakererror"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/internal/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/uber-go/tally"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"
)

// client is a client that wraps a TChannel client with a circuit breaker.
type client struct {
	enabled    bool
	shadowMode bool
	logger     *zap.Logger
	circuit    *circuitbreaker.Circuit
	metrics    *circuitBreakerMetrics
	host       string
	next       rpc.TChanNode
}

// m3dbMiddleware is a function that takes a TChannel client and returns a circuit breaker client interface.
type m3dbMiddleware func(rpc.TChanNode) Client

// Client defines the interface for a circuit breaker client.
type Client interface {
	rpc.TChanNode
}

// New creates a new circuit breaker middleware.
func New(config Config, logger *zap.Logger, scope tally.Scope, host string) (m3dbMiddleware, error) {
	c, err := circuitbreaker.NewCircuit(config.CircuitBreakerConfig)
	if err != nil {
		logger.Warn("failed to create circuit breaker", zap.Error(err))
		return nil, err
	}

	return func(next rpc.TChanNode) (Client, error) {
		return &client{
			enabled:    config.Enabled,
			shadowMode: config.ShadowMode,
			next:       next,
			logger:     logger,
			host:       host,
			metrics:    newMetrics(scope, host),
			circuit:    c,
		}, nil
	}, nil
}

// withBreaker executes the given call with a circuit breaker if enabled.
func withBreaker[T any](c *client, ctx tchannel.ContextWithHeaders, req T, call func(tchannel.ContextWithHeaders, T) error) error {
	if !c.enabled {
		return c.executeWithoutBreaker(call)
	}

	if c.circuit == nil || !c.circuit.IsRequestAllowed() {
		return c.handleRejectedRequest(call)
	}

	return c.executeWithBreaker(call)
}

// executeWithoutBreaker executes the given call without a circuit breaker.
func (c *client) executeWithoutBreaker(call func() error) error {
	c.logger.Debug("circuit breaker disabled, calling next", zap.String("host", c.host))
	return call()
}

// handleRejectedRequest handles a rejected request by the circuit breaker.
func (c *client) handleRejectedRequest(call func() error) error {
	c.metrics.rejects.Inc(1)
	c.logger.Debug("circuit breaker request rejected", zap.String("host", c.host))
	if !c.shadowMode {
		return circuitbreakererror.New(c.host)
	}
	return call()
}

// executeWithBreaker executes the given call with a circuit breaker and handles success or failure.
func (c *client) executeWithBreaker(call func() error) error {
	err := call()
	if err == nil {
		c.handleSuccess()
	} else {
		c.handleFailure()
	}
	return err
}

// handleSuccess handles a successful request by the circuit breaker.
func (c *client) handleSuccess() {
	c.circuit.ReportRequestStatus(true)
	c.metrics.successes.Inc(1)
}

// handleFailure handles a failed request by the circuit breaker.
func (c *client) handleFailure() {
	c.circuit.ReportRequestStatus(false)
	c.logger.Debug("circuit breaker call failed", zap.String("host", c.host))
	c.metrics.failures.Inc(1)
}

// WriteBatchRaw is a method that writes a batch of raw data.
func (c *client) WriteBatchRaw(ctx thrift.Context, req *rpc.WriteBatchRawRequest) error {
	return withBreaker(c, ctx, req, c.next.WriteBatchRaw)
}

// Forward all other TChanNode methods to the underlying client
func (c *client) Aggregate(ctx thrift.Context, req *rpc.AggregateQueryRequest) (*rpc.AggregateQueryResult_, error) {
	return c.next.Aggregate(ctx, req)
}

func (c *client) AggregateRaw(ctx thrift.Context, req *rpc.AggregateQueryRawRequest) (*rpc.AggregateQueryRawResult_, error) {
	return c.next.AggregateRaw(ctx, req)
}

func (c *client) AggregateTiles(ctx thrift.Context, req *rpc.AggregateTilesRequest) (*rpc.AggregateTilesResult_, error) {
	return c.next.AggregateTiles(ctx, req)
}

func (c *client) Bootstrapped(ctx thrift.Context) (*rpc.NodeBootstrappedResult_, error) {
	return c.next.Bootstrapped(ctx)
}

func (c *client) BootstrappedInPlacementOrNoPlacement(ctx thrift.Context) (*rpc.NodeBootstrappedInPlacementOrNoPlacementResult_, error) {
	return c.next.BootstrappedInPlacementOrNoPlacement(ctx)
}

func (c *client) DebugIndexMemorySegments(ctx thrift.Context, req *rpc.DebugIndexMemorySegmentsRequest) (*rpc.DebugIndexMemorySegmentsResult_, error) {
	return c.next.DebugIndexMemorySegments(ctx, req)
}

func (c *client) DebugProfileStart(ctx thrift.Context, req *rpc.DebugProfileStartRequest) (*rpc.DebugProfileStartResult_, error) {
	return c.next.DebugProfileStart(ctx, req)
}

func (c *client) DebugProfileStop(ctx thrift.Context, req *rpc.DebugProfileStopRequest) (*rpc.DebugProfileStopResult_, error) {
	return c.next.DebugProfileStop(ctx, req)
}

func (c *client) Fetch(ctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	return c.next.Fetch(ctx, req)
}

func (c *client) FetchBatchRaw(ctx thrift.Context, req *rpc.FetchBatchRawRequest) (*rpc.FetchBatchRawResult_, error) {
	return c.next.FetchBatchRaw(ctx, req)
}

func (c *client) FetchBatchRawV2(ctx thrift.Context, req *rpc.FetchBatchRawV2Request) (*rpc.FetchBatchRawResult_, error) {
	return c.next.FetchBatchRawV2(ctx, req)
}

func (c *client) FetchBlocksMetadataRawV2(ctx thrift.Context, req *rpc.FetchBlocksMetadataRawV2Request) (*rpc.FetchBlocksMetadataRawV2Result_, error) {
	return c.next.FetchBlocksMetadataRawV2(ctx, req)
}

func (c *client) FetchBlocksRaw(ctx thrift.Context, req *rpc.FetchBlocksRawRequest) (*rpc.FetchBlocksRawResult_, error) {
	return c.next.FetchBlocksRaw(ctx, req)
}

func (c *client) FetchTagged(ctx thrift.Context, req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	return c.next.FetchTagged(ctx, req)
}

func (c *client) GetPersistRateLimit(ctx thrift.Context) (*rpc.NodePersistRateLimitResult_, error) {
	return c.next.GetPersistRateLimit(ctx)
}

func (c *client) GetWriteNewSeriesAsync(ctx thrift.Context) (*rpc.NodeWriteNewSeriesAsyncResult_, error) {
	return c.next.GetWriteNewSeriesAsync(ctx)
}

func (c *client) GetWriteNewSeriesBackoffDuration(ctx thrift.Context) (*rpc.NodeWriteNewSeriesBackoffDurationResult_, error) {
	return c.next.GetWriteNewSeriesBackoffDuration(ctx)
}

func (c *client) GetWriteNewSeriesLimitPerShardPerSecond(ctx thrift.Context) (*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult_, error) {
	return c.next.GetWriteNewSeriesLimitPerShardPerSecond(ctx)
}

func (c *client) Health(ctx thrift.Context) (*rpc.NodeHealthResult_, error) {
	return c.next.Health(ctx)
}

func (c *client) Query(ctx thrift.Context, req *rpc.QueryRequest) (*rpc.QueryResult_, error) {
	return c.next.Query(ctx, req)
}

func (c *client) Repair(ctx thrift.Context) error {
	return c.next.Repair(ctx)
}

func (c *client) SetPersistRateLimit(ctx thrift.Context, req *rpc.NodeSetPersistRateLimitRequest) (*rpc.NodePersistRateLimitResult_, error) {
	return c.next.SetPersistRateLimit(ctx, req)
}

func (c *client) SetWriteNewSeriesAsync(ctx thrift.Context, req *rpc.NodeSetWriteNewSeriesAsyncRequest) (*rpc.NodeWriteNewSeriesAsyncResult_, error) {
	return c.next.SetWriteNewSeriesAsync(ctx, req)
}

func (c *client) SetWriteNewSeriesBackoffDuration(ctx thrift.Context, req *rpc.NodeSetWriteNewSeriesBackoffDurationRequest) (*rpc.NodeWriteNewSeriesBackoffDurationResult_, error) {
	return c.next.SetWriteNewSeriesBackoffDuration(ctx, req)
}

func (c *client) SetWriteNewSeriesLimitPerShardPerSecond(ctx thrift.Context, req *rpc.NodeSetWriteNewSeriesLimitPerShardPerSecondRequest) (*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult_, error) {
	return c.next.SetWriteNewSeriesLimitPerShardPerSecond(ctx, req)
}

func (c *client) Truncate(ctx thrift.Context, req *rpc.TruncateRequest) (*rpc.TruncateResult_, error) {
	return c.next.Truncate(ctx, req)
}

func (c *client) Write(ctx thrift.Context, req *rpc.WriteRequest) error {
	return c.next.Write(ctx, req)
}

func (c *client) WriteBatchRawV2(ctx thrift.Context, req *rpc.WriteBatchRawV2Request) error {
	return c.next.WriteBatchRawV2(ctx, req)
}

func (c *client) WriteTagged(ctx thrift.Context, req *rpc.WriteTaggedRequest) error {
	return c.next.WriteTagged(ctx, req)
}

func (c *client) WriteTaggedBatchRaw(ctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) error {
	return c.next.WriteTaggedBatchRaw(ctx, req)
}

func (c *client) WriteTaggedBatchRawV2(ctx thrift.Context, req *rpc.WriteTaggedBatchRawV2Request) error {
	return c.next.WriteTaggedBatchRawV2(ctx, req)
}
