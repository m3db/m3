package middleware

import (
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/circuitbreakererror"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
)

// client is a client that wraps a TChannel client with a circuit breaker.
type client struct {
	logger   *zap.Logger
	circuit  *circuitbreaker.Circuit
	metrics  *circuitBreakerMetrics
	host     string
	next     rpc.TChanNode
	provider EnableProvider
}

// M3DBMiddleware is a function that takes a TChannel client and returns a circuit breaker client interface.
type M3DBMiddleware func(rpc.TChanNode) Client

// Client defines the interface for a circuit breaker client.
type Client interface {
	rpc.TChanNode
}

// NewNop returns a no-op middleware that simply forwards all calls to the underlying client
func NewNop() M3DBMiddleware {
	return func(next rpc.TChanNode) Client {
		return next
	}
}

// Params contains all parameters needed to create a new middleware
type Params struct {
	Config         Config
	Logger         *zap.Logger
	Scope          tally.Scope
	Host           string
	EnableProvider EnableProvider
}

// New creates a new circuit breaker middleware.
func New(params Params) (M3DBMiddleware, error) {
	params.Logger.Info("creating circuit breaker middleware", zap.Any("config", params.Config))
	c, err := circuitbreaker.NewCircuit(params.Config.CircuitBreakerConfig)
	if err != nil {
		params.Logger.Warn("failed to create circuit breaker", zap.Error(err))
		return nil, err
	}

	return func(next rpc.TChanNode) Client {
		return &client{
			next:     next,
			logger:   params.Logger,
			host:     params.Host,
			metrics:  newMetrics(params.Scope, params.Host),
			circuit:  c,
			provider: params.EnableProvider,
		}
	}, nil
}

// withBreaker executes the given call with a circuit breaker if enabled.
func withBreaker[T any](c *client, ctx thrift.Context, req T, call func(thrift.Context, T) error) error {
	// Early return if provider is nil, circuit breaker is disabled or not initialized
	if c.provider == nil {
		c.logger.Warn("withBreaker called with nil provider, bypassing circuit breaker")
		return call(ctx, req)
	}

	// Early return if circuit breaker is disabled or not initialized
	if c.circuit == nil || !c.provider.IsEnabled() {
		return call(ctx, req)
	}

	c.logger.Info("circuit breaker is enabled", zap.Bool("enabled", c.provider.IsEnabled()))

	// Check if request is allowed
	isAllowed := c.circuit.IsRequestAllowed()

	// If request is not allowed, log and return error
	if !isAllowed {
		if c.provider.IsShadowMode() {
			c.metrics.shadowRejects.Inc(1)
		} else {
			c.metrics.rejects.Inc(1)
			return circuitbreakererror.New(c.host)
		}
	}

	// Execute the request and update metrics
	err := call(ctx, req)
	if err == nil {
		c.metrics.successes.Inc(1)
	} else {
		c.metrics.failures.Inc(1)
	}

	// Report request status to circuit breaker
	if isAllowed {
		c.circuit.ReportRequestStatus(err == nil)
	}
	return err
}

// WriteBatchRaw is a method that writes a batch of raw data.
func (c *client) WriteBatchRaw(ctx thrift.Context, req *rpc.WriteBatchRawRequest) error {
	return withBreaker(c, ctx, req, c.next.WriteBatchRaw)
}

// Forward all other TChanNode methods to the underlying client
func (c *client) Aggregate(ctx thrift.Context, req *rpc.AggregateQueryRequest) (*rpc.AggregateQueryResult_, error) {
	return c.next.Aggregate(ctx, req)
}

func (c *client) AggregateRaw(
	ctx thrift.Context,
	req *rpc.AggregateQueryRawRequest,
) (*rpc.AggregateQueryRawResult_, error) {
	return c.next.AggregateRaw(ctx, req)
}

func (c *client) AggregateTiles(
	ctx thrift.Context,
	req *rpc.AggregateTilesRequest,
) (*rpc.AggregateTilesResult_, error) {
	return c.next.AggregateTiles(ctx, req)
}

func (c *client) Bootstrapped(ctx thrift.Context) (*rpc.NodeBootstrappedResult_, error) {
	return c.next.Bootstrapped(ctx)
}

func (c *client) BootstrappedInPlacementOrNoPlacement(
	ctx thrift.Context,
) (*rpc.NodeBootstrappedInPlacementOrNoPlacementResult_, error) {
	return c.next.BootstrappedInPlacementOrNoPlacement(ctx)
}

func (c *client) DebugIndexMemorySegments(
	ctx thrift.Context,
	req *rpc.DebugIndexMemorySegmentsRequest,
) (*rpc.DebugIndexMemorySegmentsResult_, error) {
	return c.next.DebugIndexMemorySegments(ctx, req)
}

func (c *client) DebugProfileStart(
	ctx thrift.Context,
	req *rpc.DebugProfileStartRequest,
) (*rpc.DebugProfileStartResult_, error) {
	return c.next.DebugProfileStart(ctx, req)
}

func (c *client) DebugProfileStop(
	ctx thrift.Context,
	req *rpc.DebugProfileStopRequest,
) (*rpc.DebugProfileStopResult_, error) {
	return c.next.DebugProfileStop(ctx, req)
}

func (c *client) Fetch(ctx thrift.Context, req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	return c.next.Fetch(ctx, req)
}

func (c *client) FetchBatchRaw(ctx thrift.Context, req *rpc.FetchBatchRawRequest) (*rpc.FetchBatchRawResult_, error) {
	return c.next.FetchBatchRaw(ctx, req)
}

func (c *client) FetchBatchRawV2(
	ctx thrift.Context,
	req *rpc.FetchBatchRawV2Request,
) (*rpc.FetchBatchRawResult_, error) {
	return c.next.FetchBatchRawV2(ctx, req)
}

func (c *client) FetchBlocksMetadataRawV2(
	ctx thrift.Context,
	req *rpc.FetchBlocksMetadataRawV2Request,
) (*rpc.FetchBlocksMetadataRawV2Result_, error) {
	return c.next.FetchBlocksMetadataRawV2(ctx, req)
}

func (c *client) FetchBlocksRaw(
	ctx thrift.Context,
	req *rpc.FetchBlocksRawRequest,
) (*rpc.FetchBlocksRawResult_, error) {
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

func (c *client) GetWriteNewSeriesBackoffDuration(
	ctx thrift.Context,
) (*rpc.NodeWriteNewSeriesBackoffDurationResult_, error) {
	return c.next.GetWriteNewSeriesBackoffDuration(ctx)
}

func (c *client) GetWriteNewSeriesLimitPerShardPerSecond(
	ctx thrift.Context,
) (*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult_, error) {
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

func (c *client) SetPersistRateLimit(
	ctx thrift.Context,
	req *rpc.NodeSetPersistRateLimitRequest,
) (*rpc.NodePersistRateLimitResult_, error) {
	return c.next.SetPersistRateLimit(ctx, req)
}

func (c *client) SetWriteNewSeriesAsync(
	ctx thrift.Context,
	req *rpc.NodeSetWriteNewSeriesAsyncRequest,
) (*rpc.NodeWriteNewSeriesAsyncResult_, error) {
	return c.next.SetWriteNewSeriesAsync(ctx, req)
}

func (c *client) SetWriteNewSeriesBackoffDuration(
	ctx thrift.Context,
	req *rpc.NodeSetWriteNewSeriesBackoffDurationRequest,
) (*rpc.NodeWriteNewSeriesBackoffDurationResult_, error) {
	return c.next.SetWriteNewSeriesBackoffDuration(ctx, req)
}

func (c *client) SetWriteNewSeriesLimitPerShardPerSecond(
	ctx thrift.Context,
	req *rpc.NodeSetWriteNewSeriesLimitPerShardPerSecondRequest,
) (*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult_, error) {
	return c.next.SetWriteNewSeriesLimitPerShardPerSecond(ctx, req)
}

func (c *client) Truncate(ctx thrift.Context, req *rpc.TruncateRequest) (*rpc.TruncateResult_, error) {
	return c.next.Truncate(ctx, req)
}

func (c *client) Write(ctx thrift.Context, req *rpc.WriteRequest) error {
	return withBreaker(c, ctx, req, c.next.Write)
}

func (c *client) WriteBatchRawV2(ctx thrift.Context, req *rpc.WriteBatchRawV2Request) error {
	return withBreaker(c, ctx, req, c.next.WriteBatchRawV2)
}

func (c *client) WriteTagged(ctx thrift.Context, req *rpc.WriteTaggedRequest) error {
	return withBreaker(c, ctx, req, c.next.WriteTagged)
}

func (c *client) WriteTaggedBatchRaw(ctx thrift.Context, req *rpc.WriteTaggedBatchRawRequest) error {
	return withBreaker(c, ctx, req, c.next.WriteTaggedBatchRaw)
}

func (c *client) WriteTaggedBatchRawV2(ctx thrift.Context, req *rpc.WriteTaggedBatchRawV2Request) error {
	return withBreaker(c, ctx, req, c.next.WriteTaggedBatchRawV2)
}
