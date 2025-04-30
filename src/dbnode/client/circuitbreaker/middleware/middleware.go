package middleware

import (
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/circuitbreakererror"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/internal/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/uber-go/tally"
	tchannel "github.com/uber/tchannel-go"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
)

// circuitBreakerClient is a client that wraps a TChannel client with a circuit breaker.
type circuitBreakerClient struct {
	enabled    bool
	shadowMode bool
	logger     *zap.Logger
	cb         *atomic.Value // *circuitbreaker.Circuit
	metrics    *circuitBreakerMetrics
	host       string
	next       rpc.TChanNode
}

// M3dbMiddleware is a function that takes a TChannel client and returns a circuit breaker client interface.
type M3DBMiddleware func(rpc.TChanNode) CircuitBreakerClient

// CircuitBreakerClient defines the interface for a circuit breaker client.
type CircuitBreakerClient interface {
	WriteBatchRaw(ctx tchannel.ContextWithHeaders, req *rpc.WriteBatchRawRequest) error
}

// circuitBreakerState holds the shared state for circuit breakers
type circuitBreakerState struct {
	cbInitOnce sync.Map // map[string]*sync.Once
	cbMap      sync.Map // map[string]*atomic.Value
	metricsMap sync.Map // map[string]*circuitBreakerMetrics
}

// NewCircuitBreakerMiddleware creates a new circuit breaker middleware.
func NewCircuitBreakerMiddleware(config Config, logger *zap.Logger, scope tally.Scope, host string) M3DBMiddleware {
	state := &circuitBreakerState{}
	initializeCircuitBreaker(config, logger, scope, host, state)

	return func(next rpc.TChanNode) *circuitBreakerClient {
		return createCircuitBreakerClient(config, logger, host, next, state)
	}
}

// initializeCircuitBreaker initializes the circuit breaker for the given host.
func initializeCircuitBreaker(config Config, logger *zap.Logger, scope tally.Scope, host string, state *circuitBreakerState) {
	onceIface, _ := state.cbInitOnce.LoadOrStore(host, new(sync.Once))
	once := onceIface.(*sync.Once)

	once.Do(func() {
		logger.Info("creating circuit breaker middleware", zap.String("host", host))
		metrics := newMetrics(scope, host)
		state.metricsMap.Store(host, metrics)

		cb, err := circuitbreaker.NewCircuit(config.CircuitBreakerConfig)
		if err != nil {
			logger.Warn("failed to create circuit breaker", zap.Error(err))
			return
		}

		cbVal := &atomic.Value{}
		cbVal.Store(cb)
		state.cbMap.Store(host, cbVal)
	})
}

// createCircuitBreakerClient creates a new circuit breaker client.
func createCircuitBreakerClient(config Config, logger *zap.Logger, host string, next rpc.TChanNode, state *circuitBreakerState) *circuitBreakerClient {
	cbIface, _ := state.cbMap.Load(host)
	metricsIface, _ := state.metricsMap.Load(host)

	return &circuitBreakerClient{
		enabled:    config.Enabled,
		shadowMode: config.ShadowMode,
		next:       next,
		logger:     logger,
		host:       host,
		metrics:    metricsIface.(*circuitBreakerMetrics),
		cb:         cbIface.(*atomic.Value),
	}
}

// withBreaker executes the given call with a circuit breaker if enabled.
func withBreaker[T any](c *circuitBreakerClient, ctx tchannel.ContextWithHeaders, call func() error) error {
	if !c.enabled {
		return c.executeWithoutBreaker(call)
	}

	cb := c.getCircuit()
	if cb == nil || !cb.IsRequestAllowed() {
		return c.handleRejectedRequest()
	}

	return c.executeWithBreaker(cb, call)
}

// executeWithoutBreaker executes the given call without a circuit breaker.
func (c *circuitBreakerClient) executeWithoutBreaker(call func() error) error {
	c.logger.Info("circuit breaker disabled, calling next", zap.String("host", c.host))
	return call()
}

// getCircuit retrieves the circuit breaker from the atomic value.
func (c *circuitBreakerClient) getCircuit() *circuitbreaker.Circuit {
	cb, _ := c.cb.Load().(*circuitbreaker.Circuit)
	return cb
}

// handleRejectedRequest handles a rejected request by the circuit breaker.
func (c *circuitBreakerClient) handleRejectedRequest() error {
	c.metrics.rejects.Inc(1)
	c.logger.Info("circuit breaker request rejected", zap.String("host", c.host))
	if !c.shadowMode {
		return circuitbreakererror.New(c.host)
	}
	return nil
}

// executeWithBreaker executes the given call with a circuit breaker and handles success or failure.
func (c *circuitBreakerClient) executeWithBreaker(cb *circuitbreaker.Circuit, call func() error) error {
	err := call()
	if err == nil {
		c.handleSuccess(cb)
	} else {
		c.handleFailure(cb)
	}
	c.logger.Info("circuit breaker call done", zap.String("host", c.host))
	return err
}

// handleSuccess handles a successful request by the circuit breaker.
func (c *circuitBreakerClient) handleSuccess(cb *circuitbreaker.Circuit) {
	cb.ReportRequestStatus(true)
	c.logger.Info("circuit breaker call success", zap.String("host", c.host))
	c.metrics.successes.Inc(1)
}

// handleFailure handles a failed request by the circuit breaker.
func (c *circuitBreakerClient) handleFailure(cb *circuitbreaker.Circuit) {
	cb.ReportRequestStatus(false)
	c.logger.Info("circuit breaker call failed", zap.String("host", c.host))
	c.metrics.failures.Inc(1)
}

// WriteBatchRaw is a method that writes a batch of raw data.
func (c *circuitBreakerClient) WriteBatchRaw(ctx tchannel.ContextWithHeaders, req *rpc.WriteBatchRawRequest) error {
	return withBreaker[*rpc.WriteBatchRawRequest](c, ctx, func() error {
		return c.next.WriteBatchRaw(ctx, req)
	})
}
