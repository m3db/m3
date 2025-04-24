package middleware

import (
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreakererror"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/uber-go/tally"
	tchannel "github.com/uber/tchannel-go"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
)

const (
	service      = "service"
	procedure    = "procedure"
	_packageName = "circuit_breaker"
)

var (
	shaaredCB     *atomic.Value
	sharedmetrics *circuitBreakerMetrics
	once          sync.Once
)

// MiddlerWareOutbound wraps a unary outbound circuit breaker middleware.
type circuitBreakerClient struct {
	enabler Enabler
	logger  *zap.Logger
	cb      *atomic.Value // *circuitbreaker.Circuit
	metrics *circuitBreakerMetrics
	host    string
	next    rpc.TChanNode
}

// middleware function to wrap a client
type M3dbtsMiddleware func(rpc.TChanNode) *circuitBreakerClient

// NewCircuitBreakerMiddleware returns a unary outbound circuit breaker middleware based on
// the provided config.
func NewCircuitBreakerMiddleware(config Config, logger *zap.Logger, scope tally.Scope, enabler Enabler, host string) M3dbtsMiddleware {
	once.Do(func() {
		logger.Info("creating circuit breaker middleware")
		scope = scope.Tagged(map[string]string{
			"component": _packageName,
			"host":      host,
		})
		sharedmetrics = &circuitBreakerMetrics{
			successes: scope.Counter("circuit_breaker_successes"),
			failures:  scope.Counter("circuit_breaker_failures"),
			rejects:   scope.Counter("circuit_breaker_rejects"),
		}

		c, err := circuitbreaker.NewCircuit(circuitbreaker.Config{})
		if err != nil {
			logger.Warn("failed to create circuit breaker, using nil", zap.Error(err))
		}
		shaaredCB = &atomic.Value{}
		shaaredCB.Store(c)
	})
	return func(next rpc.TChanNode) *circuitBreakerClient {
		return &circuitBreakerClient{
			next:    next,
			enabler: enabler,
			logger:  logger,
			host:    host,
			metrics: sharedmetrics,
			cb:      shaaredCB,
		}
	}
}

func withBreaker[T any](c *circuitBreakerClient, ctx tchannel.ContextWithHeaders, call func() error) error {
	cb, _ := c.cb.Load().(*circuitbreaker.Circuit)
	if cb == nil || !cb.IsRequestAllowed() {
		c.metrics.rejects.Inc(1)
		c.logger.Info("circuit breaker request rejected", zap.String("host", c.host))
		return circuitbreakererror.New(service, procedure)
	}

	err := call()
	if err == nil {
		cb.ReportRequestStatus(true)
		c.logger.Info("circuit breaker call success", zap.String("host", c.host))
		c.metrics.successes.Inc(1)
	} else {
		cb.ReportRequestStatus(false)
		c.logger.Info("circuit breaker call failed", zap.String("host", c.host))
		c.metrics.failures.Inc(1)
	}
	c.logger.Info("circuit breaker call done", zap.String("host", c.host))
	return err
}

func (c *circuitBreakerClient) WriteBatchRaw(ctx tchannel.ContextWithHeaders, req *rpc.WriteBatchRawRequest) error {
	return withBreaker[*rpc.WriteBatchRawRequest](c, ctx, func() error {
		return c.next.WriteBatchRaw(ctx, req)
	})
}

type circuitBreakerMetrics struct {
	// Successes counter that counts the number of successful requests.
	successes tally.Counter

	// Failures counter that counts the number of failed requests.
	failures tally.Counter

	// Rejects counter that counts the number of rejected requests.
	rejects tally.Counter
}
