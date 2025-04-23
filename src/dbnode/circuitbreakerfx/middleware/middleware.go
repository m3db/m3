package middleware

import (
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreakererror"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/uber-go/tally"
	tchannel "github.com/uber/tchannel-go"
	"go.uber.org/zap"
	"sync"
)

const (
	service   = "service"
	procedure = "procedure"
)

// MiddlerWareOutbound wraps a unary outbound circuit breaker middleware.
type MiddlerWareOutbound struct {
	enabler        Enabler
	logger         *zap.Logger
	circuitManager *circuitManager
	observer       *observer
	host           string
	next           rpc.TChanNode
}

// middleware function to wrap a client
type m3dbtsMiddleware func(rpc.TChanNode) *MiddlerWareOutbound

var (
	sharedCircuitManager *circuitManager
	sharedobserver       *observer
	once                 sync.Once
)

// NewMiddlerWareOutbound returns a unary outbound circuit breaker middleware based on
// the provided config.
func NewMiddlerWareOutbound(config Config, logger *zap.Logger, scope tally.Scope, enabler Enabler, host string) m3dbtsMiddleware {
	once.Do(func() {
		sharedCircuitManager = newCircuitManager(newPolicyProvider(config))
		sharedobserver, _ = newObserver(host, scope)
	})

	return func(next rpc.TChanNode) *MiddlerWareOutbound {
		return &MiddlerWareOutbound{
			next:           next,
			enabler:        enabler,
			logger:         logger,
			host:           host,
			observer:       sharedobserver,
			circuitManager: sharedCircuitManager,
		}
	}
}

func withBreaker[T any](u *MiddlerWareOutbound, ctx tchannel.ContextWithHeaders, call func() error) error {
	if u == nil || !u.enabler.IsEnabled(ctx, service, procedure) {
		u.logger.Info("Circuit breaker not enabled",
			zap.String("host", u.host),
		)
		return call()
	}

	// create new or fetch cached circuit
	circuit, err := u.circuitManager.circuit(service, procedure)
	if err != nil {
		u.logger.Error("Failed to create circuit breaker",
			zap.String("host", u.host),
			zap.Error(err),
		)
		return call()
	}

	if circuit == nil {
		return call()
	}

	// edge handles emitting metrics of the circuit breaker.
	edge, err := u.observer.edge(u.host)
	if err != nil {
		u.logger.Error("Failed to fetch call edge",
			zap.String("host", u.host),
			zap.Error(err),
		)
		return call()
	}

	mode := u.enabler.Mode(ctx, service, procedure)
	isAllowed := circuit.IsRequestAllowed()

	if !isAllowed {
		u.logger.Info("Circuit breaker request not allowed",
			zap.String("host", u.host),
		)
		edge.reportRequestRejected()
		if mode == Rejection {
			return circuitbreakererror.New(service, procedure)
		}
	}

	err = call()
	isSuccess := err == nil

	if isAllowed {
		circuit.ReportRequestStatus(isSuccess)
		edge.reportRequestComplete(isSuccess)
	}

	u.logger.Info("Circuit breaker call done",
		zap.String("host", u.host),
	)

	return err
}

func (u *MiddlerWareOutbound) WriteBatchRaw(ctx tchannel.ContextWithHeaders, req *rpc.WriteBatchRawRequest) error {
	return withBreaker[*rpc.WriteBatchRawRequest](u, ctx, func() error {
		return u.next.WriteBatchRaw(ctx, req)
	})
}
