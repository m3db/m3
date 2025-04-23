package middleware

import (
	"context"
	// "fmt"
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreakererror"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/uber-go/tally"
	tchannel "github.com/uber/tchannel-go"
	// "go.uber.org/net/metrics"
	"go.uber.org/zap"
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

// NewMiddlerWareOutbound returns a unary outbound circuit breaker middleware based on
// the provided config.
func NewMiddlerWareOutbound(config Config, logger *zap.Logger, scope tally.Scope, enabler Enabler, host string) m3dbtsMiddleware {

	return func(next rpc.TChanNode) *MiddlerWareOutbound {
		observer, err := newObserver(host, scope)
		if err != nil {
			return &MiddlerWareOutbound{}
		}

		return &MiddlerWareOutbound{
			next:           next,
			enabler:        enabler,
			logger:         logger.With(zap.String("component", _packageName)).WithOptions(zap.AddCaller()),
			host:           host,
			observer:       observer,
			circuitManager: newCircuitManager(newPolicyProvider(config)),
		}
	}
}

func withBreaker[T any](u *MiddlerWareOutbound, ctx tchannel.ContextWithHeaders, call func() error) error {
	if u == nil || !u.enabler.IsEnabled(ctx, "", "") {
		u.logger.Info("Circuit breaker not enabled",
			zap.String("host", u.host),
		)
		// fmt.Println("Circuit breaker not enabled", u.host)
		return call()
	}

	circuit, err := u.circuitManager.circuit("", "")
	if err != nil {
		u.logger.Error("Failed to create circuit breaker",
			zap.String("host", u.host),
			zap.Error(err),
		)
		// fmt.Println("Failed to create circuit breaker", u.host)
		return call()
	}

	if circuit == nil {
		return call()
	}

	edge, err := u.observer.edge(u.host)
	if err != nil {
		u.logger.Error("Failed to fetch call edge",
			zap.String("host", u.host),
			zap.Error(err),
		)
		return call()
	}

	mode := u.enabler.Mode(ctx, "", "")
	isAllowed := circuit.IsRequestAllowed()

	if !isAllowed {
		u.logger.Info("Circuit breaker request not allowed",
			zap.String("host", u.host),
		)

		edge.reportRequestRejected(circuit.Status(), mode)

		if mode == Rejection {
			return circuitbreakererror.New("", "")
		}
	}

	err = call()
	isSuccess := err == nil
	// Report request status and metrics only when the request is allowed.
	// isAllowed might be false when middleware is in shadow mode.
	if isAllowed {
		circuit.ReportRequestStatus(isSuccess)
		edge.reportRequestComplete(circuit.Status(), isSuccess, err, mode)
	}

	u.logger.Info("Circuit breaker call done",
		zap.String("host", u.host),
	)

	//TODO move it to module health check
	u.ReportHeartbeatMetrics()

	return err
}

func (u *MiddlerWareOutbound) WriteBatchRaw(ctx tchannel.ContextWithHeaders, req *rpc.WriteBatchRawRequest) error {
	return withBreaker[*rpc.WriteBatchRawRequest](u, ctx, func() error {
		return u.next.WriteBatchRaw(ctx, req)
	})
}

// ReportHeartbeatMetrics emits heartbeat metrics of all the circuits with the
// state and probe ratio.
func (u *MiddlerWareOutbound) ReportHeartbeatMetrics() {
	if u == nil {
		return
	}

	u.circuitManager.walk(func(sp serviceProcedure, circuit circuitbreaker.Circuiter) {
		if circuit != nil && u.enabler.IsEnabled(context.Background(), "", "") {
			mode := u.enabler.Mode(context.TODO(), "", "")
			u.observer.reportCircuitStateHeartbeat(circuit.Status(), sp.service, sp.procedure, mode)
		}
	})
}
