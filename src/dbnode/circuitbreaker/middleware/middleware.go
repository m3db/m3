package middleware

import (
	"context"
	"fmt"
	"github.com/m3db/m3/src/dbnode/circuitbreaker/internal/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/circuitbreaker/internal/circuitbreakererror"

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
}

// NewMiddlerWareOutbound returns a unary outbound circuit breaker middleware based on
// the provided config.
func NewMiddlerWareOutbound(serviceName string, logger *zap.Logger, scope tally.Scope, enabler Enabler, host string) (MiddlerWareOutbound, error) {
	// if err := config.Validate(); err != nil {
	// 	return nil, err
	// }

	observer, err := newObserver(host, scope)
	if err != nil {
		return MiddlerWareOutbound{}, err
	}

	return MiddlerWareOutbound{
		enabler: enabler,
		// logger:         logger.With(zap.String("component", _packageName)).WithOptions(zap.AddCaller()),
		host:           host,
		observer:       observer,
		circuitManager: newCircuitManager(),
	}, nil
}

// Call implements the middleware.UnaryOutbound interface.
// Applies circuit breaker to the outbound call if a circuit breaker is available
// for the request service and procedure.
func (u *MiddlerWareOutbound) WriteBatchRaw(ctx tchannel.ContextWithHeaders, req *rpc.WriteBatchRawRequest, tchanNodeClient rpc.TChanNode) error {

	if u == nil || !u.enabler.IsEnabled(ctx, "", "") {

		fmt.Println("Circuit breaker not enabled", u.host)
		return tchanNodeClient.WriteBatchRaw(ctx, req)
	}

	circuit, err := u.circuitManager.circuit("", "")
	if err != nil {
		// u.logger.Error("Failed to create circuit breaker",
		// 	zap.String("dest", ""),
		// 	zap.String("procedure", ""),
		// 	zap.Error(err),
		// )
		fmt.Println("Failed to create circuit breaker", u.host)
		return tchanNodeClient.WriteBatchRaw(ctx, req)
	}

	if circuit == nil {
		fmt.Println("Circuit created but is nil", u.host)
		return tchanNodeClient.WriteBatchRaw(ctx, req)
	}

	edge, err := u.observer.edge(u.host)
	if err != nil {
		u.logger.Error("Failed to fetch call edge",
			zap.String("host", u.host),
			zap.Error(err),
		)
		return tchanNodeClient.WriteBatchRaw(ctx, req)
	}

	mode := Rejection
	fmt.Println("Circuit breaker mode is", mode, "host", u.host)
	isAllowed := circuit.IsRequestAllowed(u.host)

	fmt.Println("Circuit breaker allowed value is", isAllowed, "host", u.host)

	if !isAllowed {
		fmt.Println("Circuit breaker request not allowed, host", u.host)
		edge.reportRequestRejected(circuit.Status(), mode)

		if mode == Rejection {

			fmt.Println("Circuit breaker mode is Rejection host=", u.host)

			return circuitbreakererror.New("", "")
		}
	}

	fmt.Println("Circuit breaker request is allowed", u.host)

	err = tchanNodeClient.WriteBatchRaw(ctx, req)
	isSuccess := err == nil
	// Report request status and metrics only when the request is allowed.
	// isAllowed might be false when middleware is in shadow mode.
	if isAllowed {
		circuit.ReportRequestStatus(isSuccess)
		edge.reportRequestComplete(circuit.Status(), isSuccess, err, mode)
	}
	fmt.Println("Circuit breaker call done", err, "host", u.host)

	//TODO move it to module health check
	u.ReportHeartbeatMetrics()

	return err
}

// ReportHeartbeatMetrics emits heartbeat metrics of all the circuits with the
// state and probe ratio.
func (u *MiddlerWareOutbound) ReportHeartbeatMetrics() {
	if u == nil {
		return
	}

	u.circuitManager.walk(func(sp serviceProcedure, circuit circuitbreaker.Circuiter) {
		if circuit != nil && u.enabler.IsEnabled(context.Background(), sp.service, sp.procedure) {
			mode := u.enabler.Mode(context.TODO(), sp.service, sp.procedure)
			u.observer.reportCircuitStateHeartbeat(circuit.Status(), sp.service, sp.procedure, mode)
		}
	})
}
