package middleware

import (
	// "context"
	"fmt"
	// "github.com/m3db/m3/src/dbnode/circuitbreaker/internal/circuitbreaker"
	"github.com/m3db/m3/src/dbnode/circuitbreaker/internal/circuitbreakererror"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	tchannel "github.com/uber/tchannel-go"
	"go.uber.org/zap"
)

// MiddlerWareOutbound wraps a unary outbound circuit breaker middleware.
type MiddlerWareOutbound struct {
	enabler        Enabler
	logger         *zap.Logger
	circuitManager *circuitManager
	host           string
}

// NewMiddlerWareOutbound returns a unary outbound circuit breaker middleware based on
// the provided config.
func NewMiddlerWareOutbound(serviceName string, logger *zap.Logger, enabler Enabler, host string) (MiddlerWareOutbound, error) {
	// if err := config.Validate(); err != nil {
	// 	return nil, err
	// }

	return MiddlerWareOutbound{
		enabler: enabler,
		// logger:         logger.With(zap.String("component", _packageName)).WithOptions(zap.AddCaller()),
		host:           host,
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

	mode := Rejection
	fmt.Println("Circuit breaker mode is", mode, "host", u.host)
	isAllowed := circuit.IsRequestAllowed(u.host)

	fmt.Println("Circuit breaker allowed value is", isAllowed, "host", u.host)

	if !isAllowed {
		fmt.Println("Circuit breaker request not allowed, host", u.host)

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
	}
	fmt.Println("Circuit breaker call done", err, "host", u.host)
	return err
}
