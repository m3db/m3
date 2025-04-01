package middleware

import (
	// "context"

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
}

// NewMiddlerWareOutbound returns a unary outbound circuit breaker middleware based on
// the provided config.
func NewMiddlerWareOutbound(serviceName string, logger *zap.Logger, enabler Enabler) (MiddlerWareOutbound, error) {
	// if err := config.Validate(); err != nil {
	// 	return nil, err
	// }

	return MiddlerWareOutbound{
		enabler: enabler,
		// logger:         logger.With(zap.String("component", _packageName)).WithOptions(zap.AddCaller()),
		circuitManager: newCircuitManager(),
	}, nil
}

// Call implements the middleware.UnaryOutbound interface.
// Applies circuit breaker to the outbound call if a circuit breaker is available
// for the request service and procedure.
func (u *MiddlerWareOutbound) WriteBatchRaw(ctx tchannel.ContextWithHeaders, req *rpc.WriteBatchRawRequest, tchanNodeClient rpc.TChanNode) error {

	if u == nil || !u.enabler.IsEnabled(ctx, "", "") {
		return tchanNodeClient.WriteBatchRaw(ctx, req)
	}

	circuit, err := u.circuitManager.circuit("", "")
	if err != nil {
		u.logger.Error("Failed to create circuit breaker",
			zap.String("dest", ""),
			zap.String("procedure", ""),
			zap.Error(err),
		)
		return tchanNodeClient.WriteBatchRaw(ctx, req)
	}

	if circuit == nil {
		return tchanNodeClient.WriteBatchRaw(ctx, req)
	}

	mode := u.enabler.Mode(ctx, "", "")
	isAllowed := circuit.IsRequestAllowed()
	if !isAllowed {
		if mode == Rejection {
			return circuitbreakererror.New("", "")
		}
	}

	err = tchanNodeClient.WriteBatchRaw(ctx, req)
	isSuccess := err == nil
	// Report request status and metrics only when the request is allowed.
	// isAllowed might be false when middleware is in shadow mode.
	if isAllowed {
		circuit.ReportRequestStatus(isSuccess)
	}
	return err
}
