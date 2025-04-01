package circuitbreakererror

import (
	"errors"
	"strings"
	// "go.uber.org/yarpc/yarpcerrors"
)

var _ error = (*circuitBreakerError)(nil)

// circuitBreakerError wraps the error and yarpcerror interface.
type circuitBreakerError struct {
	service   string
	procedure string
}

// New returns a circuit breaker error with the given service and procedure.
func New(service, procedure string) error {
	return circuitBreakerError{service: service, procedure: procedure}
}

// Error method ensures circuit breaker error is compliant to the Error interface.
// Returns an error message with the details of service and procedure.
func (e circuitBreakerError) Error() string {
	var b strings.Builder
	b.WriteString("request rejected by circuit breaker of outbound-service: ")
	b.WriteString(e.service)
	b.WriteString(" procedure: ")
	b.WriteString(e.procedure)
	return b.String()
}

// // YARPCError method ensures circuit breaker error is compliant to the YARPC error
// // interface. Returns a yarpc error status with code Unavailable and error message.
// // See more: https://github.com/yarpc/yarpc-go/blob/8ea1c1ebef3ee39b2ff5340d31b59bffce7758e9/yarpcerrors/errors.go#L50
// func (e circuitBreakerError) YARPCError() *yarpcerrors.Status {
// 	return yarpcerrors.Newf(yarpcerrors.CodeUnavailable, "%w", e)
// }

// Retryable method ensures that the circuit breaker error implements the retyrable interface.
// Always returning false makes the circuit breaker error a non retryable error,
// so that the retryfx middleware prevents further retries upon receiving a circuit breaker error.
// Link to the retryable interface in the retryfx middleware: https://sourcegraph.uberinternal.com/code.uber.internal/go/retryfx/-/blob/retry/err.go#L7:1
func (e circuitBreakerError) Retryable() bool {
	return false
}

// Is returns true when the given error is a circuit breaker error.
func Is(target error) bool {
	var cbErr circuitBreakerError
	return errors.As(target, &cbErr)
}
