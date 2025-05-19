package circuitbreakererror

import (
	"strings"

	xerrors "github.com/m3db/m3/src/x/errors"
)

// circuitBreakerError is an error type that indicates a circuit breaker error.
type circuitBreakerError struct {
	host string
}

var _ error = (*circuitBreakerError)(nil)

// New creates a new circuit breaker error with the given host.
func New(host string) error {
	err := circuitBreakerError{host: host}
	return xerrors.NewNonRetryableError(err)
}

// Error returns the error message for the circuit breaker error.
func (e circuitBreakerError) Error() string {
	var b strings.Builder
	b.WriteString("request rejected by circuit breaker of outbound-service: ")
	b.WriteString(e.host)
	return b.String()
}
