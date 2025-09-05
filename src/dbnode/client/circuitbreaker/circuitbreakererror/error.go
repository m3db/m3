package circuitbreakererror

import (
	"strings"

	xerrors "github.com/m3db/m3/src/x/errors"
)

// circuitBreakerError is an error type that indicates a circuit breaker error.
type circuitBreakerError struct {
	host      string
	lastError error
}

const (
	circuitBreakerRejectMessage = "request rejected by circuit breaker"
)

var _ error = (*circuitBreakerError)(nil)

// New creates a new circuit breaker error with the given host.
func New(host string) error {
	err := circuitBreakerError{host: host}
	return xerrors.NewNonRetryableError(err)
}

// NewWithLastError creates a new circuit breaker error with the given host and last error.
func NewWithLastError(host string, lastError error) error {
	err := circuitBreakerError{host: host, lastError: lastError}
	return xerrors.NewNonRetryableError(err)
}

// Error returns the error message for the circuit breaker error.
func (e circuitBreakerError) Error() string {
	var b strings.Builder
	b.WriteString(circuitBreakerRejectMessage)
	b.WriteString(e.host)
	if e.lastError != nil {
		b.WriteString(" (last error: ")
		b.WriteString(e.lastError.Error())
		b.WriteString(")")
	}
	return b.String()
}
