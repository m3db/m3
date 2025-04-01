package middleware

import (
	"fmt"
	"strings"
)

const (
	// Rejection mode rejects requests in the middleware when circuit breaker does not allow
	// a request.
	Rejection Mode = iota + 1

	// Shadow mode allows the requests even when circuit breaker does not allow a request.
	// Useful for onboarding/testing.
	Shadow
)

var (
	_modeToString = map[Mode]string{
		Rejection: "rejection",
		Shadow:    "shadow",
	}

	_stringToMode = map[string]Mode{
		"rejection": Rejection,
		"shadow":    Shadow,
	}
)

// Mode represents the mode of the circuit breaker middleware.
type Mode int

// String returns a lower-case ASCII representation of the mode.
func (m Mode) String() string {
	s, ok := _modeToString[m]
	if ok {
		return s
	}
	return "unknown"
}

// MarshalText implements encoding.TextMarshaler.
func (m Mode) MarshalText() ([]byte, error) {
	s, ok := _modeToString[m]
	if ok {
		return []byte(s), nil
	}
	return nil, fmt.Errorf("unknown mode: %d", int(m))
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (m *Mode) UnmarshalText(text []byte) error {
	i, ok := _stringToMode[strings.ToLower(string(text))]
	if !ok {
		return fmt.Errorf("unknown mode string: %s", string(text))
	}
	*m = i
	return nil
}
