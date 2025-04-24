package middleware

import (
	"github.com/m3db/m3/src/dbnode/circuitbreakerfx/circuitbreaker"
)

// Config is a definition of the circuit breaker middleware.
type Config struct {

	// Enable must be set to true to enable circuit breaker middleware.
	Enabled bool `yaml:"enabled"`

	// EnableShadowMode when set enables shadow mode where outbound requests are not
	// blocked when circuit breaker rejects the request.
	ShadowMode bool `yaml:"ShadowMode"`

	// Configs is the user defined circuit breaker config.
	CircuitBreakerConfig circuitbreaker.Config `yaml:"circuitBreakerConfig"`
}
