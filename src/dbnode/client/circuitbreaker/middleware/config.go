package middleware

import (
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker/circuitbreaker"
)

// Config represents the configuration for the circuit breaker middleware.
type Config struct {
	Enabled              bool                  `yaml:"enabled"`
	ShadowMode           bool                  `yaml:"ShadowMode"`
	CircuitBreakerConfig circuitbreaker.Config `yaml:"circuitBreakerConfig"`
}
