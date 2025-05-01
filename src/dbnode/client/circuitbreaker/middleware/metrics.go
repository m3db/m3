package middleware

import (
	"github.com/uber-go/tally"
)

const (
	_packageName = "circuit_breaker"
)

type circuitBreakerMetrics struct {
	rejects       tally.Counter
	shadowRejects tally.Counter
	successes     tally.Counter
	failures      tally.Counter
}

func newMetrics(scope tally.Scope, host string) *circuitBreakerMetrics {
	return &circuitBreakerMetrics{
		rejects:       scope.Tagged(map[string]string{"host": host}).Counter("circuit_breaker_rejects"),
		shadowRejects: scope.Tagged(map[string]string{"host": host}).Counter("circuit_breaker_shadow_rejects"),
		successes:     scope.Tagged(map[string]string{"host": host}).Counter("circuit_breaker_successes"),
		failures:      scope.Tagged(map[string]string{"host": host}).Counter("circuit_breaker_failures"),
	}
}
