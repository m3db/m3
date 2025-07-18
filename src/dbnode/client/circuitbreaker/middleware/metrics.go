package middleware

import (
	"github.com/uber-go/tally"
)

type circuitBreakerMetrics struct {
	rejects       tally.Counter
	shadowRejects tally.Counter
	successes     tally.Counter
	failures      tally.Counter
}

func newMetrics(scope tally.Scope, host string) *circuitBreakerMetrics {
	return &circuitBreakerMetrics{
		successes:     scope.Tagged(map[string]string{"host": host}).Counter("circuit_breaker_successes"),
		failures:      scope.Tagged(map[string]string{"host": host}).Counter("circuit_breaker_failures"),
		rejects:       scope.Tagged(map[string]string{"host": host, "mode": "live"}).Counter("circuit_breaker_rejects"),
		shadowRejects: scope.Tagged(map[string]string{"host": host, "mode": "shadow"}).Counter("circuit_breaker_rejects"),
	}
}
