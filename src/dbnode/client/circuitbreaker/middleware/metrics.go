package middleware

import (
	"github.com/uber-go/tally"
)

const (
	_packageName = "circuit_breaker"
)

type circuitBreakerMetrics struct {
	successes tally.Counter // counter for successful requests
	failures  tally.Counter // counter for failed requests
	rejects   tally.Counter // counter for rejected requests
}

func newMetrics(scope tally.Scope, host string) *circuitBreakerMetrics {
	taggedScope := scope.Tagged(map[string]string{
		"component": _packageName,
		"host":      host,
	})

	metrics := &circuitBreakerMetrics{
		successes: taggedScope.Counter("circuit_breaker_successes"),
		failures:  taggedScope.Counter("circuit_breaker_failures"),
		rejects:   taggedScope.Counter("circuit_breaker_rejects"),
	}
	return metrics
}
