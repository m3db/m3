package middleware

import (
	"github.com/uber-go/tally/v4"
)

const hostTag = "host"

type circuitBreakerMetrics struct {
	rejects          tally.Counter
	shadowRejects    tally.Counter
	successes        tally.Counter
	failures         tally.Counter
	filteredFailures tally.Counter
}

func newMetrics(scope tally.Scope, host string) *circuitBreakerMetrics {
	return &circuitBreakerMetrics{
		successes:        scope.Tagged(map[string]string{hostTag: host}).Counter("circuit_breaker_successes"),
		failures:         scope.Tagged(map[string]string{hostTag: host}).Counter("circuit_breaker_failures"),
		filteredFailures: scope.Tagged(map[string]string{hostTag: host}).Counter("circuit_breaker_filtered_failures"),
		rejects:          scope.Tagged(map[string]string{hostTag: host, "mode": "live"}).Counter("circuit_breaker_rejects"),
		shadowRejects:    scope.Tagged(map[string]string{hostTag: host, "mode": "shadow"}).Counter("circuit_breaker_rejects"),
	}
}
