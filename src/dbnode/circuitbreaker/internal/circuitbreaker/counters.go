package circuitbreaker

import "go.uber.org/atomic"

// counters hold the values required by the circuit breaker.
type counters struct {
	totalRequests      atomic.Int64 // counter for total requests seen by the circuit breaker.
	successfulRequests atomic.Int64 // counter for successful requests reported to the circuit breaker.
	failedRequests     atomic.Int64 // counter for failed requests reported to the circuit breaker.

	totalProbeRequests      atomic.Int64 // counter for total probe requests allowed by the circuit breaker.
	failedProbeRequests     atomic.Int64 // counter for failed probe requests reported to the circuit breaker.
	successfulProbeRequests atomic.Int64 // counter for successful probe requests reported to the circuit breaker.
}

// sub reduces all its counters from the provided counters.
func (c *counters) sub(cs *counters) {
	if c == nil || cs == nil {
		return
	}
	c.failedProbeRequests.Sub(cs.failedProbeRequests.Load())
	c.failedRequests.Sub(cs.failedRequests.Load())
	c.successfulProbeRequests.Sub(cs.successfulProbeRequests.Load())
	c.successfulRequests.Sub(cs.successfulRequests.Load())
	c.totalProbeRequests.Sub(cs.totalProbeRequests.Load())
	c.totalRequests.Sub(cs.totalRequests.Load())
}

// reset empties all the counters.
func (c *counters) reset() {
	if c == nil {
		return
	}
	c.failedProbeRequests.Store(0)
	c.failedRequests.Store(0)
	c.successfulProbeRequests.Store(0)
	c.successfulRequests.Store(0)
	c.totalProbeRequests.Store(0)
	c.totalRequests.Store(0)
}
