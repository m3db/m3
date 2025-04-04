// Circuit breaker is a core library that provides APIs for a failure-rate based
// circuit breaking for high throughput systems. It uses a sliding window to hold
// the aggregated stats of the circuit, where window size and bucket sizes of the
// sliding window are configurable. It implements a smooth traffic rollout during
// probing mode to avoid thundering herd requests towards the affected service.
//
// Circuit initially starts in the Healthy state.
// In the healthy state, the circuit allows all the requests and simultaneously
// monitors the failure rate. When the failure rate is above the failure ratio
// given in config.FailureRatio, the circuit transitions to the Unhealthy state.
// It should be noted that circuit must notice the status of minimum requests as
// given in config.MinimumRequests to move to the Unhealthy state.
//
// In an Unhealthy state, the circuit rejects all the requests. When the circuit
// breaker moves to the Unhealthy state, a timer is set based on configured
// config.RecoveryTime + rand [0, config.Jitter). Once this timer completes,
// the circuit transitions to a Probing state.
//
// In the Probing state, the circuit partially accepts few requests to probe
// based on the probing ratio, and the rest of the requests are rejected.
// Each probe ratio represents the ratio of the average RPS seen by the circuit.
// A probe ratio of 0.2 with ~100rps means 20 requests must be probed.
// The probing begins by allowing a percentage of requests based on the first
// probing ratio from config.ProbeRatios against the current average rps, and
// asserts the failure rate at end of each ratio.
// The number of requests to be probed would be the maximum of either probe
// ratio at current RPS or configured config.MinimumProbeRequests.
// When the failure rate is below config.FailureRatio, it moves to the next
// probe ratio or it transitions back to the unhealthy state.
// Once all the probe ratios have been probed, it transitions to the Healthy state.
// It should be noted probing has a max probe timeout after which the circuit
// must transitions to the healthy state, this timeout is set when the circuit
// transitions to the probing state based on configured config.MaxProbeTime.
//
// State names used by the circuit breaker matches the industry standard states
// where Healthy=Close, Unhealthy=Open and Probing=Half-open.
//
// More details of the implementation can be found in the ERD:
// https://erd.uberinternal.com/projects/erd/602302ea-1123-42e4-9206-3e1ce06cc8bd
//
// Example usage:
//	cb := circuitbreaker.NewCircuitBreaker(Config{...})
//	...
//	func handler() {
//		// Check if circuit breaker allows this operation.
//    allowed := cb.IsRequestAllowed();
//    if !allowed {
//			return
//		}
//
//		err := doSomeOp()
//		// Report the operation status to the circuit breaker.
//		if err != nil {
//			cb.ReportRequestStatus(false)
//		} else {
//			cb.ReportRequestStatus(true)
//		}
//	}

package circuitbreaker
