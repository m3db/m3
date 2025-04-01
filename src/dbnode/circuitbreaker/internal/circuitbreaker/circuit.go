package circuitbreaker

import (
	"fmt"
	"math"
	"sync"
	"time"
)

var _ Circuiter = (*Circuit)(nil)

// Circuiter is a minimal circuit breaker interface where users can check
// if the request processing is allowed and report the status of the processed
// request.
type Circuiter interface {
	// IsRequestAllowed returns true if the request can be processed. Clients
	// must invoke this method before processing a request.
	// Note: you must call ReportRequestStatus method to report of status of
	// the request if the request was allowed by the circuit.
	// This method is safe for concurrent use.
	IsRequestAllowed(host string) (allowed bool)

	// ReportRequestStatus must be invoked to report the request status where
	// true mean a successful request.
	// This method will check the current failure rate and transition to Unhealthy
	// or Probing state if needed.
	// This method is safe for concurrent use.
	ReportRequestStatus(requestStatus bool)

	// Status returns the internal circuit status.
	// This method is safe for concurrent use.
	Status() *Status
}

// Circuit is an error rate based circuit breaker implementation
// which uses sliding window of buckets to maintain the counters of requests.
// It implements the core APIs of the circuit breaker to check if the request
// is allowed and accepts the status of a request, which is used to decide
// to transition between states(healthy, unhealthy, and probing) based on the
// success and failure ratio of the requests.
type Circuit struct {
	mu sync.Mutex

	window *slidingWindow
	config Config
	status *Status
}

// NewCircuit returns a circuit breaker based on the provided config.
// Returns an error if the provided config is invalid.
func NewCircuit(config Config) (*Circuit, error) {
	config = config.applyDefaultValues()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Circuit{
		config: config,
		window: newSlidingWindow(config.WindowSize, config.BucketDuration),
		status: NewStatus(config),
	}, nil
}

// IsRequestAllowed returns true if the request can be processed. Clients must invoke
// this method before processing a request and must continue processing request
// only when this method returns true.
// This method is safe for concurrent use.
func (c *Circuit) IsRequestAllowed(host string) bool {
	fmt.Println("successfulRequests=", c.window.counters().successfulRequests, "host=", host)
	fmt.Println("failedRequests=", c.window.counters().failedRequests, "host=", host)
	fmt.Println("totalRequests=", c.window.counters().totalRequests, "host=", host)

	fmt.Println("successfulProbeRequests=", c.window.counters().successfulProbeRequests, "host=", host)
	fmt.Println("failedProbeRequests=", c.window.counters().failedProbeRequests, "host=", host)
	fmt.Println("totalProbeRequests=", c.window.counters().totalProbeRequests, "host=", host)

	c.window.incTotalRequests()

	state := c.transitionStateIfNeeded()
	if state == Probing && c.shouldProbe() {
		c.window.incTotalProbeRequests()
		return true
	}

	fmt.Println("cicruitstate="+state.String(), "host=", host)

	return state == Healthy
}

// ReportRequestStatus must be invoked to report the request status where
// true means it was a successful request.
// This increments the counters of success or failure requests and processes the
// state to check the failure ratio and transition state if required.
// This method is safe for concurrent use.
func (c *Circuit) ReportRequestStatus(success bool) {
	switch c.status.State() {
	case Healthy:
		if success {
			c.window.incSuccessfulRequests()
		} else {
			c.window.incFailedRequests()
		}

		c.processHealthyState()
	case Probing:
		if success {
			c.window.incSuccessfulProbeRequests()
		} else {
			c.window.incFailedProbeRequests()
		}

		c.processProbingState()
	}
}

// Status returns the status of the circuit.
// This method is safe for concurrent use.
func (c *Circuit) Status() *Status {
	c.transitionStateIfNeeded()
	return c.status
}

// shouldProbe returns true if the circuit must allow a request for probing.
// This method is safe for concurrent use.
func (c *Circuit) shouldProbe() bool {
	counters := c.window.counters()
	probeRequests := counters.totalProbeRequests.Load()
	// Allow probe if current allowed probes are less than configured minimum probes.
	if probeRequests < c.config.MinimumProbeRequests {
		return true
	}

	expectedProbeRatio, ok := c.status.ProbeRatio()
	if !ok {
		// Do not allow probe when the current status is no more in Probing state.
		return false
	}
	currentProbeRatio := computeRatio(probeRequests, c.currentRPS(counters))
	return currentProbeRatio <= expectedProbeRatio
}

// transitionStateIfNeeded returns the state after checking if the timeouts are
// complete in unhealthy or probing state. It also processes the probing state.
// This method is safe for concurrent use.
func (c *Circuit) transitionStateIfNeeded() State {
	state := c.status.State()
	switch state {
	case Unhealthy:
		// Transition to Probing state if recovery timeout is over.
		if c.status.isRecoveryTimeoutComplete() {
			c.transitionState(Unhealthy, Probing)
			return c.status.State()
		}
	case Probing:
		// Transition to Healthy state if maximum probe timeout is over.
		if c.status.isProbeTimeoutComplete() {
			c.transitionState(Probing, Healthy)
			return c.status.State()
		}

		// Must assert the probing state behaviour here to handle an edge case which
		// happens due to the eviction of a large RPS bucket which could delay probe
		// state changes.
		// Example: assume buckets with the following - [10000rps, 100rps, 100rps, 100rps]
		// the average rps is 2500/reqs and current probe ratio is 0.20 (500reqs).
		// Circuit allows 100/500 probe reqs and the window slides to new bucket
		// removing older 10000rps bucket - [100rps, 100rps, 100rps, 100rps].
		// Now average rps to drop to ~100rps. Now the required probes are 20 (0.2 of 100).
		// Since the circuit has already seen 100probes, it doesn't allow any more
		// (100 > 20) probes which means probe state assertion is not called by report
		// request status until bucket with majority of probe reqs expires.
		// Calling the assertion here enables probe state to notice latest RPS.
		c.processProbingState()
		return c.status.State()
	}

	return state
}

// processHealthyState checks the failure rate in the healthy state.
// Transitions to unhealthy state if the failure rate is higher than the
// configured rate.
// This method is safe for concurrent use.
func (c *Circuit) processHealthyState() {
	counters := c.window.counters()
	successfulReqs := counters.successfulRequests.Load()
	failedReqs := counters.failedRequests.Load()
	if (successfulReqs + failedReqs) < c.config.MinimumRequests {
		// Do nothing when the reported requests are less than minimum required
		// requests configured.
		return
	}

	if failureRatio(successfulReqs, failedReqs) >= c.config.FailureRatio {
		// When the failure ratio is higher than configured failure ratio
		// transition to Unhealthy state.
		c.transitionState(Healthy, Unhealthy)
	}
}

// processProbingState checks the failure rate and manages probe ratio in probing state.
// When the reported probes vs total requests ratio is higher than the current
// probe ratio, then the failure rate is asserted.
// When failure rate is less than configured failure rate, it moves to the next
// probe ratio or when all the ratios are probed it transitions to Healthy Sate.
// When failure rate is higher than configured failure rate, it transitions to
// Unhealthy state.
// This method is safe for concurrent use.
func (c *Circuit) processProbingState() {
	counters := c.window.counters()
	failedProbes := counters.failedProbeRequests.Load()
	successfulProbes := counters.successfulProbeRequests.Load()
	totalReportedProbes := successfulProbes + failedProbes
	if totalReportedProbes < c.config.MinimumProbeRequests {
		// Do nothing when the reported probes are less than minimum required
		// probes configured.
		return
	}

	reportedProbeRatio := computeRatio(totalReportedProbes, c.currentRPS(counters))
	expectedProbeRatio, ok := c.status.ProbeRatio()
	if !ok {
		// Do nothing when the current status is no more in Probing state.
		return
	}

	if reportedProbeRatio < expectedProbeRatio {
		// Do nothing when the reported probe ratio is less than minimum required
		// requests configured.
		return
	}

	probeFailureRate := failureRatio(successfulProbes, failedProbes)
	// Transition to Unhealthy state when failure rate of probes is higher than
	// configured failure ratio.
	if probeFailureRate >= c.config.FailureRatio {
		c.transitionState(Probing, Unhealthy)
		return
	}

	if c.status.isNextProbeRatioAvailable() {
		c.mu.Lock()
		defer c.mu.Unlock()

		currentProbeRatio, ok := c.status.ProbeRatio()
		// Assert that the previously seen probe ratio match the current status probe
		// ratio to guard against concurrent probe ratio changes.
		if ok && expectedProbeRatio == currentProbeRatio {
			// Update the status to the next available probe ratio.
			c.status.moveToNextProbeRatio()
		}
		return
	}

	// Probing is complete as there are no more probe ratios available.
	// Now circuit must Transition to Healthy state.
	// Assert that the earlier seen probe ratio and current probe ratio as same,
	// this is guard against concurrently updated probe ratio.
	// For example with probe ratios {0.1, 0.2, 0.3}, let's say the circuit is at
	// 0.2. Two routines enter this method and find that the probing is complete at 0.2.
	// One of the routine is ahead, finds next probe ratio is available and updates
	// probe ratio to 0.3. Now the second routine finds that there are no more probe
	// ratios available and might try to move to healthy state.
	// But this guard disallows incorrect transition to healthy as probe ratio
	// do not match.
	if currentProbeRatio, ok := c.status.ProbeRatio(); ok && currentProbeRatio == expectedProbeRatio {
		c.transitionState(Probing, Healthy)
	}
}

// transitionState changes the state of the circuit breaker to the given state
// only if the provided old state is equal to the current state.
// Old state is used as a guard against outdated concurrent transition requests.
// This method is safe for concurrent use.
func (c *Circuit) transitionState(oldState State, newState State) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.status.State()
	// Use old state as a guard to check against outdated transition requests.
	if state != oldState {
		return
	}
	// Reset the state when circuit moves out of the Probing state.
	// Either during Probing -> Healthy or Probing -> Unhealthy.
	if state == Probing {
		c.window.reset()
	}

	c.status.updateState(newState)
}

// currentRPS returns the average requests per second seen by the circuit.
// This method is safe for concurrent use.
func (c *Circuit) currentRPS(counters *counters) int64 {
	// bucketsPerSecond holds the number buckets required to hold one second worth counters.
	// For instance, when bucketduration is 100ms it takes 10 (1s/100ms) buckets to
	// represent counters of 1sec.
	bucketsPerSecond := computeRatio(int64(time.Second), int64(c.config.BucketDuration))
	activePerSecondBuckets := math.Max(1, computeRatio(int64(c.window.activeBucketsCount()), int64(bucketsPerSecond)))
	rps := computeRatio(counters.totalRequests.Load(), int64(activePerSecondBuckets))
	return int64(rps)
}

// failureRatio returns the ratio of failures vs (successes+failures).
func failureRatio(successes, failures int64) float64 {
	return computeRatio(failures, successes+failures)
}

// computeRatio returns the ratio of n vs total.
func computeRatio(n, total int64) float64 {
	if total == 0 {
		return 0
	}

	return float64(n) / float64(total)
}
