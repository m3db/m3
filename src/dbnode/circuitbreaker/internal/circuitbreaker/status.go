package circuitbreaker

import (
	"math/rand"
	"sync"
	"time"
)

// Status represents the current state of the circuit and provides methods to
// transition between different states.
type Status struct {
	// state holds the current state of the circuit breaker.
	state State

	// recoveryTimeout is the time at which the circuit transitions from Unhealthy
	// state to Probing state. This is set when the circuit enter Unhealthy state.
	recoveryTimeout time.Time

	// ProbeTimeout is the maximum time the circuit will stay in Probing state.
	// This time set when the circuit breaker enter the probing state.
	probeTimeout time.Time

	// probeRatioIndex is the probeRatio index currently used in the probing state.
	// This is set to -1 when the circuit breaker is not in the probing state
	probeRatioIndex int

	config Config
	clock  clock
	mu     sync.RWMutex
}

// NewStatus returns a Status in the Healthy state.
func NewStatus(config Config) *Status {
	return &Status{
		clock:           _baseClock,
		config:          config,
		probeRatioIndex: -1,
		state:           Healthy,
	}
}

// ProbeRatio returns the probe ratio at which the circuit is currently Probing.
// It returns false in second return value when a valid probe index is not set.
// This method is safe for concurrent use.
func (s *Status) ProbeRatio() (ratio float64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.state != Probing {
		return 0.0, false
	}
	return s.config.ProbeRatios[s.probeRatioIndex], true
}

// ProbeTimeout returns the maximum time the circuit will stay in Probing state.
// This method is safe for concurrent use.
func (s *Status) ProbeTimeout() time.Time {
	s.mu.RLock()
	timeout := s.probeTimeout
	s.mu.RUnlock()
	return timeout
}

// RecoveryTimeout returns the time at which the circuit transitions from
// Unhealthy state to Probing state.
// This method is safe for concurrent use.
func (s *Status) RecoveryTimeout() time.Time {
	s.mu.RLock()
	timeout := s.recoveryTimeout
	s.mu.RUnlock()
	return timeout
}

// State returns the current state of the circuit.
// This method is safe for concurrent use.
func (s *Status) State() State {
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	return state
}

// isNextProbeRatioAvailable returns true when there are more probe ratios available.
// This method is safe for concurrent use.
func (s *Status) isNextProbeRatioAvailable() bool {
	s.mu.RLock()
	isAvailable := s.probeRatioIndex+1 < len(s.config.ProbeRatios)
	s.mu.RUnlock()
	return isAvailable
}

// isProbeTimeoutComplete returns true when the current time is after the probe timeout.
// This method is safe for concurrent use.
func (s *Status) isProbeTimeoutComplete() bool {
	s.mu.RLock()
	isComplete := s.clock.Now().After(s.probeTimeout)
	s.mu.RUnlock()
	return isComplete
}

// isRecoveryTimeout returns true when the current time is after the recovery timeout.
// This method is safe for concurrent use.
func (s *Status) isRecoveryTimeoutComplete() bool {
	s.mu.RLock()
	isComplete := s.clock.Now().After(s.recoveryTimeout)
	s.mu.RUnlock()
	return isComplete
}

// moveToNextProbeRatio moves to next probe ratio index.
// This method is safe for concurrent use.
func (s *Status) moveToNextProbeRatio() {
	s.mu.Lock()
	s.probeRatioIndex++
	s.mu.Unlock()
}

// updateState changes its state to the given state.
// This method is safe for concurrent use.
func (s *Status) updateState(newState State) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch newState {
	case Healthy:
		s.setHealthy()
	case Unhealthy:
		s.setUnhealthy()
	case Probing:
		s.setProbing()
	}
}

// setHealthy sets its state as healthy state.
func (s *Status) setHealthy() {
	s.reset()
	s.state = Healthy
}

// setProbing sets its state as Unhealthy with recovery timeout.
func (s *Status) setUnhealthy() {
	s.reset()
	s.state = Unhealthy
	jitterDuration := time.Duration(rand.Int63n(int64(s.config.Jitter)))
	s.recoveryTimeout = s.clock.Now().Add(s.config.RecoveryTime).Add(jitterDuration)
}

// setProbing sets its state as Probing with initial probe ratio and probe timeout.
func (s *Status) setProbing() {
	s.reset()
	s.state = Probing
	s.probeRatioIndex = 0
	s.probeTimeout = s.clock.Now().Add(s.config.MaxProbeTime)
}

// reset defaults the state to Healthy, and empties other fields.
func (s *Status) reset() {
	s.state = Healthy
	s.recoveryTimeout = time.Time{}
	s.probeTimeout = time.Time{}
	s.probeRatioIndex = -1
}
