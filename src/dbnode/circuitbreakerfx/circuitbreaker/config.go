package circuitbreaker

import (
	"fmt"
	"time"
)

const (
	_defaultWindowSize           = 15
	_defaultBucketDuration       = time.Second
	_defaultRecoveryTime         = time.Second * 2
	_defaultJitter               = time.Second * 1
	_defaultMaxProbeTime         = time.Second * 25
	_defaultMinimumRequests      = int64(100)
	_defaultFailureRatio         = 0.50
	_defaultMinimumProbeRequests = 25
)

var _defaultProbeRatios = []float64{0.05, 0.15, 0.30, 0.50, 0.75}

// Config represents configuration values for the circuit breaker.
type Config struct {
	// WindowSize defines the number of buckets in the sliding window.
	WindowSize int `yaml:"windowSize"`

	// BucketDuration defines the time duration of a bucket in the sliding window.
	BucketDuration time.Duration `yaml:"bucketDuration"`

	// RecoveryTime defines minimum amount of time duration circuit must be in the
	// Unhealthy state before transitioning to Probing state.
	RecoveryTime time.Duration `yaml:"recoveryTime"`

	// Jitter defines the additional duration on top of the recovery timeout where
	// the circuit must be in Unhealth state. A random value between 0 to Jitter
	// will be applied on top of recovery time.
	Jitter time.Duration `yaml:"jitter"`

	// MaxProbeTime defines the maximum time duration a circuit can stay in Probing state.
	MaxProbeTime time.Duration `yaml:"maxProbeTime"`

	// MinimumRequests defines the minimum number of requests a Healthy circuit
	// must be reported in the entire window before circuit checks the failure
	// rate.
	MinimumRequests int64 `yaml:"minimumRequests"`

	// FailureRatio defines the ratio at which circuit must transition to Unhealthy state.
	// The value ranges from [0.0, 1.0] where 1.0 means 100%.
	FailureRatio float64 `yaml:"failureRatio"`

	// ProbeRatios defines the list of RPS ratios to be probed during Probing state.
	// The values in the list must be sorted in the increasing order..
	// For instance when the average RPS in the window is 1000rps and a ratio of 0.1
	// means probing 100requests (0.1 * 1000).
	ProbeRatios []float64 `yaml:"probeRatios"`

	// MinimumProbeRequests defines the minimum number of requests a Probing circuit
	// must be reported in the entire window before circuit checks the failure rate
	// to either move to next probe ratio or transition to Healthy state.
	MinimumProbeRequests int64 `yaml:"minimumProbeRequests"`
}

// Validate return an error when there is a misconfigured config value.
func (c Config) Validate() error {
	if c.WindowSize < 0 {
		return fmt.Errorf("invalid window size: %d", c.WindowSize)
	}

	if c.BucketDuration < 0 {
		return fmt.Errorf("invalid bucket duration: %d", c.BucketDuration)
	}

	if c.RecoveryTime < 0 {
		return fmt.Errorf("invalid recovery time: %s", c.RecoveryTime)
	}

	if c.MaxProbeTime < 0 {
		return fmt.Errorf("invalid max probe time: %s", c.MaxProbeTime)
	}

	if c.Jitter < 0 {
		return fmt.Errorf("invalid jitter time: %s", c.Jitter)
	}

	if c.MinimumRequests < 0 {
		return fmt.Errorf("invalid minimum requests: %d", c.MinimumRequests)
	}

	if c.FailureRatio < 0 || c.FailureRatio > 1 {
		return fmt.Errorf("invalid failure ratio: %0.2f", c.FailureRatio)
	}

	if c.MinimumProbeRequests < 0 {
		return fmt.Errorf("invalid minimum probe requests: %d", c.MinimumProbeRequests)
	}

	for i, ratio := range c.ProbeRatios {
		if ratio <= 0 || ratio > 1 || (i != 0 && c.ProbeRatios[i-1] >= ratio) {
			return fmt.Errorf("invalid probe ratios: %v", c.ProbeRatios)
		}
	}

	return nil
}

// applyDefaultValues applies default values for zero value fields.
func (c Config) applyDefaultValues() Config {
	if c.WindowSize == 0 {
		c.WindowSize = _defaultWindowSize
	}

	if c.BucketDuration == 0 {
		c.BucketDuration = _defaultBucketDuration
	}

	if c.RecoveryTime == 0 {
		c.RecoveryTime = _defaultRecoveryTime
	}

	if c.MaxProbeTime == 0 {
		c.MaxProbeTime = _defaultMaxProbeTime
	}

	if c.Jitter == 0 {
		c.Jitter = _defaultJitter
	}

	if c.MinimumRequests == 0 {
		c.MinimumRequests = _defaultMinimumRequests
	}

	if c.FailureRatio == 0 {
		c.FailureRatio = _defaultFailureRatio
	}

	if c.MinimumProbeRequests == 0 {
		c.MinimumProbeRequests = _defaultMinimumProbeRequests
	}

	if len(c.ProbeRatios) == 0 {
		c.ProbeRatios = _defaultProbeRatios
	}

	return c
}
