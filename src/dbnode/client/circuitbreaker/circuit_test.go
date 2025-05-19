package circuitbreaker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func assertStatus(t *testing.T, expected, seen *Status) {
	assert.Equal(t, expected.probeRatioIndex, seen.probeRatioIndex, "unexpected *status probe ratio index returned")
	assert.Equal(t, expected.recoveryTimeout, seen.recoveryTimeout, "unexpected recovery timeout returned")
	assert.Equal(t, expected.probeTimeout, seen.probeTimeout, "unexpected probe timeout returned")
	assert.Equal(t, expected.state, seen.state, "unexpected state returned")
}

func TestNewCircuit(t *testing.T) {
	t.Run("no_error", func(t *testing.T) {
		config := Config{
			WindowSize:           1,
			BucketDuration:       time.Microsecond,
			RecoveryTime:         time.Second,
			Jitter:               time.Second,
			MaxProbeTime:         time.Second,
			MinimumRequests:      1,
			FailureRatio:         0.1,
			ProbeRatios:          []float64{0.1, 0.2},
			MinimumProbeRequests: 1,
		}
		circuit, err := NewCircuit(config)
		require.NoError(t, err, "unexpected error on circuit creation")
		require.NotNil(t, circuit, "unexpected nil circuit")
		assert.Equal(t, time.Microsecond, circuit.window.bucketDuration, "expected microsecond bucket size")
		assert.Len(t, circuit.window.buckets, 1, "expected five buckets")
		assert.Equal(t, Healthy, circuit.status.state, "expected healthy circuit *status")
	})

	t.Run("must_return_error", func(t *testing.T) {
		config := Config{
			WindowSize:           -1,
			BucketDuration:       time.Microsecond,
			RecoveryTime:         time.Second,
			Jitter:               time.Second,
			MaxProbeTime:         time.Second,
			MinimumRequests:      1,
			FailureRatio:         0.1,
			ProbeRatios:          []float64{0.1, 0.2},
			MinimumProbeRequests: 1,
		}
		circuit, err := NewCircuit(config)
		assert.Error(t, err, "expected error on circuit creation")
		assert.Nil(t, circuit, "expected nil circuit")
	})

	t.Run("must_apply_default_values_for_zero_configs", func(t *testing.T) {
		config := Config{}
		circuit, err := NewCircuit(config)
		assert.NoError(t, err, "unexpected error on circuit creation")
		require.NotNil(t, circuit, "unexpected nil circuit")
		assert.Equal(t, time.Second*2, circuit.config.RecoveryTime, "unexpected recovery time")
	})
}

func TestRatioHelper(t *testing.T) {
	assert.Equal(t, 0.1, computeRatio(10, 100), "unexpected ratio")
	assert.Equal(t, 0.0, computeRatio(10, 0), "unexpected ratio")
	assert.Equal(t, 0.0, computeRatio(0, 10), "unexpected ratio")
}

func TestFailureRatio(t *testing.T) {
	assert.Equal(t, 0.2, failureRatio(40, 10), "unexpected failure ratio")
}

func TestCurrentRPS(t *testing.T) {
	tests := []struct {
		description         string
		givenBucketDuration time.Duration
		givenTotalRequests  int
		givenActiveBuckets  int
		expectedRPS         int64
	}{
		{
			description:         "10_1s_buckets",
			givenTotalRequests:  1000,
			givenActiveBuckets:  10,
			expectedRPS:         100,
			givenBucketDuration: time.Second,
		},
		{
			description:         "5_500ms_buckets",
			givenTotalRequests:  1000,
			givenActiveBuckets:  5,
			expectedRPS:         500,
			givenBucketDuration: time.Millisecond * 500,
		},
		{
			description:         "10_10ms_buckets",
			givenTotalRequests:  1000,
			givenActiveBuckets:  10,
			expectedRPS:         1000,
			givenBucketDuration: time.Millisecond * 10,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			circuit := Circuit{
				config: Config{
					BucketDuration: test.givenBucketDuration,
				},
				window: &slidingWindow{
					oldestBucketIndex:  0,
					currentBucketIndex: test.givenActiveBuckets - 1,
					buckets:            make([]bucket, test.givenActiveBuckets),
				},
			}
			givenCounters := counters{totalRequests: *atomic.NewInt64(int64(test.givenTotalRequests))}
			assert.Equal(t, test.expectedRPS, circuit.currentRPS(&givenCounters))
		})
	}
}

func TestStatus(t *testing.T) {
	expectedStatus := NewStatus(Config{FailureRatio: 0.102})
	circuit := Circuit{status: expectedStatus}
	require.Equal(t, expectedStatus, circuit.Status(), "unexpected status")
}

func TestTransitionState(t *testing.T) {
	tests := []struct {
		description            string
		mockTime               time.Time
		givenConfig            Config
		givenOldState          State
		givenNewState          State
		givenStatus            *Status
		expectedStatus         *Status
		expectedAggregateReset bool
	}{
		{
			description:    "healthy_to_unhealthy",
			mockTime:       time.Unix(10, 1),
			givenConfig:    Config{RecoveryTime: time.Second, Jitter: 1},
			givenOldState:  Healthy,
			givenNewState:  Unhealthy,
			givenStatus:    &Status{state: Healthy, probeRatioIndex: -1},
			expectedStatus: &Status{state: Unhealthy, recoveryTimeout: time.Unix(11, 1), probeRatioIndex: -1},
		},
		{
			description:    "unhealthy_to_probing",
			mockTime:       time.Unix(10, 1),
			givenConfig:    Config{MaxProbeTime: time.Second},
			givenOldState:  Unhealthy,
			givenNewState:  Probing,
			givenStatus:    &Status{state: Unhealthy, probeRatioIndex: -1},
			expectedStatus: &Status{state: Probing, probeTimeout: time.Unix(11, 1)},
		},
		{
			description:            "probing_to_healthy",
			mockTime:               time.Unix(11, 1),
			givenConfig:            Config{MaxProbeTime: time.Second},
			givenOldState:          Probing,
			givenNewState:          Healthy,
			givenStatus:            &Status{state: Probing, probeRatioIndex: 0, probeTimeout: time.Unix(11, 1)},
			expectedStatus:         &Status{state: Healthy, probeRatioIndex: -1},
			expectedAggregateReset: true,
		},
		{
			description:            "probing_to_unhealthy",
			mockTime:               time.Unix(11, 1),
			givenConfig:            Config{RecoveryTime: time.Second, Jitter: 1},
			givenOldState:          Probing,
			givenNewState:          Unhealthy,
			givenStatus:            &Status{state: Probing, probeRatioIndex: 0, probeTimeout: time.Unix(11, 1)},
			expectedStatus:         &Status{state: Unhealthy, recoveryTimeout: time.Unix(12, 1), probeRatioIndex: -1},
			expectedAggregateReset: true,
		},
		{
			description:    "no_transition_when_old_and_new_state_are_unhealthy",
			mockTime:       time.Unix(11, 1),
			givenConfig:    Config{RecoveryTime: time.Second, Jitter: 1},
			givenOldState:  Healthy, // givenStatus is already on Unhealthy state.
			givenNewState:  Unhealthy,
			givenStatus:    &Status{state: Unhealthy, recoveryTimeout: time.Unix(12, 1), probeRatioIndex: -1},
			expectedStatus: &Status{state: Unhealthy, recoveryTimeout: time.Unix(12, 1), probeRatioIndex: -1},
		},
		{
			description:    "no_transition_when_old_and_new_state_are_healthy",
			mockTime:       time.Unix(11, 1),
			givenConfig:    Config{RecoveryTime: time.Second, Jitter: 1},
			givenOldState:  Probing, // givenStatus is already on Healthy state.
			givenNewState:  Healthy,
			givenStatus:    &Status{state: Healthy, probeRatioIndex: -1},
			expectedStatus: &Status{state: Healthy, probeRatioIndex: -1},
		},
		{
			description:    "no_transition_when_old_and_new_start_are_probing",
			mockTime:       time.Unix(11, 1),
			givenConfig:    Config{RecoveryTime: time.Second, Jitter: 1},
			givenOldState:  Unhealthy, // givenStatus is already on Probing state.
			givenNewState:  Probing,
			givenStatus:    &Status{state: Probing, probeRatioIndex: -1},
			expectedStatus: &Status{state: Probing, probeRatioIndex: -1},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			clock := &mockClock{now: test.mockTime}
			circuit := Circuit{
				config: test.givenConfig,
				status: NewStatus(test.givenConfig),
				window: newSlidingWindow(1, time.Second),
			}

			circuit.status.state = test.givenStatus.state
			circuit.status.recoveryTimeout = test.givenStatus.recoveryTimeout
			circuit.status.probeTimeout = test.givenStatus.probeTimeout
			circuit.status.probeRatioIndex = test.givenStatus.probeRatioIndex
			circuit.status.clock = clock

			circuit.window.clock = clock
			// Increment now, transition state resets the counters when it transitions
			// out of probing state.
			circuit.window.incTotalRequests()

			// Simulate concurrent transition state updates.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				circuit.transitionState(test.givenOldState, test.givenNewState)
				wg.Done()
			}()
			circuit.transitionState(test.givenOldState, test.givenNewState)
			wg.Wait()

			assertStatus(t, test.expectedStatus, circuit.status)
			counters := circuit.window.counters()
			if test.expectedAggregateReset {
				assert.Equal(t, int64(0), counters.totalRequests.Load(), "expected zero total requests due to reset")
			} else {
				assert.Equal(t, int64(1), counters.totalRequests.Load(), "expected total requests to not reset")
			}
		})
	}
}

func TestShouldProbe(t *testing.T) {
	tests := []struct {
		description string
		givenWindow *slidingWindow
		givenConfig Config
		givenStatus *Status
		expectProbe bool
	}{
		{
			description: "must_allow_probe_when_probes_are_lower_than_50%_of_100rps",
			givenConfig: Config{
				BucketDuration: time.Second,
				ProbeRatios:    []float64{0.5},
			},
			givenWindow: &slidingWindow{
				aggregatedCounters: counters{
					totalRequests:      *atomic.NewInt64(500),
					totalProbeRequests: *atomic.NewInt64(49),
				},
				buckets: make([]bucket, 5),
			},
			givenStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectProbe: true,
		},
		{
			description: "must_allow_probe_when_probe_ratio_reached_but_probes_less_than_minimum_probes",
			givenConfig: Config{
				MinimumProbeRequests: 30,
				BucketDuration:       time.Second,
				ProbeRatios:          []float64{0.2},
			},
			givenWindow: &slidingWindow{
				aggregatedCounters: counters{
					totalRequests:      *atomic.NewInt64(100),
					totalProbeRequests: *atomic.NewInt64(24),
				},
				buckets: make([]bucket, 5),
			},
			givenStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectProbe: true,
		},
		{
			description: "must_not_allow_probe_when_probes_ratio_is_higher_than_20%_of_100rps",
			givenConfig: Config{
				BucketDuration: time.Second,
				ProbeRatios:    []float64{0.5},
			},
			givenWindow: &slidingWindow{
				aggregatedCounters: counters{
					totalRequests:      *atomic.NewInt64(100),
					totalProbeRequests: *atomic.NewInt64(51),
				},
				buckets: make([]bucket, 5),
			},
			givenStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectProbe: false,
		},
		{
			description: "must_not_allow_probe_when_not_in_probing_state",
			givenConfig: Config{
				BucketDuration: time.Second,
				ProbeRatios:    []float64{0.5},
			},
			givenWindow: &slidingWindow{
				aggregatedCounters: counters{
					totalRequests:      *atomic.NewInt64(100),
					totalProbeRequests: *atomic.NewInt64(51),
				},
				buckets: make([]bucket, 5),
			},
			givenStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectProbe: false,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			clock := &mockClock{now: time.Unix(10, 1)}
			circuit := Circuit{
				window: test.givenWindow,
				config: test.givenConfig,
				status: test.givenStatus,
			}
			circuit.status = test.givenStatus
			circuit.status.clock = clock
			circuit.status.config = test.givenConfig
			circuit.window.clock = clock

			assert.Equal(t, test.expectProbe, circuit.shouldProbe(), "unexpected probe state")
		})
	}
}

type circuitTest struct {
	description        string
	givenConfig        Config
	initialStatus      *Status
	expectedStatus     *Status
	givenSuccessProbes int
	givenFailedProbes  int
	givenTotalRequests int
	expectedReset      bool
}

func runCircuitTest(t *testing.T, tests []circuitTest, windowSize int, processFunc func(*Circuit)) {
	clock := &mockClock{now: time.Unix(10, 1)}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			circuit := Circuit{
				config: test.givenConfig,
				window: newSlidingWindow(windowSize, time.Second),
				status: NewStatus(test.givenConfig),
			}
			circuit.status.state = test.initialStatus.state
			circuit.status.recoveryTimeout = test.initialStatus.recoveryTimeout
			circuit.status.probeTimeout = test.initialStatus.probeTimeout
			circuit.status.probeRatioIndex = test.initialStatus.probeRatioIndex
			circuit.status.clock = clock
			circuit.window.clock = clock

			// Increment appropriate counters based on test case
			for i := 0; i < test.givenSuccessProbes; i++ {
				circuit.window.incSuccessfulProbeRequests()
			}
			for i := 0; i < test.givenFailedProbes; i++ {
				circuit.window.incFailedProbeRequests()
			}
			for i := 0; i < test.givenTotalRequests; i++ {
				circuit.window.incTotalRequests()
			}

			processFunc(&circuit)
			assertStatus(t, test.expectedStatus, circuit.status)
		})
	}
}

func TestProcessProbingState(t *testing.T) {
	tests := []circuitTest{
		{
			description:        "no_status_change_as_reported_probes_less_than_first_ratio",
			givenSuccessProbes: 10,
			givenFailedProbes:  8,
			givenTotalRequests: 100,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedReset: false,
		},
		{
			description:        "first_ratio_probed_move_to_next_probe_ratio",
			givenSuccessProbes: 150,
			givenFailedProbes:  50,
			givenTotalRequests: 1000,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 1,
			},
			expectedReset: false,
		},
		{
			description:        "no_change_when_probe_ratio_reached_but_probes_are_less_than_configured_minimum_probes",
			givenSuccessProbes: 299,
			givenTotalRequests: 1000,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 300,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedReset: false,
		},
		{
			description:        "probe_complete_must_complete_and_move_to_healthy",
			givenSuccessProbes: 570,
			givenFailedProbes:  40,
			givenTotalRequests: 1000,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 2,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedReset: true,
		},
		{
			description:        "probe_failure_rate_is_high_must_move_to_unhealthy",
			givenSuccessProbes: 24,
			givenFailedProbes:  45,
			givenTotalRequests: 100,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 2,
			},
			expectedStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
				recoveryTimeout: time.Unix(11, 1),
			},
			expectedReset: true,
		},
		{
			description:        "must_not_anything_when_status_not_in_probing_state",
			givenSuccessProbes: 170,
			givenFailedProbes:  40,
			givenTotalRequests: 1000,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedReset: false,
		},
	}
	runCircuitTest(t, tests, 1, func(c *Circuit) { c.processProbingState() })
}

func TestProcessHealthyState(t *testing.T) {
	clock := &mockClock{now: time.Unix(10, 1)}
	tests := []circuitTest{
		{
			description:        "no_status_change_as_reported_requests_less_than_minimum_requests",
			givenSuccessProbes: 10,
			givenFailedProbes:  8,
			givenTotalRequests: 19,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      20,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedReset: false,
		},
		{
			description:        "must_move_to_probing_state",
			givenSuccessProbes: 10,
			givenFailedProbes:  8,
			givenTotalRequests: 20,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      20,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedReset: false,
		},
		{
			description:        "must_move_to_unhealthy_state",
			givenSuccessProbes: 10,
			givenFailedProbes:  10,
			givenTotalRequests: 20,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      20,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
				recoveryTimeout: clock.now.Add(time.Second),
			},
			expectedReset: true,
		},
	}
	runCircuitTest(t, tests, 15, func(c *Circuit) { c.processHealthyState() })
}

func TestReportRequestStatus(t *testing.T) {
	clock := &mockClock{now: time.Unix(10, 1)}
	tests := []struct {
		description                  string
		givenConfig                  Config
		initialStatus                *Status
		expectedStatus               *Status
		givenReportSuccess           bool
		givenTotalProbes             int64
		givenTotalRequests           int64
		givenSuccessProbes           int64
		givenFailedProbes            int64
		givenSuccessRequests         int64
		givenFailedRequests          int64
		expectTotalRequests          int64
		expectedTotalProbes          int64
		expectedSuccessRequests      int64
		expectedFailureRequests      int64
		expectedSuccessProbeRequests int64
		expectedFailureProbeRequests int64
	}{
		{
			description: "must_stay_healthy_as_reported_requests_are_less_than_minimum_requests",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      100,
				FailureRatio:         0.20,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenReportSuccess:   true,
			givenTotalRequests:   98,
			givenSuccessRequests: 20,
			givenFailedRequests:  78,
			initialStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedSuccessRequests: 21,
			expectedFailureRequests: 78,
			expectTotalRequests:     98,
		},
		{
			description: "failure_rate_reached_must_move_to_unhealthy",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      100,
				FailureRatio:         0.2,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenReportSuccess:   false,
			givenTotalRequests:   99,
			givenSuccessRequests: 20,
			givenFailedRequests:  79,
			initialStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
				recoveryTimeout: clock.now.Add(time.Second),
			},
			expectedSuccessRequests: 20,
			expectedFailureRequests: 80,
			expectTotalRequests:     99,
		},
		{
			description: "must_not_change_as_circuit_is_unhealthy",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenReportSuccess:   false,
			givenTotalRequests:   100,
			givenSuccessRequests: 20,
			givenFailedRequests:  80,
			initialStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
			},
			expectedSuccessRequests: 20,
			expectedFailureRequests: 80,
			expectTotalRequests:     100,
		},
		{
			description: "must_stay_probing_as_minimum_probes_has_not_reached",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 25,
			},
			givenReportSuccess: false,
			givenTotalRequests: 100,
			givenTotalProbes:   20,
			givenSuccessProbes: 15,
			givenFailedProbes:  5,
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedFailureProbeRequests: 6,
			expectedTotalProbes:          20,
			expectedSuccessProbeRequests: 15,
			expectTotalRequests:          100,
		},
		{
			description: "must_proceed_to_next_probe_ratio",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.3, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenReportSuccess: true,
			givenTotalRequests: 100,
			givenTotalProbes:   31,
			givenSuccessProbes: 30,
			givenFailedProbes:  0,
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 1,
			},
			expectedTotalProbes:          31,
			expectedSuccessProbeRequests: 31,
			expectTotalRequests:          100,
		},
		{
			description: "must_transition_probing_to_unhealthy_as_probe_failure_rate_is_high",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.3, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenReportSuccess: false,
			givenTotalRequests: 100,
			givenTotalProbes:   31,
			givenSuccessProbes: 14,
			givenFailedProbes:  16,
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
				recoveryTimeout: clock.now.Add(time.Second),
			},
		},
		{
			description: "must_transition_probing_to_healthy_as_probe_ratios_are_complete",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.3, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenReportSuccess: false,
			givenTotalRequests: 100,
			givenTotalProbes:   61,
			givenSuccessProbes: 55,
			givenFailedProbes:  5,
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 2,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			circuit := Circuit{
				config: test.givenConfig,
				window: newSlidingWindow(15, time.Second),
				status: NewStatus(test.givenConfig),
			}
			circuit.status.state = test.initialStatus.state
			circuit.status.recoveryTimeout = test.initialStatus.recoveryTimeout
			circuit.status.probeTimeout = test.initialStatus.probeTimeout
			circuit.status.probeRatioIndex = test.initialStatus.probeRatioIndex
			circuit.status.clock = clock
			circuit.window.clock = clock

			for i := 0; i < int(test.givenSuccessRequests); i++ {
				circuit.window.incSuccessfulRequests()
			}

			for i := 0; i < int(test.givenFailedRequests); i++ {
				circuit.window.incFailedRequests()
			}

			for i := 0; i < int(test.givenTotalRequests); i++ {
				circuit.window.incTotalRequests()
			}

			for i := 0; i < int(test.givenTotalProbes); i++ {
				circuit.window.incTotalProbeRequests()
			}

			for i := 0; i < int(test.givenSuccessProbes); i++ {
				circuit.window.incSuccessfulProbeRequests()
			}

			for i := 0; i < int(test.givenFailedProbes); i++ {
				circuit.window.incFailedProbeRequests()
			}

			circuit.ReportRequestStatus(test.givenReportSuccess)
			assertStatus(t, test.expectedStatus, circuit.status)
			assert.Equal(t, test.expectTotalRequests, circuit.window.aggregatedCounters.totalRequests.Load(), "unexpected total requests")
			assert.Equal(t, test.expectedTotalProbes, circuit.window.aggregatedCounters.totalProbeRequests.Load(), "unexpected total probe requests")
			assert.Equal(t, test.expectedFailureProbeRequests, circuit.window.aggregatedCounters.failedProbeRequests.Load(), "unexpected failure probe requests")
			assert.Equal(t, test.expectedFailureRequests, circuit.window.aggregatedCounters.failedRequests.Load(), "unexpected failure requests")
			assert.Equal(t, test.expectedSuccessProbeRequests, circuit.window.aggregatedCounters.successfulProbeRequests.Load(), "unexpected success probe requests")
			assert.Equal(t, test.expectedSuccessRequests, circuit.window.aggregatedCounters.successfulRequests.Load(), "unexpected success requests")
		})
	}
}

func TestTransitionStateIfNeeded(t *testing.T) {
	clock := &mockClock{now: time.Unix(10, 1)}
	tests := []struct {
		description        string
		givenConfig        Config
		givenStatus        *Status
		givenTotalRequests int64
		givenTotalProbes   int64
		givenSuccessProbes int64
		expectedStatus     *Status
	}{
		{
			description: "no_change_in_status_when_in_healthy_state",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
		},
		{
			description: "no_state_change_when_recovery_timeout_is_not_complete_in_unhealthy_state",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenStatus: &Status{
				state:           Unhealthy,
				recoveryTimeout: clock.now.Add(time.Second),
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Unhealthy,
				recoveryTimeout: clock.now.Add(time.Second),
				probeRatioIndex: -1,
			},
		},
		{
			description: "must_move_from_unhealthy_to_probing_when_recovery_timeout_is_complete",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenStatus: &Status{
				state:           Unhealthy,
				recoveryTimeout: clock.now.Add(-1),
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
				probeTimeout:    clock.now.Add(time.Second),
			},
		},
		{
			description: "no_state_change_when_probe_timeout_is_not_complete",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenStatus: &Status{
				state:           Probing,
				probeTimeout:    clock.now.Add(time.Second),
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeTimeout:    clock.now.Add(time.Second),
				probeRatioIndex: 0,
			},
		},
		{
			description: "must_move_from_probing_to_unhealthy_when_probe_timeout_is_complete",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenStatus: &Status{
				state:           Probing,
				probeTimeout:    clock.now.Add(-1),
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
		},
		{
			description:        "must_move_from_probing_to_healthy_due_to_completed_probe_ratio",
			givenTotalRequests: 100,
			givenTotalProbes:   80,
			givenSuccessProbes: 70,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenStatus: &Status{
				state:           Probing,
				probeRatioIndex: 2,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
		},
		{
			description:        "must_move_to_next_probe_ratio",
			givenTotalRequests: 100,
			givenTotalProbes:   41,
			givenSuccessProbes: 41,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			givenStatus: &Status{
				state:           Probing,
				probeRatioIndex: 1,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 2,
				probeTimeout:    clock.now.Add(time.Second),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			circuit := Circuit{
				config: test.givenConfig,
				window: newSlidingWindow(15, time.Second),
				status: NewStatus(test.givenConfig),
			}
			circuit.status.state = test.givenStatus.state
			circuit.status.recoveryTimeout = test.givenStatus.recoveryTimeout
			circuit.status.probeTimeout = test.givenStatus.probeTimeout
			circuit.status.probeRatioIndex = test.givenStatus.probeRatioIndex
			circuit.status.clock = clock
			circuit.window.clock = clock

			for i := 0; i < int(test.givenTotalRequests); i++ {
				circuit.window.incTotalRequests()
			}

			for i := 0; i < int(test.givenTotalProbes); i++ {
				circuit.window.incTotalProbeRequests()
			}

			for i := 0; i < int(test.givenSuccessProbes); i++ {
				circuit.window.incSuccessfulProbeRequests()
			}

			circuit.transitionStateIfNeeded()
			assertStatus(t, test.expectedStatus, circuit.status)
		})
	}
}

func TestStatusMethod(t *testing.T) {
	clock := &mockClock{now: time.Unix(10, 1)}
	tests := []struct {
		description    string
		givenStatus    *Status
		expectedStatus *Status
	}{
		{
			description: "return_current_state",
			givenStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
		},
		{
			description: "must_return_changed_state_due_to_recovery_timeout",
			givenStatus: &Status{
				state:           Unhealthy,
				recoveryTimeout: clock.now.Add(-1),
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
				probeTimeout:    clock.now.Add(time.Second),
			},
		},
		{
			description: "must_return_changed_state_due_to_probe_timeout",
			givenStatus: &Status{
				state:           Probing,
				probeTimeout:    clock.now.Add(-1),
				probeRatioIndex: 0,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			config := Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			}
			circuit := Circuit{
				config: config,
				window: newSlidingWindow(15, time.Second),
				status: NewStatus(config),
			}
			circuit.status.state = test.givenStatus.state
			circuit.status.recoveryTimeout = test.givenStatus.recoveryTimeout
			circuit.status.probeTimeout = test.givenStatus.probeTimeout
			circuit.status.probeRatioIndex = test.givenStatus.probeRatioIndex
			circuit.status.clock = clock
			circuit.window.clock = clock

			assertStatus(t, test.expectedStatus, circuit.Status())
		})
	}
}

func TestIsRequestAllowed(t *testing.T) {
	clock := &mockClock{now: time.Unix(10, 1)}
	tests := []struct {
		description          string
		givenConfig          Config
		givenTotalRequests   int64
		givenTotalProbes     int64
		givenSuccessProbes   int64
		initialStatus        *Status
		expectedStatus       *Status
		expectedAllow        bool
		expectedCounterReset bool
	}{
		{
			description: "must_allow_in_healthy_status",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 25,
			},
			initialStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedAllow: true,
		},
		{
			description: "must_reject_in_unhealthy_status",
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
				recoveryTimeout: clock.now.Add(time.Second),
			},
			expectedStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
				recoveryTimeout: clock.now.Add(time.Second),
			},
			expectedAllow: false,
		},
		{
			description:        "must_move_to_probing_state_as_recovery_timeout_is_complete",
			givenTotalRequests: 100,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Unhealthy,
				probeRatioIndex: -1,
				recoveryTimeout: clock.now.Add(-1),
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedAllow: true,
		},
		{
			description:        "must_reject_probe_due_to_high_probes",
			givenTotalRequests: 100,
			givenTotalProbes:   40,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedAllow: false,
		},
		{
			description:        "must_move_from_probing_to_healthy_due_to_probe_timeout",
			givenTotalRequests: 100,
			givenTotalProbes:   40,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 0,
				probeTimeout:    clock.now.Add(-1),
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedAllow:        true,
			expectedCounterReset: true,
		},
		{
			description:        "must_move_from_probing_to_healthy_due_to_completed_probe_ratio",
			givenTotalRequests: 100,
			givenTotalProbes:   80,
			givenSuccessProbes: 70,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 2,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedStatus: &Status{
				state:           Healthy,
				probeRatioIndex: -1,
			},
			expectedAllow:        true,
			expectedCounterReset: true,
		},
		{
			description:        "must_move_to_next_probe_ratio",
			givenTotalRequests: 100,
			givenTotalProbes:   41,
			givenSuccessProbes: 41,
			givenConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               1,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.5,
				ProbeRatios:          []float64{0.2, 0.4, 0.6},
				MinimumProbeRequests: 1,
			},
			initialStatus: &Status{
				state:           Probing,
				probeRatioIndex: 1,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedStatus: &Status{
				state:           Probing,
				probeRatioIndex: 2,
				probeTimeout:    clock.now.Add(time.Second),
			},
			expectedAllow:        true,
			expectedCounterReset: false,
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			circuit := Circuit{
				config: test.givenConfig,
				window: newSlidingWindow(15, time.Second),
				status: NewStatus(test.givenConfig),
			}
			circuit.status.state = test.initialStatus.state
			circuit.status.recoveryTimeout = test.initialStatus.recoveryTimeout
			circuit.status.probeTimeout = test.initialStatus.probeTimeout
			circuit.status.probeRatioIndex = test.initialStatus.probeRatioIndex
			circuit.status.clock = clock
			circuit.window.clock = clock

			for i := 0; i < int(test.givenTotalRequests); i++ {
				circuit.window.incTotalRequests()
			}

			for i := 0; i < int(test.givenTotalProbes); i++ {
				circuit.window.incTotalProbeRequests()
			}

			for i := 0; i < int(test.givenSuccessProbes); i++ {
				circuit.window.incSuccessfulProbeRequests()
			}

			isAllowed := circuit.IsRequestAllowed()
			assert.Equal(t, test.expectedAllow, isAllowed, "unexpected is allowed bool returned")
			assertStatus(t, test.expectedStatus, circuit.status)

			expectedTotalRequest := test.givenTotalRequests + 1
			if test.expectedCounterReset {
				expectedTotalRequest = 0
			}
			assert.Equal(t, expectedTotalRequest, circuit.window.counters().totalRequests.Load(), "unexpected total requests")

			expectedProbeRequests := test.givenTotalProbes
			if test.expectedCounterReset {
				expectedProbeRequests = 0
			} else if test.expectedStatus.state == Probing && test.expectedAllow {
				expectedProbeRequests++
			}
			assert.Equal(t, expectedProbeRequests, circuit.window.counters().totalProbeRequests.Load(), "unexpected probe requests")
		})
	}
}

func TestE2E(t *testing.T) {
	clock := atomicMockClock{nowInNano: *atomic.NewInt64(time.Unix(10, 1).UnixNano())}
	circuit, err := NewCircuit(Config{
		MinimumRequests:      200,
		WindowSize:           5,
		BucketDuration:       time.Second,
		RecoveryTime:         time.Second * 5,
		Jitter:               time.Nanosecond, // setting nanosecond to avoid jitter as it will make the test flaky.
		MaxProbeTime:         time.Second * 25,
		ProbeRatios:          []float64{0.05, 0.15, 0.30, 0.50},
		FailureRatio:         0.50,
		MinimumProbeRequests: 10,
	})
	require.NoError(t, err, "unexpected error")
	circuit.window.clock = &clock
	circuit.status.clock = &clock

	steps := []struct {
		description string
		// now is a time where each step controls at what precise time it's requests
		// must be dispatched. This time is set to the mock clock to simulate real
		// behaviour.
		now time.Time
		// totalReqs are number of requests to be dispatched in this step.
		totalReqs int
		// reportErrRate is the number of requests from totalReqs which must be reported
		// as failed.
		reportErrRate float64
		// expectedEndState is asserted at the end of the step.
		expectedEndState State
		// expectedEndProbeRatio ratio is asserted at the end of the step, if set to non-zero value.
		expectedEndProbeRatio float64
	}{
		// Below buckets belong to healthy state which notices varied error rate
		// Please read the above circuit config before reading the below values.

		// Healthy -> Unhealthy -> Probing -> Healthy
		//
		{description: "bucket_0", now: time.Unix(20, 1), totalReqs: 100, expectedEndState: Healthy},
		{description: "bucket_1", now: time.Unix(21, 1), reportErrRate: 0.10, totalReqs: 100, expectedEndState: Healthy},
		{description: "bucket_2", now: time.Unix(22, 1), reportErrRate: 0.20, totalReqs: 100, expectedEndState: Healthy},
		{description: "bucket_3", now: time.Unix(23, 1), reportErrRate: 0.70, totalReqs: 100, expectedEndState: Healthy},
		{description: "bucket_4", now: time.Unix(24, 1), reportErrRate: 1.00, totalReqs: 100, expectedEndState: Healthy},
		// Window of below and last 4 buckets have 60% ((100+100+70+20+10)/500) error rate
		// Now the circuit must move to Unhealthy state at the end of below bucket.
		{description: "u_bucket_5", now: time.Unix(25, 1), reportErrRate: 1.00, totalReqs: 100, expectedEndState: Unhealthy},
		{description: "u_bucket_6", now: time.Unix(26, 1), totalReqs: 100, expectedEndState: Unhealthy},
		{description: "u_bucket_7", now: time.Unix(27, 1), totalReqs: 100, expectedEndState: Unhealthy},
		{description: "u_bucket_8", now: time.Unix(28, 1), totalReqs: 100, expectedEndState: Unhealthy},
		{description: "u_bucket_9", now: time.Unix(29, 1), totalReqs: 100, expectedEndState: Unhealthy},
		{description: "u_bucket_10", now: time.Unix(30, 1), totalReqs: 100, expectedEndState: Unhealthy},
		// Now the time is beyond the recovery timeout (unhealthy started at 25, now it is 31)
		// Must transition to probing state at first ratio 0.05.
		{description: "p_bucket_11_1", now: time.Unix(31, 1), totalReqs: 5, expectedEndState: Probing, expectedEndProbeRatio: 0.05},
		// Must move probing to second ratio 0.15 as this bucket has seen 11(6+5)
		// probes which is more than the config.MinimumProbeRequests of 10 requests
		// and the first probe ratio 0.05 of 82rps => 4requests.
		{description: "p_bucket_11_2", now: time.Unix(31, 2), totalReqs: 6, expectedEndState: Probing, expectedEndProbeRatio: 0.15},
		// Must move probing to third ratio 0.30 as this bucket has seen 21(10+6+5)
		// probes which is more than the second probe ratio 0.15 of 84rps => 21 requests.
		{description: "p_bucket_11_3", now: time.Unix(31, 3), totalReqs: 10, expectedEndState: Probing, expectedEndProbeRatio: 0.3},
		// Must move probing to fourth ratio 0.50 as this bucket has seen 41(20+10+6+5)
		// probes which is more than the third probe ratio 0.30 of 88rps => 27 requests.
		{description: "p_bucket_11_3", now: time.Unix(31, 4), totalReqs: 20, expectedEndState: Probing, expectedEndProbeRatio: 0.5},
		// Must transition to healthy state has last 4 buckets has seen 61(20+20+10+6+5)
		// probes which is more than last probe ratio 0.50 of 92rps => 46 requests.
		{description: "bucket_11_4", now: time.Unix(31, 5), totalReqs: 20, expectedEndState: Healthy},

		// Healthy -> Unhealthy -> Probing -> Unhealthy (due to probe failures) -> Probing -> Healthy
		//
		// Reinitialize to healthy state now.
		{description: "bucket_12", now: time.Unix(32, 1), totalReqs: 100, expectedEndState: Healthy},
		{description: "bucket_13", now: time.Unix(33, 1), reportErrRate: 0.10, totalReqs: 100, expectedEndState: Healthy},
		{description: "bucket_14", now: time.Unix(34, 1), reportErrRate: 0.10, totalReqs: 100, expectedEndState: Healthy},
		// Move to unhealthy state since last 5 (windowsize) buckets have 480(450+10+10) failures
		// which is 60% failure rate (480/800).
		{description: "u_bucket_15", now: time.Unix(35, 1), reportErrRate: 0.90, totalReqs: 500, expectedEndState: Unhealthy},
		// Skip 4 time buckets and ensure state is still unhealthy.
		{description: "u_bucket_19", now: time.Unix(39, 1), totalReqs: 100, expectedEndState: Unhealthy},
		// Skip 3 time buckets and ensure state transitions to Probing state as recovery time of 5s is over.
		{description: "p_bucket_22", now: time.Unix(42, 1), reportErrRate: 1.00, totalReqs: 9, expectedEndState: Probing},
		// Must transition to unhealthy from probing state as all probe requests (1+9) have failed.
		{description: "u_bucket_23", now: time.Unix(43, 1), reportErrRate: 1.00, totalReqs: 1, expectedEndState: Unhealthy},
		{description: "u_bucket_24", now: time.Unix(44, 1), totalReqs: 1000, expectedEndState: Unhealthy},
		{description: "u_bucket_25", now: time.Unix(45, 1), totalReqs: 1000, expectedEndState: Unhealthy},
		{description: "u_bucket_26", now: time.Unix(46, 1), totalReqs: 1000, expectedEndState: Unhealthy},
		{description: "u_bucket_27", now: time.Unix(47, 1), totalReqs: 1000, expectedEndState: Unhealthy},
		{description: "u_bucket_28", now: time.Unix(48, 1), totalReqs: 1000, expectedEndState: Unhealthy},
		// Must transition to probing state as it's beyond the recovery time of 5s.
		// Must be probing at first ratio 0.05.
		{description: "p_bucket_29", now: time.Unix(49, 1), totalReqs: 40, expectedEndState: Probing, expectedEndProbeRatio: 0.05},
		// Must move probing to second ratio 0.15 as this bucket has seen 120(80+40)
		// probes which is more than the first probe ratio 0.05 of 824rps (4120/5).
		{description: "p_bucket_29_1", now: time.Unix(49, 2), totalReqs: 80, expectedEndState: Probing, expectedEndProbeRatio: 0.15},
		// Must move probing to third ratio 0.30 as this bucket has seen 250(130+80+40)
		// probes which is more than the second probe ratio 0.15 of 850rps (4250/5).
		{description: "p_bucket_29_2", now: time.Unix(49, 4), totalReqs: 130, expectedEndState: Probing, expectedEndProbeRatio: 0.30},
		// Must move probing to fourth ratio 0.50 as this bucket has seen 430(180+130+80+40)
		// probes which is more than the second probe ratio 0.30 of 886rps (4430/5).
		{description: "p_bucket_29_3", now: time.Unix(49, 5), totalReqs: 180, expectedEndState: Probing, expectedEndProbeRatio: 0.50},
		// Must move to healthy after noticing 445 probes 460(30+180+130+80+40) in this time bucket
		// which is more than last probe ratio of 0.5 of 892rps (4460/5).
		{description: "bucket_29_4", now: time.Unix(49, 5), totalReqs: 30, expectedEndState: Healthy},

		// Healthy -> Unhealthy -> Probing (due to recovery timeout) -> Healthy (due to probe timeout)
		//
		// Skip 10 seconds to empty the entire sliding window and must be in healthy state.
		{description: "bucket_38", now: time.Unix(58, 1), totalReqs: 1, expectedEndState: Healthy},
		// Must transition to unhealthy as the window has 100% failure of 500/501 requests.
		{description: "u_bucket_39", now: time.Unix(59, 1), reportErrRate: 1.00, totalReqs: 500, expectedEndState: Unhealthy},
		// Skip 5 seconds to reach recovery timeout, now transition from unhealthy to
		// healthy state. Also, the sliding window is empty now due to no requests in
		// the last 5 seconds.
		{description: "bucket_44", now: time.Unix(64, 2), totalReqs: 1, expectedEndState: Probing},
		// Must still be in probe state after 20 seconds of gap even without requests
		// as probe timeout of 25s has not reached yet.
		{description: "bucket_64", now: time.Unix(84, 2), totalReqs: 1, expectedEndState: Probing},
		// Must move to healthy state as probe timeout of 25s has been reached.
		{description: "bucket_70", now: time.Unix(90, 0), totalReqs: 1, expectedEndState: Healthy},
	}
	for _, step := range steps {
		clock.updateTime(step.now)

		requestWithErr := make(chan bool)
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			// Go routine acts as request handler, which receives request from
			// the request channel.
			go func() {
				for isErr := range requestWithErr {
					if allowed := circuit.IsRequestAllowed(); allowed {
						circuit.ReportRequestStatus(isErr)
					}
				}

				wg.Done()
			}()
		}

		numberOfSuccessRequests := step.totalReqs - int(float64(step.totalReqs)*step.reportErrRate)
		for j := 0; j < step.totalReqs; j++ {
			requestWithErr <- (j < numberOfSuccessRequests)
		}

		close(requestWithErr)
		wg.Wait()

		require.Equal(t, step.expectedEndState, circuit.status.State(), "unexpected state at step:%s", step.description)
		if step.expectedEndProbeRatio != 0 {
			assert.Equal(t, step.expectedEndProbeRatio, circuit.config.ProbeRatios[circuit.status.probeRatioIndex], "unexpected probe ratio at step:%s", step.description)
		}
	}

}

func BenchmarkCircuitParallel(b *testing.B) {
	b.ReportAllocs()
	tests := []struct {
		description string
		config      Config
	}{
		{
			description: "aggressive_50µs_20_buckets",
			config: Config{
				WindowSize:           20,
				BucketDuration:       time.Microsecond * 50,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      200,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
		},
		{
			description: "50ms_20_buckets",
			config: Config{
				WindowSize:           20,
				BucketDuration:       time.Millisecond * 50,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      200,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
		},
		{
			description: "500ms_20_buckets",
			config: Config{
				WindowSize:           20,
				BucketDuration:       time.Millisecond * 500,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      200,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
		},
	}
	for _, test := range tests {
		b.Run(test.description, func(b *testing.B) {
			circuit, err := NewCircuit(test.config)
			require.NoError(b, err, "unexpected error on circuit creation")
			b.RunParallel(func(p *testing.PB) {
				for p.Next() {
					if allowed := circuit.IsRequestAllowed(); allowed {
						circuit.ReportRequestStatus(true)
					}
				}
			})
		})
	}
}
