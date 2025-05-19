package circuitbreaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

func TestYamlConfigParse(t *testing.T) {
	const configYaml = `
  windowSize: 100
  bucketDuration: 100ms
  recoveryTime: 10s
  jitter: 2s
  maxProbeTime: 5s
  minimumRequests: 100
  failureRatio: 0.25
  probeRatios: [0.2, 0.3, 0.4]
  minimumProbeRequests: 324`

	var config Config
	require.NoError(t, yaml.Unmarshal([]byte(configYaml), &config), "unexpected parse error")
	require.NoError(t, config.Validate(), "unexpected validation error")
	expectedConfig := Config{
		WindowSize:           100,
		BucketDuration:       time.Millisecond * 100,
		RecoveryTime:         time.Second * 10,
		Jitter:               time.Second * 2,
		MaxProbeTime:         time.Second * 5,
		MinimumRequests:      100,
		FailureRatio:         0.25,
		ProbeRatios:          []float64{0.2, 0.3, 0.4},
		MinimumProbeRequests: 324,
	}
	assert.Equal(t, expectedConfig, config)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		description    string
		givenConfig    Config
		expectedErrMsg string
	}{
		{
			description: "no_error_with_valid_configs",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
		},
		{
			description: "must_return_error_with_invalid_window_size",
			givenConfig: Config{
				WindowSize:           -1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid window size: -1",
		},
		{
			description: "must_return_error_with_invalid_bucket_size",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       -1,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid bucket duration: -1",
		},
		{
			description: "must_return_error_with_invalid_minimum_requests",
			givenConfig: Config{
				MinimumRequests:      -1,
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid minimum requests: -1",
		},
		{
			description: "must_return_error_with_invalid_recovery_time",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         -1,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid recovery time: -1ns",
		},
		{
			description: "must_return_error_with_invalid_jitter",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               -1,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid jitter time: -1ns",
		},
		{
			description: "must_return_error_with_invalid_max_probe_time",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         -1,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid max probe time: -1ns",
		},
		{
			description: "must_return_error_with_invalid_failure_ratio",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         1.2,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid failure ratio: 1.20",
		},
		{
			description: "must_return_error_with_negative_probe_ratio",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.2,
				ProbeRatios:          []float64{-1},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid probe ratios: [-1]",
		},
		{
			description: "must_return_error_with_probe_ratio_above_1_values",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid probe ratios: [2]",
		},
		{
			description: "must_return_error_with_unsorted_values",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.5, 0.2},
				MinimumProbeRequests: 1,
			},
			expectedErrMsg: "invalid probe ratios: [0.1 0.5 0.2]",
		},
		{
			description: "must_return_error_with_invalid_minimum_probe_requests",
			givenConfig: Config{
				WindowSize:           1,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second,
				Jitter:               time.Second,
				MaxProbeTime:         time.Second,
				MinimumRequests:      1,
				FailureRatio:         0.1,
				ProbeRatios:          []float64{0.1, 0.2},
				MinimumProbeRequests: -1,
			},
			expectedErrMsg: "invalid minimum probe requests: -1",
		},
	}
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			err := test.givenConfig.Validate()
			if test.expectedErrMsg != "" {
				require.Error(t, err, "expected validation error")
				require.Equal(t, test.expectedErrMsg, err.Error())
			} else {
				require.NoError(t, err, "unexpected validation error")
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	tests := []struct {
		description    string
		givenConfig    Config
		expectedConfig Config
	}{
		{
			description: "apply_all_defaults",
			givenConfig: Config{},
			expectedConfig: Config{
				WindowSize:           15,
				BucketDuration:       time.Second,
				RecoveryTime:         time.Second * 2,
				Jitter:               time.Second * 1,
				MaxProbeTime:         time.Second * 25,
				MinimumRequests:      100,
				FailureRatio:         0.50,
				ProbeRatios:          []float64{0.05, 0.15, 0.30, 0.50, 0.75},
				MinimumProbeRequests: 25,
			},
		},
		{
			description: "must_not_default_on_invalid_values",
			givenConfig: Config{
				WindowSize:           -1,
				BucketDuration:       -1,
				RecoveryTime:         -1,
				Jitter:               -1,
				MaxProbeTime:         -1,
				MinimumRequests:      -1,
				FailureRatio:         100,
				MinimumProbeRequests: -1,
				ProbeRatios:          []float64{0.90, 0.15, 0.30, 0.50},
			},
			expectedConfig: Config{
				WindowSize:           -1,
				BucketDuration:       -1,
				RecoveryTime:         -1,
				Jitter:               -1,
				MaxProbeTime:         -1,
				MinimumRequests:      -1,
				FailureRatio:         100,
				MinimumProbeRequests: -1,
				ProbeRatios:          []float64{0.90, 0.15, 0.30, 0.50},
			},
		},
		{
			description: "valid_values_must_not_be_overwritten",
			givenConfig: Config{
				WindowSize:           100,
				BucketDuration:       time.Millisecond * 500,
				RecoveryTime:         time.Millisecond * 500,
				Jitter:               time.Millisecond,
				MaxProbeTime:         time.Hour,
				MinimumRequests:      10000,
				FailureRatio:         0.999,
				MinimumProbeRequests: 12000,
				ProbeRatios:          []float64{0.1, 0.2, 0.3, 0.4, 0.5},
			},
			expectedConfig: Config{
				WindowSize:           100,
				BucketDuration:       time.Millisecond * 500,
				RecoveryTime:         time.Millisecond * 500,
				Jitter:               time.Millisecond,
				MaxProbeTime:         time.Hour,
				MinimumRequests:      10000,
				FailureRatio:         0.999,
				MinimumProbeRequests: 12000,
				ProbeRatios:          []float64{0.1, 0.2, 0.3, 0.4, 0.5},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			assert.Equal(t, test.expectedConfig, test.givenConfig.applyDefaultValues(), "unexpected config")
		})
	}
}
