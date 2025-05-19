package circuitbreaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStatus(t *testing.T) {
	config := Config{}
	status := NewStatus(config)
	assert.Equal(t, -1, status.probeRatioIndex, "expected probe index as -1")
	assert.Equal(t, _baseClock, status.clock, "unexpected clock")
	assert.Equal(t, Healthy, status.state, "unexpected state")
	assert.Equal(t, config, status.config, "unexpected config")
}

func TestReset(t *testing.T) {
	status := Status{
		probeTimeout:    time.Unix(10, 0),
		recoveryTimeout: time.Unix(11, 0),
		state:           Unhealthy,
		probeRatioIndex: 1,
	}
	status.reset()
	assert.Equal(t, -1, status.probeRatioIndex, "unexpected probe index")
	assert.Equal(t, time.Time{}, status.probeTimeout, "unexpected probe timeout")
	assert.Equal(t, time.Time{}, status.recoveryTimeout, "unexpected recovery timeout")
	assert.Equal(t, Healthy, status.state, "unexpected state")
}

func TestIsNextProbeRatioAvailable(t *testing.T) {
	t.Run("return_true_when_next_ratio_is_available", func(t *testing.T) {
		status := NewStatus(Config{ProbeRatios: []float64{0.1, 0.2, 0.3}})
		status.probeRatioIndex = 0
		assert.True(t, status.isNextProbeRatioAvailable(), "expected next ratio to be available")
		assert.Equal(t, 0, status.probeRatioIndex, "expected same probe index")
	})

	t.Run("false_on_last_ratio", func(t *testing.T) {
		status := NewStatus(Config{ProbeRatios: []float64{0.1, 0.2, 0.3}})
		status.probeRatioIndex = 2
		assert.False(t, status.isNextProbeRatioAvailable(), "expected next ratio to be unavailable")
		assert.Equal(t, 2, status.probeRatioIndex, "expected same probe index")
	})
}

func TestMoveToNextProbeRatio(t *testing.T) {
	status := NewStatus(Config{ProbeRatios: []float64{0.1, 0.2, 0.3}})
	status.probeRatioIndex = 0
	status.moveToNextProbeRatio()
	assert.Equal(t, 1, status.probeRatioIndex, "expected same probe index")
}

func TestProbeRatio(t *testing.T) {
	t.Run("non_probing_returns_1.0", func(t *testing.T) {
		status := NewStatus(Config{ProbeRatios: []float64{0.1, 0.2, 0.3}})
		gotRatio, gotOk := status.ProbeRatio()
		require.False(t, gotOk, "unexpected ok probe ratio")
		assert.Equal(t, 0.0, gotRatio, "unexpected probe ratio")
	})

	t.Run("probe_ratio_returns_0.1", func(t *testing.T) {
		status := NewStatus(Config{ProbeRatios: []float64{0.1, 0.2, 0.3}})
		status.probeRatioIndex = 0
		status.state = Probing
		gotRatio, gotOk := status.ProbeRatio()
		require.True(t, gotOk, "unexpected ok probe ratio")
		assert.Equal(t, 0.1, gotRatio, "unexpected probe ratio")
	})
}

func TestProbeTimeoutComplete(t *testing.T) {
	t.Run("before_probe_timeout", func(t *testing.T) {
		mockClock := mockClock{now: time.Unix(9, 1)}
		status := Status{
			clock:        &mockClock,
			probeTimeout: time.Unix(10, 1),
		}
		assert.False(t, status.isProbeTimeoutComplete(), "unexpected probe timeout")
	})

	t.Run("after_probe_timeout", func(t *testing.T) {
		mockClock := mockClock{now: time.Unix(10, 2)}
		status := Status{
			clock:        &mockClock,
			probeTimeout: time.Unix(10, 1),
		}
		assert.True(t, status.isProbeTimeoutComplete(), "unexpected probe timeout")
	})
}

func TestRecoveryTimeoutComplete(t *testing.T) {
	t.Run("before_recovery_timeout", func(t *testing.T) {
		mockClock := mockClock{now: time.Unix(9, 1)}
		status := Status{
			clock:           &mockClock,
			recoveryTimeout: time.Unix(10, 1),
		}
		assert.False(t, status.isRecoveryTimeoutComplete(), "unexpected recovery timeout")
	})

	t.Run("after_recovery_timeout", func(t *testing.T) {
		mockClock := mockClock{now: time.Unix(10, 2)}
		status := Status{
			clock:           &mockClock,
			recoveryTimeout: time.Unix(10, 1),
		}
		assert.True(t, status.isRecoveryTimeoutComplete(), "unexpected recovery timeout")
	})
}

func TestSetProbing(t *testing.T) {
	now := time.Unix(10, 1)
	mockClock := &mockClock{now: now}
	timeout := time.Second * 2
	status := Status{
		config: Config{
			MaxProbeTime: timeout,
		},
		clock: mockClock,
	}
	status.setProbing()
	assert.Equal(t, Probing, status.state, "unexpected state")
	assert.Equal(t, 0, status.probeRatioIndex, "unexpected probe ratio index")
	assert.Equal(t, now.Add(timeout), status.probeTimeout, "unexpected probe timeout")
}

func TestSetUnhealthy(t *testing.T) {
	now := time.Unix(10, 1)
	mockClock := &mockClock{now: now}
	jitter := time.Second
	recoveryTime := time.Second * 2
	status := NewStatus(Config{
		RecoveryTime: recoveryTime,
		Jitter:       jitter,
	})
	status.clock = mockClock
	status.setUnhealthy()
	assert.Equal(t, Unhealthy, status.state, "unexpected state")

	floorRecoveryTimeout := now.Add(recoveryTime)
	ceilRecoveryTimeout := floorRecoveryTimeout.Add(jitter)
	assert.True(t, !status.recoveryTimeout.Before(floorRecoveryTimeout), "unexpected recovery timeout below floor timeout")
	assert.True(t, status.recoveryTimeout.Before(ceilRecoveryTimeout), "unexpected recovery timeout above ceil timeout")
}

func TestSetHealthy(t *testing.T) {
	status := Status{
		state: Unhealthy,
	}
	status.setHealthy()
	assert.Equal(t, Healthy, status.state, "unexpected state")
}

func TestUpdateState(t *testing.T) {
	status := NewStatus(Config{
		RecoveryTime: time.Second,
		MaxProbeTime: time.Second,
		Jitter:       time.Nanosecond,
	})
	status.updateState(Healthy)
	assert.Equal(t, Healthy, status.state, "expected healthy state")

	status.updateState(Unhealthy)
	assert.Equal(t, Unhealthy, status.state, "expected healthy state")

	status.updateState(Probing)
	assert.Equal(t, Probing, status.state, "expected healthy state")
}

func TestStateMethod(t *testing.T) {
	status := Status{state: Unhealthy}
	assert.Equal(t, Unhealthy, status.State(), "unexpected state")
}

func TestRecoveryTimeoutMethod(t *testing.T) {
	now := time.Unix(10, 1)
	status := Status{recoveryTimeout: now}
	assert.Equal(t, now, status.RecoveryTimeout(), "unexpected recovery time")
}

func TestProbeTimeoutMethod(t *testing.T) {
	now := time.Unix(10, 1)
	status := Status{probeTimeout: now}
	assert.Equal(t, now, status.ProbeTimeout(), "unexpected recovery time")
}
