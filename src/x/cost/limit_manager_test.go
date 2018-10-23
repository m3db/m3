package cost

import (
	"errors"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamicLimitManager(t *testing.T) {
	var (
		threshold    Cost = 100
		enabled           = true
		store             = mem.NewStore()
		thresholdKey      = "threshold"
		enabledKey        = "enabled"
	)

	validateFn := func(data interface{}) error {
		val := data.(float64)
		if val < 50 {
			return errors.New("limit cannot be below 50")
		}
		return nil
	}

	var (
		limit = Limit{
			Threshold: threshold,
			Enabled:   enabled,
		}
		opts = NewLimitManagerOptions().
			SetDefaultLimit(limit).
			SetValidateLimitFn(validateFn)
	)

	m, err := NewDynamicLimitManager(
		store, thresholdKey, enabledKey, opts,
	)
	require.NoError(t, err)

	testLimitManager(t, m, threshold, enabled)

	// Test updates to the threshold.
	threshold = 200
	store.Set(thresholdKey, &commonpb.Float64Proto{Value: float64(threshold)})

	for {
		if m.Limit().Threshold == threshold {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	testLimitManager(t, m, threshold, enabled)

	// Test updates to enabled.
	enabled = false
	store.Set(enabledKey, &commonpb.BoolProto{Value: enabled})

	for {
		if !m.Limit().Enabled {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	testLimitManager(t, m, threshold, enabled)

	// Test that invalid updates are ignored.
	store.Set(thresholdKey, &commonpb.Float64Proto{Value: 25})
	time.Sleep(100 * time.Millisecond)

	testLimitManager(t, m, threshold, enabled)

	// Ensure we do not leak any goroutines.
	m.Close()
	leaktest.Check(t)
}

func TestStaticLimitManager(t *testing.T) {
	var (
		threshold = Cost(100)
		enabled   = true
		limit     = Limit{
			Threshold: threshold,
			Enabled:   enabled,
		}
		manager = NewStaticLimitManager(NewLimitManagerOptions().SetDefaultLimit(limit))
	)

	limit = manager.Limit()
	assert.Equal(t, threshold, limit.Threshold)
	assert.Equal(t, enabled, limit.Enabled)

	// Ensure we do not leak any goroutines.
	manager.Close()
	leaktest.Check(t)
}

func testLimitManager(
	t *testing.T, m LimitManager, expectedThreshold Cost, expectedEnabled bool,
) {
	l := m.Limit()
	assert.Equal(t, expectedThreshold, l.Threshold)
	assert.Equal(t, expectedEnabled, l.Enabled)
}
