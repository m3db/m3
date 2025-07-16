package circuitbreaker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestCountersReset(t *testing.T) {
	t.Run("nil_check", func(t *testing.T) {
		var c *counters
		assert.NotPanics(t, func() { c.reset() }, "unexpected panic on reset")
	})

	t.Run("reset", func(t *testing.T) {
		testCounters := counters{
			totalRequests:           *atomic.NewInt64(1),
			totalProbeRequests:      *atomic.NewInt64(1),
			successfulRequests:      *atomic.NewInt64(1),
			successfulProbeRequests: *atomic.NewInt64(1),
			failedRequests:          *atomic.NewInt64(1),
			failedProbeRequests:     *atomic.NewInt64(1),
		}
		testCounters.reset()
		assert.Zero(t, testCounters.failedProbeRequests.Load(), "expected failed probes to reset to 0")
		assert.Zero(t, testCounters.failedRequests.Load(), "expected failed requests to reset to 0")
		assert.Zero(t, testCounters.successfulProbeRequests.Load(), "expected successful probes to reset to 0")
		assert.Zero(t, testCounters.successfulRequests.Load(), "expected successful requests to reset to 0")
		assert.Zero(t, testCounters.totalProbeRequests.Load(), "expected total probes to reset to 0")
		assert.Zero(t, testCounters.totalRequests.Load(), "expected total requests to reset to 0")
	})
}

func TestCountersSub(t *testing.T) {
	t.Run("nil_receiver", func(t *testing.T) {
		var c *counters
		assert.NotPanics(t, func() { c.sub(&counters{}) }, "unexpected panic on nil sub")
	})

	t.Run("nil_argument", func(t *testing.T) {
		c := &counters{}
		assert.NotPanics(t, func() { c.sub(nil) }, "unexpected panic on nil sub")
	})

	t.Run("sub", func(t *testing.T) {
		c := counters{
			totalRequests:           *atomic.NewInt64(100),
			totalProbeRequests:      *atomic.NewInt64(100),
			successfulRequests:      *atomic.NewInt64(100),
			successfulProbeRequests: *atomic.NewInt64(100),
			failedRequests:          *atomic.NewInt64(100),
			failedProbeRequests:     *atomic.NewInt64(100),
		}
		c.sub(&counters{
			totalRequests:           *atomic.NewInt64(80),
			totalProbeRequests:      *atomic.NewInt64(80),
			successfulRequests:      *atomic.NewInt64(80),
			successfulProbeRequests: *atomic.NewInt64(80),
			failedRequests:          *atomic.NewInt64(80),
			failedProbeRequests:     *atomic.NewInt64(80),
		})
		expectedcounters := counters{
			totalRequests:           *atomic.NewInt64(20),
			totalProbeRequests:      *atomic.NewInt64(20),
			successfulRequests:      *atomic.NewInt64(20),
			successfulProbeRequests: *atomic.NewInt64(20),
			failedRequests:          *atomic.NewInt64(20),
			failedProbeRequests:     *atomic.NewInt64(20),
		}
		assert.Equal(t, c, expectedcounters, "unexpected counters values")
	})
}
