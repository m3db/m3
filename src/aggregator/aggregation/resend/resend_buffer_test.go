package resend

import (
	"math"
	"testing"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/tallytest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestMaxResendBuffer(t *testing.T) {
	var (
		numToAdd   = 10
		bufferSize = 4

		scope      = tally.NewTestScope("", nil)
		iOpts      = instrument.NewOptions().SetMetricsScope(scope)
		maxMetrics = NewMaxResendBufferMetrics(bufferSize, iOpts)
		maxBuffer  = NewMaxBuffer(bufferSize, maxMetrics)

		inserted int64
		updated  int64
	)

	b := maxBuffer.(*resendBuffer)
	assert.True(t, math.IsNaN(maxBuffer.Value()))

	// Insert progressively larger values that should update the max each insert.
	for i := 0; i < numToAdd; i++ {
		inserted++
		floatVal := float64(i)
		maxBuffer.Insert(floatVal)
		require.Equal(t, floatVal, maxBuffer.Value())
	}

	// Insert small values that should not affect the max or change the list.
	currMax := maxBuffer.Value()
	currBuffer := append([]float64{}, b.list...)
	for i := 0; i < numToAdd-bufferSize; i++ {
		inserted++
		floatVal := float64(i)
		maxBuffer.Insert(floatVal)
		require.Equal(t, currMax, maxBuffer.Value())
		require.Equal(t, currBuffer, b.list)
	}

	// Resend small values that should not affect the max or change the list.
	for i := 0; i < numToAdd-bufferSize; i++ {
		updated++
		maxBuffer.Update(float64(i), 0)
		require.Equal(t, currMax, maxBuffer.Value())
		require.Equal(t, currBuffer, b.list)
	}

	// Resend large values that appear in the list that should update the max.
	for i := numToAdd - bufferSize; i < numToAdd; i++ {
		updated++
		updatedVal := float64(10 + i)
		maxBuffer.Update(float64(i), updatedVal)
		require.Equal(t, updatedVal, maxBuffer.Value())
	}

	// Capture current largest value.
	currMax = maxBuffer.Value()

	// Resend  a large value that should not appear in the list.
	largeVal := float64(100)

	updated++
	maxBuffer.Update(0, largeVal)
	require.Equal(t, largeVal, maxBuffer.Value())

	// Update the previously resent large value with a small value to ensure value
	// is returned to max before the large value came in.
	updated++
	maxBuffer.Update(largeVal, 0)
	require.Equal(t, currMax, maxBuffer.Value())

	maxTags := map[string]string{"type": "max"}
	tallytest.
		AssertCounterValue(t, 1, scope.Snapshot(), "resend.count", maxTags)
	tallytest.
		AssertCounterValue(t, inserted, scope.Snapshot(), "resend.inserted", maxTags)
	tallytest.
		AssertCounterValue(t, updated, scope.Snapshot(), "resend.updated", maxTags)

	maxBuffer.Close()
}
