package ts

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSample struct {
	input       Datapoints
	expected    []float64
	start       time.Time
	end         time.Time
	interval    time.Duration
	hasNans     bool
	description string
}

func generateDatapoints(start time.Time, interval time.Duration, num int) Datapoints {
	dps := make(Datapoints, num)
	for i := 0; i < num; i++ {
		dps[i] = Datapoint{
			Timestamp: start.Add(interval * time.Duration(i)),
			Value:     float64(i),
		}
	}
	return dps
}

func createExamples() []testSample {
	now := time.Time{}
	samples := []testSample{
		{
			input:       generateDatapoints(now.Add(-5*time.Second), time.Second, 5),
			expected:    []float64{0, 1, 2, 3, 4},
			start:       now.Add(-5 * time.Second),
			end:         now,
			interval:    time.Second,
			description: "same resolution and times",
		},
		{
			input:       generateDatapoints(now.Add(-5*time.Second), 2*time.Second, 5),
			expected:    []float64{0, 0, 1, 1, 2},
			start:       now.Add(-5 * time.Second),
			end:         now,
			interval:    time.Second,
			description: "half interval than resolution, some datapoints would be repeated",
		},
		{
			input:       generateDatapoints(now.Add(-5*time.Second), time.Second, 5),
			expected:    []float64{0, 2},
			start:       now.Add(-5 * time.Second),
			end:         now,
			interval:    2 * time.Second,
			description: "twice interval than resolution, since datapoints are generated faster, we would skip some",
		},
		{
			input:       generateDatapoints(now.Add(time.Second), 2*time.Second, 5),
			expected:    []float64{1},
			start:       now.Add(3 * time.Second),
			end:         now.Add(3 * time.Second),
			interval:    time.Second,
			description: "when start and end are the same, take the value closest to start",
		},
		{
			input: generateDatapoints(now.Add(time.Second), 2*time.Second, 5),
			// start time after end time of datapoints so we get NaNs
			expected:    []float64{math.NaN(), math.NaN()},
			start:       now.Add(10 * time.Second),
			end:         now.Add(12 * time.Second),
			interval:    time.Second,
			hasNans:     true,
			description: "start time after end time of datapoints so we get NaNs",
		},
		{
			input:       generateDatapoints(now.Add(time.Second), 10*time.Second, 10),
			expected:    []float64{0, 0},
			start:       now,
			end:         now.Add(20 * time.Second),
			interval:    10 * time.Second,
			hasNans:     true,
			description: "first datapoint after start",
		},
	}
	return samples
}

func TestRawPointsToFixedStep(t *testing.T) {
	samples := createExamples()
	for idx, sample := range samples {
		fixdRes, err := RawPointsToFixedStep(sample.input, sample.start, sample.end, sample.interval)
		require.NoError(t, err)
		if !sample.hasNans {
			assert.Equal(t, fixdRes.(*fixedResolutionValues).values, sample.expected, "Datapoints: %s, description: %s", sample.input, sample.description)
			continue
		}

		// Need to handle nan case separately since assert cant compare nans, we prefer comparing the whole thing whenever possible since debugging is easier
		fixResValues := fixdRes.(*fixedResolutionValues)
		debugMsg := fmt.Sprintf("index: %d, converted %v, expected %v, description: %s", idx, fixResValues.values, sample.expected, sample.description)
		require.Len(t, fixResValues.values, len(sample.expected), debugMsg)
		for i, v := range sample.expected {
			if math.IsNaN(v) {
				require.True(t, math.IsNaN(fixResValues.values[i]), debugMsg)
			} else {
				require.Equal(t, v, fixResValues.values[i], debugMsg)
			}
		}
	}
}
