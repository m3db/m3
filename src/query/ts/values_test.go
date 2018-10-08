// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
