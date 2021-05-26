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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateDatapoints(t xtime.UnixNano, times []int) Datapoints {
	num := len(times)
	dps := make(Datapoints, len(times))
	for i := 0; i < num; i++ {
		dps[i] = Datapoint{
			Timestamp: t.Add(time.Duration(times[i]) * time.Second),
			Value:     float64(i),
		}
	}
	return dps
}

var now = xtime.Now()
var samples = []struct {
	input                  Datapoints
	expected               [][]float64
	expectedNoWriteForward [][]float64
	bounds                 models.Bounds
	description            string
}{
	{
		input:                  generateDatapoints(now, []int{-1, 0, 10, 18, 28, 38}),
		expected:               [][]float64{{0, 1}, {2}, {3}, {4}},
		expectedNoWriteForward: [][]float64{{0, 1}, {2}, {3}, {4}},
		bounds: models.Bounds{
			Start:    now,
			Duration: 40 * time.Second,
			StepSize: 10 * time.Second,
		},
		description: "some points line up and others before",
	},
	{
		input:                  generateDatapoints(now, []int{1, 10, 18, 28}),
		expected:               [][]float64{{}, {0, 1}, {2}, {3}},
		expectedNoWriteForward: [][]float64{{}, {0, 1}, {2}, {3}},
		bounds: models.Bounds{
			Start:    now,
			Duration: 40 * time.Second,
			StepSize: 10 * time.Second,
		},
		description: "only points after start",
	},
	{
		input:                  generateDatapoints(now, []int{0, 10, 18, 28}),
		expected:               [][]float64{{0}, {0}, {1}, {1}, {2}, {2}, {3}, {3}},
		expectedNoWriteForward: [][]float64{{0}, {}, {1}, {}, {2}, {}, {3}, {}},
		bounds: models.Bounds{
			Start:    now,
			Duration: 40 * time.Second,
			StepSize: 5 * time.Second,
		},
		description: "half resolution so datapoints repeated",
	},
	{
		input:                  generateDatapoints(now, []int{0, 10, 18, 28}),
		expected:               [][]float64{{0}, {1, 2}},
		expectedNoWriteForward: [][]float64{{0}, {1, 2}},
		bounds: models.Bounds{
			Start:    now,
			Duration: 40 * time.Second,
			StepSize: 20 * time.Second,
		},
		description: "double resolution so multiple dps in the same interval",
	},
	{
		input:                  generateDatapoints(now, []int{0, 4, 5, 14}),
		expected:               [][]float64{{0}, {1, 2}, {2}, {3}},
		expectedNoWriteForward: [][]float64{{0}, {1, 2}, {}, {3}},
		bounds: models.Bounds{
			Start:    now,
			Duration: 20 * time.Second,
			StepSize: 5 * time.Second,
		},
		description: "third interval has repeated datapoint",
	},
	{
		input: generateDatapoints(now.Add(-10*time.Minute),
			[]int{-1, 0, 10, 18, 28, 38}),
		expected:               [][]float64{{}, {}, {}, {}},
		expectedNoWriteForward: [][]float64{{}, {}, {}, {}},
		bounds: models.Bounds{
			Start:    now,
			Duration: 40 * time.Second,
			StepSize: 10 * time.Second,
		},
		description: "skip really old datapoints",
	},
}

func TestDPAlign(t *testing.T) {
	for _, sample := range samples {
		dpSlice := sample.input.AlignToBounds(sample.bounds, time.Minute, nil)
		require.Len(t, dpSlice, len(sample.expected), sample.description)
		for i, dps := range dpSlice {
			assert.Equal(t, sample.expected[i], dps.Values())
		}

		dpSlice = sample.input.AlignToBoundsNoWriteForward(
			sample.bounds, time.Minute, dpSlice)
		require.Len(t, dpSlice, len(sample.expected), sample.description)
		for i, dps := range dpSlice {
			require.Equal(t, sample.expectedNoWriteForward[i], dps.Values())
		}
	}
}
