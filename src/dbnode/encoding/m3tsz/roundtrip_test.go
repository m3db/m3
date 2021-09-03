// Copyright (c) 2016 Uber Technologies, Inc.
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

package m3tsz

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding/testgen"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestCountsRoundTrip(t *testing.T) {
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateCounterDatapoints(numPoints))
	}
}

func TestTimerRoundTrip(t *testing.T) {
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateTimerDatapoints(numPoints))
	}
}

func TestSmallGaugeRoundTrip(t *testing.T) {
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateSmallFloatDatapoints(numPoints))
	}
}

func TestPreciseGaugeRoundTrip(t *testing.T) {
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generatePreciseFloatDatapoints(numPoints))
	}
}

func TestNegativeGaugeFloatsRoundTrip(t *testing.T) {
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateNegativeFloatDatapoints(numPoints))
	}
}

func TestMixedGaugeIntRoundTrip(t *testing.T) {
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateMixSignIntDatapoints(numPoints))
	}
}

func TestMixedRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateMixedDatapoints(numPoints, timeUnit))
	}
}

func TestIntOverflow(t *testing.T) {
	testRoundTrip(t, generateOverflowDatapoints())
}

func TestConstantValuesRoundTrip(t *testing.T) {
	numIterations := 100
	for i := 1; i < numIterations; i++ {
		testRoundTrip(t, generateConstDatapoints(i, 5))
	}
	for i := 1; i < numIterations; i++ {
		testRoundTrip(t, generateConstDatapoints(i, 99.5))
	}
}

func TestMostlyConstantValuesRoundTrip(t *testing.T) {
	var (
		currentTime = xtime.FromSeconds(1630436128)
		dps         []ts.Datapoint
	)

	dps, currentTime = addDP(dps, currentTime, time.Second, 1)
	dps, currentTime = addDP(dps, currentTime, time.Second, 1)
	dps, currentTime = addDP(dps, currentTime, time.Second, 1)
	dps, currentTime = addDP(dps, currentTime, time.Second, 2)
	dps, currentTime = addDP(dps, currentTime, time.Second, 3)
	dps, currentTime = addDP(dps, currentTime, time.Second, 3)
	dps, currentTime = addDP(dps, currentTime, time.Second, 3)
	dps, currentTime = addDP(dps, currentTime, time.Second, 3)
	dps, currentTime = addDP(dps, currentTime, time.Second, 0)
	dps, currentTime = addDP(dps, currentTime, time.Second, 1)
	dps, currentTime = addDP(dps, currentTime, time.Second, 3)
	dps, currentTime = addDP(dps, currentTime, time.Second, 3)
	dps, currentTime = addDP(dps, currentTime, time.Second, 3)
	dps, currentTime = addDP(dps, currentTime, time.Second, 4)
	dps, currentTime = addDP(dps, currentTime, time.Second, 4)
	dps, currentTime = addDP(dps, currentTime, time.Second, 4)
	dps, currentTime = addDP(dps, currentTime, 2*time.Second, 4)
	dps, currentTime = addDP(dps, currentTime, 3*time.Second, 4)
	dps, currentTime = addDP(dps, currentTime, time.Second, 0)
	dps, currentTime = addDP(dps, currentTime, time.Second, 0)

	testRoundTrip(t, dps)
}

func testRoundTrip(t *testing.T, input []ts.Datapoint) {
	validateRoundTrip(t, input, true)
	validateRoundTrip(t, input, false)
}

func validateRoundTrip(t *testing.T, input []ts.Datapoint, intOpt bool) {
	ctx := context.NewBackground()
	defer ctx.Close()

	encoder := NewEncoder(testStartTime, nil, intOpt, nil)
	timeUnits := make([]xtime.Unit, 0, len(input))
	annotations := make([]ts.Annotation, 0, len(input))

	for i, v := range input {
		timeUnit := xtime.Second
		if i == 0 {
			timeUnit = xtime.Millisecond
		} else if i == 10 {
			timeUnit = xtime.Microsecond
		}

		var annotation, annotationCopy ts.Annotation
		if i < 5 {
			annotation = ts.Annotation("foo")
		} else if i < 7 {
			annotation = ts.Annotation("bar")
		} else if i == 10 {
			annotation = ts.Annotation("long annotation long annotation long annotation long annotation")
		}

		if annotation != nil {
			annotationCopy = append(annotationCopy, annotation...)
		}

		annotations = append(annotations, annotationCopy)
		timeUnits = append(timeUnits, timeUnit)

		err := encoder.Encode(v, timeUnit, annotation)
		require.NoError(t, err)

		for j := range annotation {
			// Invalidate the original annotation to make sure encoder is not holding a reference to it.
			annotation[j] = '*'
		}
	}

	decoder := NewDecoder(intOpt, nil)
	stream, ok := encoder.Stream(ctx)
	require.True(t, ok)

	it := decoder.Decode(stream)
	defer it.Close()

	i := 0
	for it.Next() {
		v, u, a := it.Current()

		expectedAnnotation := annotations[i]
		if i > 0 && bytes.Equal(annotations[i-1], expectedAnnotation) {
			// Repeated annotation values must be discarded.
			expectedAnnotation = nil
		}

		require.Equal(t, input[i].TimestampNanos, v.TimestampNanos)
		require.Equal(t, input[i].Value, v.Value)
		require.Equal(t, timeUnits[i], u)
		require.Equal(t, expectedAnnotation, a)

		i++
	}

	require.NoError(t, it.Err())
	require.Equal(t, len(input), i)
	it.Reset(nil, nil)
	it.Close()
}

func generateCounterDatapoints(numPoints int) []ts.Datapoint {
	return generateDataPoints(numPoints, 12, 0)
}

func generateTimerDatapoints(numPoints int) []ts.Datapoint {
	return generateDataPoints(numPoints, 7, 6)
}

func generateSmallFloatDatapoints(numPoints int) []ts.Datapoint {
	return generateDataPoints(numPoints, 0, 1)
}

func generatePreciseFloatDatapoints(numPoints int) []ts.Datapoint {
	return generateDataPoints(numPoints, 2, 16)
}

func generateNegativeFloatDatapoints(numPoints int) []ts.Datapoint {
	dps := generateDataPoints(numPoints, 5, 3)
	for i, dp := range dps {
		dps[i].Value = -1 * dp.Value
	}

	return dps
}

func generateMixSignIntDatapoints(numPoints int) []ts.Datapoint {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	dps := generateDataPoints(numPoints, 3, 0)
	for i, dp := range dps {
		if r.Float64() < 0.5 {
			dps[i].Value = -1 * dp.Value
		}
	}

	return dps
}

func generateDataPoints(numPoints int, numDig, numDec int) []ts.Datapoint {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var startTime int64 = 1427162462
	currentTime := xtime.FromSeconds(startTime)
	endTime := testStartTime.Add(2 * time.Hour)
	currentValue := 1.0
	res := []ts.Datapoint{{TimestampNanos: currentTime, Value: currentValue}}
	for i := 1; i < numPoints; i++ {
		currentTime = currentTime.Add(time.Second * time.Duration(rand.Intn(1200)))
		currentValue = testgen.GenerateFloatVal(r, numDig, numDec)
		if !currentTime.Before(endTime) {
			break
		}
		res = append(res, ts.Datapoint{TimestampNanos: currentTime, Value: currentValue})
	}
	return res
}

func generateMixedDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var startTime int64 = 1427162462
	currentTime := xtime.FromSeconds(startTime)
	endTime := testStartTime.Add(2 * time.Hour)
	currentValue := testgen.GenerateFloatVal(r, 3, 16)
	res := []ts.Datapoint{{TimestampNanos: currentTime, Value: currentValue}}

	for i := 1; i < numPoints; i++ {
		currentTime = currentTime.Add(time.Second * time.Duration(r.Intn(7200)))
		if r.Float64() < 0.1 {
			currentValue = testgen.GenerateFloatVal(r, 5, 0)
		} else if r.Float64() < 0.2 {
			currentValue = testgen.GenerateFloatVal(r, 3, 16)
		}

		if !currentTime.Before(endTime) {
			break
		}
		res = append(res, ts.Datapoint{TimestampNanos: currentTime, Value: currentValue})
	}
	return res
}

func generateOverflowDatapoints() []ts.Datapoint {
	var startTime int64 = 1427162462
	currentTime := xtime.FromSeconds(startTime)
	largeInt := float64(math.MaxInt64 - 1)
	largeNegInt := float64(math.MinInt64 + 1)

	vals := []float64{largeInt, 10, largeNegInt, 10, largeNegInt, largeInt, -12, largeInt, 14.5, largeInt, largeNegInt, 12.34858499392, largeInt}
	res := make([]ts.Datapoint, len(vals))

	for i, val := range vals {
		res[i] = ts.Datapoint{TimestampNanos: currentTime, Value: val}
		currentTime = currentTime.Add(time.Second)
	}

	return res
}

func generateConstDatapoints(numPoints int, value float64) []ts.Datapoint {
	var (
		currentTime = xtime.FromSeconds(1630436128)
		res         = make([]ts.Datapoint, 0, numPoints)
	)

	for i := 0; i < numPoints; i++ {
		res, currentTime = addDP(res, currentTime, time.Second, value)
	}

	return res
}

func addDP(res []ts.Datapoint, currentTime xtime.UnixNano, delta time.Duration, value float64) ([]ts.Datapoint, xtime.UnixNano) {
	res = append(res, ts.Datapoint{TimestampNanos: currentTime, Value: value})
	currentTime = currentTime.Add(delta)

	return res, currentTime
}
