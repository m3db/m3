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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding/testgen"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestCountsRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateCounterDatapoints(numPoints, timeUnit))
	}
}

func TestTimerRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateTimerDatapoints(numPoints, timeUnit))
	}
}

func TestSmallGaugeRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateSmallFloatDatapoints(numPoints, timeUnit))
	}
}

func TestPreciseGaugeRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generatePreciseFloatDatapoints(numPoints, timeUnit))
	}
}

func TestNegativeGaugeFloatsRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateNegativeFloatDatapoints(numPoints, timeUnit))
	}
}

func TestMixedGaugeIntRoundTrip(t *testing.T) {
	timeUnit := time.Second
	numPoints := 1000
	numIterations := 100
	for i := 0; i < numIterations; i++ {
		testRoundTrip(t, generateMixSignIntDatapoints(numPoints, timeUnit))
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

func testRoundTrip(t *testing.T, input []ts.Datapoint) {
	validateRoundTrip(t, input, true)
	validateRoundTrip(t, input, false)
}

func validateRoundTrip(t *testing.T, input []ts.Datapoint, intOpt bool) {
	encoder := NewEncoder(testStartTime, nil, intOpt, nil)
	for j, v := range input {
		if j == 0 {
			encoder.Encode(v, xtime.Millisecond, proto.EncodeVarint(10))
		} else if j == 10 {
			encoder.Encode(v, xtime.Microsecond, proto.EncodeVarint(60))
		} else {
			encoder.Encode(v, xtime.Second, nil)
		}
	}
	decoder := NewDecoder(intOpt, nil)
	it := decoder.Decode(encoder.Stream())
	defer it.Close()
	var decompressed []ts.Datapoint
	j := 0
	for it.Next() {
		v, _, a := it.Current()

		if j == 0 {
			s, _ := proto.DecodeVarint(a)
			require.Equal(t, uint64(10), s)
		} else if j == 10 {
			s, _ := proto.DecodeVarint(a)
			require.Equal(t, uint64(60), s)
		} else {
			require.Nil(t, a)
		}
		decompressed = append(decompressed, v)
		j++
	}
	require.NoError(t, it.Err())
	require.Equal(t, len(input), len(decompressed))
	for i := 0; i < len(input); i++ {
		require.Equal(t, input[i].Timestamp, decompressed[i].Timestamp)
		require.Equal(t, input[i].Value, decompressed[i].Value)
	}
	it.Reset(nil)
	it.Close()
}

func generateCounterDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	return generateDataPoints(numPoints, timeUnit, 12, 0)
}

func generateTimerDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	return generateDataPoints(numPoints, timeUnit, 7, 6)
}

func generateSmallFloatDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	return generateDataPoints(numPoints, timeUnit, 0, 1)
}

func generatePreciseFloatDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	return generateDataPoints(numPoints, timeUnit, 2, 16)
}

func generateNegativeFloatDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	dps := generateDataPoints(numPoints, timeUnit, 5, 3)
	for i, dp := range dps {
		dps[i].Value = -1 * dp.Value
	}

	return dps
}

func generateMixSignIntDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	dps := generateDataPoints(numPoints, timeUnit, 3, 0)
	for i, dp := range dps {
		if r.Float64() < 0.5 {
			dps[i].Value = -1 * dp.Value
		}
	}

	return dps
}

func generateDataPoints(numPoints int, timeUnit time.Duration, numDig, numDec int) []ts.Datapoint {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var startTime int64 = 1427162462
	currentTime := time.Unix(startTime, 0)
	endTime := testStartTime.Add(2 * time.Hour)
	currentValue := 1.0
	res := []ts.Datapoint{{currentTime, currentValue}}
	for i := 1; i < numPoints; i++ {
		currentTime = currentTime.Add(time.Second * time.Duration(rand.Intn(1200)))
		currentValue = testgen.GenerateFloatVal(r, numDig, numDec)
		if !currentTime.Before(endTime) {
			break
		}
		res = append(res, ts.Datapoint{Timestamp: currentTime, Value: currentValue})
	}
	return res
}

func generateMixedDatapoints(numPoints int, timeUnit time.Duration) []ts.Datapoint {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var startTime int64 = 1427162462
	currentTime := time.Unix(startTime, 0)
	endTime := testStartTime.Add(2 * time.Hour)
	currentValue := testgen.GenerateFloatVal(r, 3, 16)
	res := []ts.Datapoint{{currentTime, currentValue}}

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
		res = append(res, ts.Datapoint{Timestamp: currentTime, Value: currentValue})
	}
	return res
}

func generateOverflowDatapoints() []ts.Datapoint {
	var startTime int64 = 1427162462
	currentTime := time.Unix(startTime, 0)
	largeInt := float64(math.MaxInt64 - 1)
	largeNegInt := float64(math.MinInt64 + 1)

	vals := []float64{largeInt, 10, largeNegInt, 10, largeNegInt, largeInt, -12, largeInt, 14.5, largeInt, largeNegInt, 12.34858499392, largeInt}
	res := make([]ts.Datapoint, len(vals))

	for i, val := range vals {
		res[i] = ts.Datapoint{Timestamp: currentTime, Value: val}
		currentTime = currentTime.Add(time.Second)
	}

	return res
}
