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

package benchmarks

import (
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3ts"
	"github.com/m3db/m3db/encoding/tsz"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"
)

var (
	testProdBlockNumDP = 720
	testStepSize       = 10 * time.Second
)

func BenchmarkTSZCounters(b *testing.B) {
	benchTSZ(b, generateCountDatapoints, testProdBlockNumDP)
}

func BenchmarkM3TSCounters(b *testing.B) {
	benchM3TS(b, generateCountDatapoints, testProdBlockNumDP)
}

func BenchmarkTSZTimers(b *testing.B) {
	benchTSZ(b, generateTimerDatapoints, testProdBlockNumDP)
}

func BenchmarkM3TSTimers(b *testing.B) {
	benchM3TS(b, generateTimerDatapoints, testProdBlockNumDP)
}

func BenchmarkTSZGauges(b *testing.B) {
	benchTSZ(b, generateGaugeDatapoints, testProdBlockNumDP)
}

func BenchmarkM3TSGauges(b *testing.B) {
	benchM3TS(b, generateGaugeDatapoints, testProdBlockNumDP)
}

func benchTSZ(b *testing.B, f generateFunc, numPoints int) {
	dps, start := f(numPoints, testStepSize)
	for n := 0; n < b.N; n++ {
		tszEncoder := tsz.NewEncoder(start, []byte{}, nil)
		for _, dp := range dps {
			tszEncoder.Encode(dp, xtime.Second, nil)
		}
		tszEncoder.Seal()
	}
}

func benchM3TS(b *testing.B, f generateFunc, numPoints int) {
	dps, start := f(numPoints, testStepSize)
	for n := 0; n < b.N; n++ {
		m3tsEncoder := m3ts.NewEncoder(start, []byte{}, nil)
		for _, dp := range dps {
			m3tsEncoder.Encode(dp, xtime.Second, nil)
		}
		m3tsEncoder.Seal()
	}
}

type generateFunc func(int, time.Duration) ([]ts.Datapoint, time.Time)

func generateCountDatapoints(numPoints int, timeUnit time.Duration) ([]ts.Datapoint, time.Time) {
	return generateDataPoints(numPoints, timeUnit, 5, 0)
}

func generateTimerDatapoints(numPoints int, timeUnit time.Duration) ([]ts.Datapoint, time.Time) {
	return generateDataPoints(numPoints, timeUnit, 7, 6)
}

func generateGaugeDatapoints(numPoints int, timeUnit time.Duration) ([]ts.Datapoint, time.Time) {
	return generateDataPoints(numPoints, timeUnit, 2, 16)
}

func generateDataPoints(numPoints int, timeUnit time.Duration, numDig, numDec int) ([]ts.Datapoint, time.Time) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now().Add(-1 * time.Duration(numPoints) * timeUnit).Truncate(time.Second)
	curTime := start
	res := []ts.Datapoint{}

	for i := 0; i < numPoints; i++ {
		curTime = curTime.Add(timeUnit)
		curVal := encoding.GenerateFloatVal(r, numDig, numDec)
		res = append(res, ts.Datapoint{
			Timestamp: curTime,
			Value:     curVal,
		})
	}

	return res, start
}
