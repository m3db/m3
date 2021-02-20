// Copyright (c) 2017 Uber Technologies, Inc.
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

package aggregation

import (
	"math"
	"math/rand"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	_insertAndCompressEvery = 1024
	_sampleBatches          = 100
	_eps                    = 0.001
	_heapCapacity           = 32
)

var (
	_aggregationOptions = NewOptions(instrument.NewOptions())
	_now                = time.Now()
	_cmOptions          cm.Options
)

func getTimer() Timer {
	return NewTimer(testQuantiles, _cmOptions, _aggregationOptions)
}

func timerSamples() [][]float64 {
	rnd := rand.New(rand.NewSource(0)) //nolint:gosec
	samples := make([][]float64, _sampleBatches)
	for i := 0; i < len(samples); i++ {
		samples[i] = make([]float64, 1000)
		for j := 0; j < 1000; j++ {
			samples[i][j] = rnd.Float64()
			//* float64(
			//	rnd.Int63n(int64(time.Minute/time.Millisecond)))
			//fmt.Println(samples[i][j])
			//fmt.Println(samples[i][j] * _eps)
		}
	}
	return samples
}

func BenchmarkTimerValues(b *testing.B) {
	timer := getTimer()
	for i := 1; i <= 2500; i++ {
		timer.Add(_now, float64(i))
	}
	for n := 0; n < b.N; n++ {
		timer.Sum()
		timer.SumSq()
		timer.Mean()
		timer.Min()
		timer.Max()
		timer.Count()
		timer.Stdev()
		timer.Quantile(0.5)
		timer.Quantile(0.5)
		timer.Quantile(0.95)
		timer.Quantile(0.99)
	}
}

func BenchmarkTimerValueOf(b *testing.B) {
	timer := getTimer()
	for n := 0; n < b.N; n++ {
		for _, aggType := range testAggTypes {
			timer.ValueOf(aggType)
		}
	}
}

func BenchmarkTimerAddBatch(b *testing.B) {
	var samples = timerSamples()

	b.Run("100k samples in 100 batches", func(b *testing.B) {
		benchAddBatch(b, samples[:100])
	})

	b.Run("10k samples in 10 batches", func(b *testing.B) {
		benchAddBatch(b, samples[:10])
	})

	b.Run("5k samples in 5 batches", func(b *testing.B) {
		benchAddBatch(b, samples[:5])
	})

	b.Run("1k samples in 1 batch", func(b *testing.B) {
		benchAddBatch(b, samples[:1])
	})
}

func benchAddBatch(b *testing.B, samples [][]float64) {
	var q float64
	var z []float64

	const _debug = true

	if _debug {
		for i := range samples {
			for j := range samples[i] {
				z = append(z, samples[i][j])
			}
		}
		sort.Float64s(z)
	}

	b.ResetTimer()
	b.SetBytes(int64(8 * len(samples) * len(samples[0])))
	for n := 0; n < b.N; n++ {
		timer := getTimer()

		for i := 0; i < len(samples); i++ {
			timer.AddBatch(_now, samples[i])
		}

		for i := range testQuantiles {
			q = timer.Quantile(testQuantiles[i])
		}

		if math.IsNaN(q) {
			b.FailNow()
		}

		if _debug && n == 0 {
			q = timer.Quantile(testQuantiles[len(testQuantiles)-1])
			n := int(float64(len(z)) * testQuantiles[len(testQuantiles)-1])
			//fmt.Println(q, z[n], n, len(z))
			//fmt.Println(z[0], z[len(z)-1])
			delta := math.Abs(q - z[n])
			eps := testQuantiles[len(testQuantiles)-1] * _eps // error bound is quantile * epsilon
			if delta > 2*eps {
				b.Logf("unexpected delta: (q %f) (expected %v)  (delta %f) (eps %f)", q, z[n], delta, eps)
				b.FailNow()
				return
			}
			//fmt.Printf("EXPECTED delta: (q %f) (expected %v)  (delta %f) (eps %f)\n", q, z[n], delta, eps)

			//panic("")
		}
		timer.Close()
	}
	runtime.KeepAlive(q)
}

func init() {
	_aggregationOptions.ResetSetData(testAggTypes)
	_cmOptions = cm.NewOptions().
		SetInsertAndCompressEvery(_insertAndCompressEvery).
		SetEps(_eps).
		SetCapacity(_heapCapacity)
	_aggregationOptions.ResetSetData(testAggTypes)
}
